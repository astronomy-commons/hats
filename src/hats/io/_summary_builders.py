from itertools import starmap

import human_readable
import nested_pandas as npd
import numpy as np
import pandas as pd
from upath import UPath

from hats.catalog import CollectionProperties
from hats.catalog.dataset import Dataset
from hats.catalog.healpix_dataset.healpix_dataset import HealpixDataset
from hats.io._summary_formatters import _fmt_count_percent, _format_example_value
from hats.io.file_io import read_parquet_file_to_pandas
from hats.io.paths import get_data_thumbnail_pointer


def _cone_code_example(column_table: pd.DataFrame, cat_props) -> dict | None:
    if "example" not in column_table:
        return None
    ra = np.round(float(column_table.loc[cat_props.ra_column]["example"]))
    if ra >= 360.0:
        ra -= 360.0
    dec = np.round(float(column_table.loc[cat_props.dec_column]["example"]))
    if dec >= 90.0:
        dec = 89.9
    if dec <= -90.0:
        dec = -89.9
    return {"ra": ra, "dec": dec}


def _gen_metadata_table(catalog: Dataset, total_columns: int | None) -> dict[str, object]:
    props = catalog.catalog_info
    has_healpix_column = props.healpix_column is not None

    metadata_table = {}
    if props.total_rows is not None:
        metadata_table["Number of rows"] = f"{props.total_rows:,}"
    if total_columns is not None:
        key = "Number of columns"
        value = f"{total_columns - int(has_healpix_column):,}"
        if props.default_columns is not None:
            key = "Number of columns (default columns)"
            value = f"{value} ({len(props.default_columns):,})"
        metadata_table[key] = value
    if isinstance(catalog, HealpixDataset):
        metadata_table["Number of partitions"] = f"{len(catalog.get_healpix_pixels()):,}"
    if (hats_estsize_kb := props.extra_dict().get("hats_estsize")) is not None:
        metadata_table["Size on disk"] = human_readable.file_size(int(hats_estsize_kb) * 1024, binary=True)
    if (hats_builder := props.extra_dict().get("hats_builder")) is not None:
        metadata_table["HATS Builder"] = hats_builder
    return metadata_table


def _build_column_table(
    nf: npd.NestedFrame, default_columns, fmt_value=_format_example_value
) -> pd.DataFrame:
    """Build column info table from a NestedFrame and default column names."""
    default_columns = frozenset(default_columns or [])
    has_nested_columns = len(nf.nested_columns) > 0
    has_example_row = not nf.empty

    column = []
    dtype = []
    default = [] if len(default_columns) > 0 else None
    nested_into = [] if has_nested_columns else None
    example = [] if has_example_row else None

    for name, dt in nf.dtypes.items():
        cell = None if nf.empty else nf[name].iloc[0]
        if isinstance(dt, npd.NestedDtype):
            subcolumns = nf.get_subcolumns(name)
            column.extend(subcolumns)
            dtype.extend(f"list[{nf[sc].dtype.pyarrow_dtype}]" for sc in subcolumns)
            if default is not None:
                default.extend(name in default_columns or sc in default_columns for sc in subcolumns)
            nested_into.extend([name] * len(subcolumns))
            if example is not None:
                if cell is None:
                    example_value = (fmt_value(None) for _ in subcolumns)
                else:
                    example_value = (fmt_value(series.to_list()) for _, series in cell.items())
                example.extend(example_value)
        else:
            column.append(name)
            dtype.append(str(dt.pyarrow_dtype))
            if default is not None:
                default.append(name in default_columns)
            if nested_into is not None:
                nested_into.append(None)
            if example is not None:
                example.append(fmt_value(cell))

    index = pd.Index(column, name="column")
    result = pd.DataFrame(
        {
            "dtype": pd.Series(dtype, dtype=str, index=index),
        },
        index=index,
    )
    if default is not None:
        result["default"] = pd.Series(default, dtype=bool, index=index)
    if nested_into is not None:
        result["nested_into"] = pd.Series(nested_into, dtype=str, index=index)
    if example is not None:
        result["example"] = pd.Series(example, dtype=object, index=index)

    return result


def _gen_column_table(
    catalog: Dataset, empty_nf: npd.NestedFrame | None, fmt_value=_format_example_value
) -> pd.DataFrame:
    props = catalog.catalog_info

    nf = _get_example_row(catalog)
    if nf is None:
        if empty_nf is None:
            return pd.DataFrame()
        nf = empty_nf

    result = _build_column_table(nf, props.default_columns, fmt_value)

    stats = catalog.aggregate_column_statistics(exclude_hats_columns=False)
    if stats.empty:
        return result

    index = result.index
    missed_columns = list(set(index) - set(stats.index))

    def _fill_missed(series):
        for col in missed_columns:
            series.loc[col] = "*N/A*"
        return series

    result["min_value"] = _fill_missed(stats["min_value"].map(fmt_value))
    result["max_value"] = _fill_missed(stats["max_value"].map(fmt_value))

    row_count = stats["row_count"]
    if np.any(row_count != props.total_rows):
        result["rows"] = _fill_missed(row_count.map(lambda n: f"{n:,}"))
    if stats["null_count"].sum() > 0:
        null_count = stats["null_count"]
        nulls = pd.Series(
            list(starmap(_fmt_count_percent, zip(null_count, row_count))), dtype=str, index=stats.index
        )
        result["nulls"] = _fill_missed(nulls)

    return result


def _join_catalog_uri(col_upath: str | None, path: str) -> str:
    if col_upath is None:
        return path

    try:
        upath = UPath(path)
    except ValueError:
        return path

    if upath.protocol:
        return path

    try:
        return str(UPath(col_upath) / path)
    except ValueError:
        return path


def _catalog_uris(properties: CollectionProperties, uri: str | None) -> dict[str, object]:
    margin_urls = (properties.all_margins or []).copy()
    if properties.default_margin is not None:
        default_margin_idx = margin_urls.index(properties.default_margin)
        margin_urls[0], margin_urls[default_margin_idx] = margin_urls[default_margin_idx], margin_urls[0]

    index_columns = list(properties.all_indexes or {})
    if properties.default_index is not None:
        default_index_idx = index_columns.index(properties.default_index)
        index_columns[0], index_columns[default_index_idx] = (
            index_columns[default_index_idx],
            index_columns[0],
        )

    return {
        "collection": uri or "<PATH>",
        "primary": {
            "name": properties.hats_primary_table_url,
            "uri": _join_catalog_uri(uri, properties.hats_primary_table_url),
        },
        "margins": [
            {
                "name": margin,
                "uri": _join_catalog_uri(uri, margin),
            }
            for margin in margin_urls
        ],
        "indexes": [
            {
                "column": column,
                "name": properties.all_indexes[column],
                "uri": _join_catalog_uri(uri, properties.all_indexes[column]),
            }
            for column in index_columns
        ],
    }


def _get_example_frame(catalog: Dataset, rng: np.random.Generator) -> npd.NestedFrame | None:
    if (root := catalog.catalog_path) is None or not root.exists():
        return None

    if (thumbnail_path := get_data_thumbnail_pointer(root)).exists():
        return read_parquet_file_to_pandas(thumbnail_path, is_dir=False)

    if not isinstance(catalog, HealpixDataset):
        return None

    healpix_pixels = catalog.get_healpix_pixels()
    pixel = rng.choice(healpix_pixels)
    return catalog.read_pixel_to_pandas(pixel)


def _get_example_row(catalog: HealpixDataset) -> npd.NestedFrame | None:
    """Returns a single-row nested frame with a random example row."""
    random_seed = 42
    rng = np.random.Generator(np.random.PCG64(random_seed))

    example_nf = _get_example_frame(catalog, rng)

    if example_nf is None:
        return None

    idx = rng.integers(len(example_nf))
    return example_nf.iloc[idx : idx + 1]
