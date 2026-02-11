import importlib.resources
from itertools import starmap
from pathlib import Path
from typing import Literal

import human_readable
import jinja2
import nested_pandas as npd
import numpy as np
import pandas as pd
from upath import UPath

from hats.catalog import CollectionProperties
from hats.catalog.catalog_collection import CatalogCollection
from hats.catalog.healpix_dataset.healpix_dataset import HealpixDataset
from hats.io import get_common_metadata_pointer, get_partition_info_pointer, templates
from hats.io.file_io import get_upath, read_parquet_file_to_pandas
from hats.io.paths import get_data_thumbnail_pointer
from hats.loaders.read_hats import read_hats


def write_collection_summary_file(
    collection_path: str | Path | UPath,
    *,
    fmt: Literal["markdown"],
    filename: str | None = None,
    output_dir: str | Path | UPath | None = None,
    name: str | None = None,
    description: str | None = None,
    uri: str | None = None,
    huggingface_metadata: bool = False,
    jinja2_template: str | None = None,
) -> UPath:
    """Write a summary readme file for a HATS catalog.

    Parameters
    ----------
    collection_path: str | Path | UPath
        The path to the HATS collection.
    fmt : str
        The format of the summary file. Currently only "markdown" is supported.
    filename: str | None, default=None
        The name of the summary file. If None, default depends on a `fmt`:
        - "README.md" for "markdown" format.
    output_dir : str | Path | UPath | None
        The root directory to output the summary file to. If None, the summary
        file would be written to the `collection_path`. If the directory does
        not exist, it would be created.
    name : str | None, default=None
        Human-readable name of the catalog. By default, generated based on
        catalog metadata.
    description : str | None, default=None
        Description of the catalog. By default, generated based on catalog
        metadata.
    uri : str | None, default=None
        URI of the catalog to use for the hyperlinks and code-snippet examples.
        Not validated. If None, a placeholder would be used for
        the code-snippets.
    huggingface_metadata : bool, default=False
        Whether to include Hugging Face specific metadata header in
        the Markdown file, by default False. Supported only when
        `fmt="markdown"`.
    jinja2_template : str, default=None
        `jinja2` template string to use for generating the summary file.
        If provided, it would override the default template from these
        functions:
        - `default_md_template()` for `fmt="markdown"`.

    Returns
    -------
    UPath
        The path to the written summary file.

    Notes
    -----

    1. Not all options are supported for all formats.
    2. Default template is the subject of frequent changes, do not rely on it.
    """
    collection_path = get_upath(collection_path)
    if fmt != "markdown" and huggingface_metadata:
        raise ValueError("`huggingface_metadata=True` is supported only for `fmt='markdown'`")

    collection = read_hats(collection_path)
    if not isinstance(collection, CatalogCollection):
        raise ValueError(
            f"The provided path '{collection_path}' contains a HATS catalog, but not a collection.'"
        )

    if name is None:
        name = collection.collection_properties.name

    if description is None:
        description = f"This is the collection of HATS catalogs representing {name}."

    match fmt:
        case "markdown":
            content = generate_markdown_collection_summary(
                collection=collection,
                name=name,
                description=description,
                uri=uri,
                huggingface_metadata=huggingface_metadata,
                jinja2_template=jinja2_template,
            )
        case _:
            raise ValueError(f"Unsupported format: {fmt=}")

    if filename is None:
        match fmt:
            case "markdown":
                filename = "README.md"
            case _:
                raise ValueError(f"Unsupported format: {fmt=}")

    output_dir = collection_path if output_dir is None else get_upath(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / filename

    with output_path.open("w") as f:
        f.write(content)

    return output_path


def default_md_template() -> str:
    """Get the default Jinja2 template string for generating Markdown summary files.

    Returns
    -------
    str
        The default Jinja2 template string.
    """
    return importlib.resources.read_text(templates, "default_md_template.jinja2")


def generate_markdown_collection_summary(
    collection: CatalogCollection,
    *,
    name: str,
    description: str,
    uri: str | None,
    huggingface_metadata: bool,
    jinja2_template: str | None = None,
) -> str:
    """Generate Markdown summary content for a HATS collection.

    Parameters
    ----------
    collection : CatalogCollection
        HATS collection to generate summary for.
    name : str
        Title of the Markdown document.
    description : str
        Description of the catalog.
    uri : str | None
        URI of the catalog to use for the hyperlinks and code-snippet examples.
        Not validated. If None, a placeholder would be used for
        the code-snippets.
    huggingface_metadata : bool
        Whether to include Hugging Face specific metadata header in
        the Markdown file.
    jinja2_template : str | None

    """
    col_props = collection.collection_properties
    catalog = collection.main_catalog
    cat_props = catalog.catalog_info

    env = jinja2.Environment(undefined=jinja2.StrictUndefined)
    if jinja2_template is None:
        jinja2_template = default_md_template()
    template = env.from_string(jinja2_template)

    uris = _catalog_uris(col_props, uri)
    has_partition_info = get_partition_info_pointer(collection.main_catalog_dir).exists()
    margin_thresholds = collection.get_margin_thresholds()

    if (common_metadata := get_common_metadata_pointer(collection.main_catalog_dir)).exists():
        empty_nf = read_parquet_file_to_pandas(common_metadata)
    else:
        empty_nf = None

    metadata_table = _gen_md_metadata_table(
        catalog, total_columns=None if empty_nf is None else empty_nf.shape[1]
    )

    column_table = _gen_md_column_table(catalog, empty_nf)

    if "example" in column_table:
        ra = np.round(float(column_table.loc[cat_props.ra_column]["example"]))
        if ra >= 360.0:
            ra -= 360.0
        dec = np.round(float(column_table.loc[cat_props.dec_column]["example"]))
        if dec >= 90.0:
            dec = 89.9
        if dec <= -90.0:
            dec = -89.9
        cone_code_example = {"ra": ra, "dec": dec}
    else:
        cone_code_example = None

    return template.render(
        name=name,
        description=description,
        col_props=col_props,
        cat_props=cat_props,
        uris=uris,
        has_partition_info=has_partition_info,
        has_default_columns=bool(cat_props.default_columns),
        cone_code_example=cone_code_example,
        margin_thresholds=margin_thresholds,
        uri=uri,
        huggingface_metadata=huggingface_metadata,
        metadata_table=metadata_table,
        column_table=column_table,
    )


def _gen_md_metadata_table(catalog: HealpixDataset, total_columns: int | None) -> dict[str, object]:
    props = catalog.catalog_info
    has_healpix_column = props.healpix_column is not None

    metadata_table = {}
    if props.total_rows is not None:
        metadata_table["Number of rows"] = f"{props.total_rows:,}"
    if total_columns is not None:
        key = "Number of columns"
        # Exclude HEALPix index columns from the count
        value = f"{total_columns - int(has_healpix_column):,}"
        if props.default_columns is not None:
            key = "Number of columns (default columns)"
            value = f"{value} ({len(props.default_columns):,})"
        metadata_table[key] = value
    metadata_table["Number of partitions"] = f"{len(catalog.get_healpix_pixels()):,}"
    if (hats_estsize_kb := props.extra_dict().get("hats_estsize")) is not None:
        metadata_table["Size on disk"] = human_readable.file_size(int(hats_estsize_kb) * 1024, binary=True)
    if (hats_builder := props.extra_dict().get("hats_builder")) is not None:
        metadata_table["HATS Builder"] = hats_builder
    return metadata_table


def _fmt_count_percent(n: int, total: int) -> str:
    if n == 0:
        return "0"
    percent = round(n / total * 100, 2)
    if percent < 0.01:
        return f"{n:,} (<0.01%)"
    return f"{n:,} ({percent}%)"


def _hard_truncate(s: str, limit: int) -> str:
    if len(s) <= limit:
        return s
    return s[: limit - 1] + "…"


def _format_example_value(
    value, *, float_precision: int = 4, soft_limit: int = 50, hard_limit: int = 70
) -> str:
    """Format an example value for display in a summary table.

    Floats are rounded to a limited number of significant figures.
    Lists are shown with as many items as fit within ``soft_limit``
    characters (always at least one), with a ``(N total)`` suffix when
    truncated. Any resulting string longer than ``hard_limit`` is
    truncated with ``…``.
    """
    if value is None:
        return "*NULL*"

    if isinstance(value, (float, np.floating)):
        if np.isnan(value):
            return "*NaN*"
        if np.isinf(value):
            return "-∞" if value < 0 else "∞"
        return f"{value:.{float_precision}g}"

    if isinstance(value, (list, tuple, np.ndarray)):
        items = list(value)
        if len(items) == 0:
            return "[]"
        fmt_kwargs = {"float_precision": float_precision, "soft_limit": soft_limit, "hard_limit": hard_limit}
        suffix = f", … ({len(items)} total)]"
        # Always include at least one item
        parts = [_format_example_value(items[0], **fmt_kwargs)]
        for item in items[1:]:
            candidate = _format_example_value(item, **fmt_kwargs)
            # Check if adding this item would exceed the soft limit,
            # accounting for the truncation suffix
            preview = "[" + ", ".join(parts + [candidate]) + suffix
            if len(preview) > soft_limit:
                break
            parts.append(candidate)
        if len(parts) < len(items):
            result = "[" + ", ".join(parts) + suffix
        else:
            result = "[" + ", ".join(parts) + "]"
    else:
        result = str(value)

    return _hard_truncate(result, hard_limit)


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
            example.extend(fmt_value(series.to_list()) for _, series in cell.items())
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


def _gen_md_column_table(
    catalog: HealpixDataset, empty_nf: npd.NestedFrame | None, fmt_value=_format_example_value
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

    # If path is an absolute URI, return it as-is
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


def _get_example_frame(catalog: HealpixDataset, rng: np.random.Generator) -> npd.NestedFrame | None:
    if (root := catalog.catalog_path) is None or not root.exists():
        return None

    if (thumbnail_path := get_data_thumbnail_pointer(root)).exists():
        return read_parquet_file_to_pandas(thumbnail_path, is_dir=False)

    healpix_pixels = catalog.get_healpix_pixels()
    pixel = rng.choice(healpix_pixels)
    return catalog.read_pixel_to_pandas(pixel)


def _get_example_row(catalog: HealpixDataset) -> npd.NestedFrame | None:
    """Returns a single-row nested frame with a random example row."""
    # We want it to be pseudo-random but reproducible
    random_seed = 42
    rng = np.random.Generator(np.random.PCG64(random_seed))

    example_nf = _get_example_frame(catalog, rng)

    if example_nf is None:
        return None

    idx = rng.integers(len(example_nf))
    return example_nf.iloc[idx : idx + 1]
