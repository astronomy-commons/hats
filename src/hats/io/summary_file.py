import base64
import importlib.resources
import io
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
from hats.catalog.dataset import Dataset
from hats.catalog.healpix_dataset.healpix_dataset import HealpixDataset
from hats.catalog.index.index_catalog import IndexCatalog
from hats.catalog.margin_cache.margin_catalog import MarginCatalog
from hats.io import get_common_metadata_pointer, get_partition_info_pointer, templates
from hats.io.file_io import get_upath, read_parquet_file_to_pandas
from hats.io.paths import get_data_thumbnail_pointer
from hats.loaders.read_hats import read_hats


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
        parts = [_format_example_value(items[0], **fmt_kwargs)]
        for item in items[1:]:
            candidate = _format_example_value(item, **fmt_kwargs)
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


# pylint: disable=import-outside-toplevel,import-error
def _generate_sky_coverage_images(catalog, name: str | None = None):
    from matplotlib.colors import LogNorm

    from hats.inspection.visualize_catalog import plot_density

    pixel_title = f"Catalog pixel map - {name}" if name else None
    density_title = f"Angular density of catalog {name}" if name else None
    fig, _ = catalog.plot_pixels(plot_title=pixel_title)
    pixel_map_b64 = _fig_to_webp_base64(fig)
    fig, _ = plot_density(catalog, norm=LogNorm(), edgecolors="face", plot_title=density_title)
    density_map_b64 = _fig_to_webp_base64(fig)
    return pixel_map_b64, density_map_b64


# pylint: disable=import-outside-toplevel,import-error
def _fig_to_webp_base64(fig) -> str:
    import matplotlib.pyplot as plt

    buffer = io.BytesIO()
    fig.savefig(buffer, format="webp", bbox_inches="tight")
    plt.close(fig)
    return base64.b64encode(buffer.getvalue()).decode("ascii")


def _load_empty_nf(catalog_path) -> "npd.NestedFrame | None":
    """Reads the common metadata parquet file from a catalog directory and returns it as a nested pd"""
    if (p := get_common_metadata_pointer(catalog_path)).exists():
        return read_parquet_file_to_pandas(p)
    return None


def _load_template(jinja2_template: str | None, default_name: str) -> jinja2.Template:
    """Loads a Jinja2 temlate and returns it ready to call .read(). Also allows custom Jinja2 templates."""
    env = jinja2.Environment(undefined=jinja2.StrictUndefined)
    if jinja2_template is None:
        jinja2_template = importlib.resources.read_text(templates, default_name)
    return env.from_string(jinja2_template)


def generate_summary(
    catalog,
    *,
    fmt: Literal["markdown", "html"],
    name: str,
    description: str,
    uri: str | None,
    huggingface_metadata: bool,
    jinja2_template: str | None = None,
) -> str:
    """Generate summary content for any HATS catalog or collection."""
    if isinstance(catalog, CatalogCollection):
        md_tmpl, html_tmpl = "collection_md_template.jinja2", "collection_html_template.jinja2"
    elif isinstance(catalog, MarginCatalog):
        md_tmpl, html_tmpl = "margin_md_template.jinja2", "margin_html_template.jinja2"
    elif isinstance(catalog, IndexCatalog):
        md_tmpl, html_tmpl = "index_md_template.jinja2", "index_html_template.jinja2"
    else:
        md_tmpl, html_tmpl = "catalog_md_template.jinja2", "catalog_html_template.jinja2"
    match fmt:
        case "markdown":
            template = _load_template(jinja2_template, md_tmpl)
        case "html":
            template = _load_template(jinja2_template, html_tmpl)
        case _:
            raise ValueError(f"Unsupported format: {fmt!r}. Expected 'markdown' or 'html'.")

    is_collection = isinstance(catalog, CatalogCollection)
    inner = catalog.main_catalog if is_collection else catalog
    cat_props = inner.catalog_info
    catalog_path = catalog.main_catalog_dir if is_collection else catalog.catalog_path

    empty_nf = _load_empty_nf(catalog_path)
    column_table = _gen_column_table(inner, empty_nf)
    col_props = catalog.collection_properties if is_collection else None
    needs_sky = not isinstance(catalog, (MarginCatalog, IndexCatalog))
    has_default_columns = bool(cat_props.default_columns) if needs_sky else None
    cone_code_example = _cone_code_example(column_table, cat_props) if needs_sky else None
    pixel_map_b64, density_map_b64 = None, None
    if needs_sky:
        try:
            pixel_map_b64, density_map_b64 = _generate_sky_coverage_images(inner, name)
        except ImportError:
            pass

    return template.render(
        name=name,
        description=description,
        cat_props=cat_props,
        uri=uri,
        has_partition_info=get_partition_info_pointer(catalog_path).exists(),
        huggingface_metadata=huggingface_metadata,
        metadata_table=(
            None
            if isinstance(catalog, IndexCatalog)
            else _gen_metadata_table(inner, total_columns=None if empty_nf is None else empty_nf.shape[1])
        ),
        column_table=column_table,
        catalog_dir_name=None if is_collection else catalog.catalog_path.name,
        has_default_columns=has_default_columns,
        cone_code_example=cone_code_example,
        pixel_map_b64=pixel_map_b64,
        density_map_b64=density_map_b64,
        col_props=col_props,
        uris=_catalog_uris(col_props, uri) if is_collection else None,
        margin_thresholds=catalog.get_margin_thresholds() if is_collection else None,
    )


def write_catalog_summary_file(
    catalog_path: str | Path | UPath,
    *,
    fmt: Literal["markdown", "html"],
    filename: str | None = None,
    output_dir: str | Path | UPath | None = None,
    name: str | None = None,
    description: str | None = None,
    uri: str | None = None,
    huggingface_metadata: bool = False,
    jinja2_template: str | None = None,
) -> UPath:
    """Write a summary readme file for any HATS catalog or collection"""
    from hats.catalog.catalog import Catalog

    catalog_path = get_upath(catalog_path)
    if fmt != "markdown" and huggingface_metadata:
        raise ValueError("`huggingface_metadata=True` is supported only for `fmt='markdown'`")
    catalog = read_hats(catalog_path)

    if isinstance(catalog, CatalogCollection):
        name = name or catalog.collection_properties.name
        description = description or f"This is the collection of HATS catalogs representing {name}."
    elif isinstance(catalog, MarginCatalog):
        name = name or catalog.catalog_info.catalog_name
        description = description or f"This is the margin catalog for {name.removesuffix('_margin')}."
    elif isinstance(catalog, IndexCatalog):
        name = name or catalog.catalog_info.catalog_name
        indexing_column = catalog.catalog_info.indexing_column
        primary = catalog.catalog_info.primary_catalog
        description = description or (
            f"This index maps {indexing_column} values to partitions in the "
            f"{primary} catalog for non-spatial lookups."
        )
    elif isinstance(catalog, Catalog):
        name = name or catalog.catalog_info.catalog_name
        description = description or f"This is the HATS catalog for {name}."

    content = generate_summary(
        catalog,
        fmt=fmt,
        name=name,
        description=description,
        uri=uri,
        huggingface_metadata=huggingface_metadata,
        jinja2_template=jinja2_template,
    )

    if filename is None:
        match fmt:
            case "markdown":
                filename = "README.md"
            case "html":
                filename = "index.html"
            case _:
                raise ValueError(f"Unsupported format: {fmt!r}. Expected 'markdown' or 'html'.")
    output_dir = catalog_path if output_dir is None else get_upath(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / filename

    with output_path.open("w") as f:
        f.write(content)

    return output_path
