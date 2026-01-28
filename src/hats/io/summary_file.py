import importlib.resources
from pathlib import Path
from typing import Literal

import human_readable
import jinja2
import nested_pandas as npd
import pandas as pd
from upath import UPath

from hats.catalog.catalog_collection import CatalogCollection
from hats.catalog.healpix_dataset.healpix_dataset import HealpixDataset
from hats.io import get_common_metadata_pointer, get_partition_info_pointer, templates
from hats.io.file_io import get_upath, read_parquet_file_to_pandas
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

    has_partition_info = get_partition_info_pointer(collection.main_catalog_dir).exists()
    margin_thresholds = collection.get_margin_thresholds()

    if (common_metadata := get_common_metadata_pointer(collection.main_catalog_dir)).exists():
        empty_nf = read_parquet_file_to_pandas(common_metadata)
    else:
        empty_nf = None

    has_nested_columns = False if empty_nf is None else len(empty_nf.nested_columns) > 0

    metadata_table = _gen_md_metadata_table(
        catalog, total_columns=None if empty_nf is None else empty_nf.shape[1]
    )

    column_table = (
        pd.DataFrame()
        if empty_nf is None
        else _gen_md_column_table(empty_nf, cat_props.default_columns or [])
    )

    return template.render(
        name=name,
        description=description,
        col_props=col_props,
        cat_props=cat_props,
        has_partition_info=has_partition_info,
        margin_thresholds=margin_thresholds,
        uri=uri,
        huggingface_metadata=huggingface_metadata,
        has_default_columns=cat_props.default_columns is not None,
        has_nested_columns=has_nested_columns,
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


def _gen_md_column_table(nf: npd.NestedFrame, default_columns: list[str]) -> pd.DataFrame:
    default_columns = frozenset(default_columns)

    column = []
    dtype = []
    default = []
    nested_into = []

    for name, dt in nf.dtypes.items():
        if isinstance(dt, npd.NestedDtype):
            subcolumns = nf.get_subcolumns(name)
            column.extend(subcolumns)
            dtype.extend(f"list[{nf[sc].dtype.pyarrow_dtype}]" for sc in subcolumns)
            default.extend(name in default_columns or sc in default_columns for sc in subcolumns)
            nested_into.extend([name] * len(subcolumns))
        else:
            column.append(name)
            dtype.append(str(dt.pyarrow_dtype))
            nested_into.append(None)
            default.append(name in default_columns)

    return pd.DataFrame({"column": column, "dtype": dtype, "default": default, "nested_into": nested_into})
