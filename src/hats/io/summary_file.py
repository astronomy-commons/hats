import base64
import importlib.resources
import io
from pathlib import Path
from typing import Literal

import jinja2
import pandas as pd
from upath import UPath

from hats.catalog.catalog_collection import CatalogCollection
from hats.catalog.index.index_catalog import IndexCatalog
from hats.catalog.margin_cache.margin_catalog import MarginCatalog
from hats.io import get_common_metadata_pointer, get_partition_info_pointer, templates
from hats.io._summary_builders import (
    _catalog_uris,
    _cone_code_example,
    _gen_column_table,
    _gen_metadata_table,
)
from hats.io.file_io import get_upath, read_parquet_file_to_pandas
from hats.loaders.read_hats import read_hats


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


# pylint: disable=import-outside-toplevel,import-error
def write_skymap_png(catalog_path: str | Path | UPath) -> None:
    """Write a ``skymap.png`` pixel coverage map to the catalog directory.

    Parameters
    ----------
    catalog_path : str | Path | UPath
        Path to the catalog directory. The PNG will be written alongside
        the catalog's other files.
    """
    import matplotlib.pyplot as plt

    catalog = read_hats(get_upath(catalog_path))
    inner = catalog.main_catalog if isinstance(catalog, CatalogCollection) else catalog

    fig, _ = inner.plot_pixels()
    with (get_upath(catalog_path) / "skymap.png").open("wb") as f:
        fig.savefig(f, format="png", bbox_inches="tight")
    plt.close(fig)


# pylint: disable=import-outside-toplevel,import-error
def write_partition_info_png(catalog_path: str | Path | UPath) -> None:
    """Write a ``partition_info.png`` angular density map to the catalog directory.

    Parameters
    ----------
    catalog_path : str | Path | UPath
        Path to the catalog directory. The PNG will be written alongside
        the catalog's other files.
    """
    import matplotlib.pyplot as plt
    from matplotlib.colors import LogNorm

    from hats.inspection.visualize_catalog import plot_density

    catalog = read_hats(get_upath(catalog_path))
    inner = catalog.main_catalog if isinstance(catalog, CatalogCollection) else catalog

    fig, _ = plot_density(inner, norm=LogNorm(), edgecolors="face")
    with (get_upath(catalog_path) / "partition_info.png").open("wb") as f:
        fig.savefig(f, format="png", bbox_inches="tight")
    plt.close(fig)


def generate_summary(
    catalog,
    *,
    fmt: Literal["markdown", "html"] | None = None,
    name: str,
    description: str,
    uri: str | None,
    huggingface_metadata: bool,
    jinja2_template: str | None = None,
    extra_template_vars: dict | None = None,
) -> str:
    """Generate summary content for any HATS catalog or collection.

    Parameters
    ----------
    catalog : Catalog | CatalogCollection
        The HATS catalog or collection to summarize.
    fmt : Literal["markdown", "html"] | None
        Output format. Use ``"markdown"`` or ``"html"`` to render the built-in
        template, or ``None`` to supply a fully custom ``jinja2_template``.
    name : str
        Human-readable name rendered in the summary.
    description : str
        Description rendered in the summary.
    uri : str | None
        URI of the catalog used for hyperlinks and code-snippet examples.
        If ``None``, a placeholder is used.
    huggingface_metadata : bool
        Whether to include Hugging Face YAML frontmatter. Only valid when
        ``fmt="markdown"``.
    jinja2_template : str | None
        Jinja2 template string or path to a ``.jinja2`` file. If a file path
        is given, the file is read automatically. Overrides the built-in
        template when ``fmt`` is ``"markdown"`` or ``"html"``; required when
        ``fmt=None``.
    extra_template_vars : dict | None
        Additional variables passed into ``template.render()``. Useful for
        custom templates (e.g. VO registry XML fields) that reference variables
        HATS does not supply by default.

    Returns
    -------
    str
        Rendered summary content.
    """
    if isinstance(catalog, CatalogCollection):
        md_tmpl, html_tmpl = "collection_md_template.jinja2", "collection_html_template.jinja2"
    elif isinstance(catalog, MarginCatalog):
        md_tmpl, html_tmpl = "margin_md_template.jinja2", "margin_html_template.jinja2"
    elif isinstance(catalog, IndexCatalog):
        md_tmpl, html_tmpl = "index_md_template.jinja2", "index_html_template.jinja2"
    else:
        md_tmpl, html_tmpl = "catalog_md_template.jinja2", "catalog_html_template.jinja2"
    env = jinja2.Environment(undefined=jinja2.StrictUndefined)

    if jinja2_template is not None and Path(jinja2_template).exists():
        jinja2_template = Path(jinja2_template).read_text(encoding="utf-8")
        # sets jinja_template as a file as a path
    match fmt:
        case "markdown":
            tmpl_str = jinja2_template or importlib.resources.read_text(templates, md_tmpl)
        case "html":
            tmpl_str = jinja2_template or importlib.resources.read_text(templates, html_tmpl)
        case None:
            tmpl_str = jinja2_template
        case _:
            raise ValueError(f"Unsupported format: {fmt!r}. Expected 'markdown', 'html', or None.")
    template = env.from_string(tmpl_str)

    is_collection = isinstance(catalog, CatalogCollection)
    inner = catalog.main_catalog if is_collection else catalog
    cat_props = inner.catalog_info
    catalog_path = catalog.main_catalog_dir if is_collection else catalog.catalog_path

    empty_nf = (
        read_parquet_file_to_pandas(p) if (p := get_common_metadata_pointer(catalog_path)).exists() else None
    )
    column_table = _gen_column_table(inner, empty_nf)
    col_props = catalog.collection_properties if is_collection else None
    needs_sky = not isinstance(catalog, (MarginCatalog, IndexCatalog))
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
        column_table=pd.DataFrame() if isinstance(catalog, IndexCatalog) else column_table,
        catalog_dir_name=None if is_collection else catalog.catalog_path.name,
        has_default_columns=bool(cat_props.default_columns) if needs_sky else None,
        cone_code_example=cone_code_example,
        pixel_map_b64=pixel_map_b64,
        density_map_b64=density_map_b64,
        col_props=col_props,
        uris=_catalog_uris(col_props, uri) if is_collection else None,
        margin_thresholds=catalog.get_margin_thresholds() if is_collection else None,
        **(extra_template_vars or {}),
    )


def write_catalog_summary_file(
    catalog_path: str | Path | UPath,
    *,
    fmt: Literal["markdown", "html"] | None,
    filename: str | None = None,
    output_dir: str | Path | UPath | None = None,
    name: str | None = None,
    description: str | None = None,
    uri: str | None = None,
    huggingface_metadata: bool = False,
    jinja2_template: str | None = None,
    extra_template_vars: dict | None = None,
) -> UPath:
    """Write a summary readme file for any HATS catalog or collection.

    Parameters
    ----------
    catalog_path : str | Path | UPath
        Path to the HATS catalog or collection directory.
    fmt : Literal["markdown", "html"] | None
        Output format. Use ``"markdown"`` (writes ``README.md``) or ``"html"``
        (writes ``index.html``), or ``None`` to supply a fully custom
        ``jinja2_template`` and ``filename``.
    filename : str | None
        Output filename. Defaults to ``README.md`` for markdown and
        ``index.html`` for HTML. Required when ``fmt=None``.
    output_dir : str | Path | UPath | None
        Directory to write the file to. Defaults to ``catalog_path``.
    name : str | None
        Human-readable name. Inferred from catalog metadata if not provided.
    description : str | None
        Description. Inferred from catalog metadata if not provided.
    uri : str | None
        URI of the catalog used for hyperlinks and code-snippet examples.
        If ``None``, a placeholder is used.
    huggingface_metadata : bool
        Whether to include Hugging Face YAML frontmatter. Only valid when
        ``fmt="markdown"``.
    jinja2_template : str | None
        Jinja2 template string or path to a ``.jinja2`` file. If a file path
        is given, the file is read automatically. Overrides the built-in
        template when ``fmt`` is ``"markdown"`` or ``"html"``; required when
        ``fmt=None``.
    extra_template_vars : dict | None
        Additional variables passed into ``template.render()``. Useful for
        custom templates (e.g. VO registry XML fields) that reference variables
        HATS does not supply by default.

    Returns
    -------
    UPath
        Path to the written summary file.
    """
    from hats.catalog.catalog import Catalog

    catalog_path = get_upath(catalog_path)
    if fmt != "markdown" and huggingface_metadata:
        raise ValueError("`huggingface_metadata=True` is supported only for `fmt='markdown'`")
    match fmt:
        case "markdown":
            filename = filename or "README.md"
        case "html":
            filename = filename or "index.html"
        case None:
            if jinja2_template is None:
                raise ValueError("`jinja2_template` is required when `fmt` is None.")
            if filename is None:
                raise ValueError("`filename` is required when `fmt` is None.")
        case _:
            raise ValueError(f"Unsupported format: {fmt!r}. Expected 'markdown', 'html', or None.")
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
        extra_template_vars=extra_template_vars,
    )

    output_dir = catalog_path if output_dir is None else get_upath(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / filename

    with output_path.open("w") as f:
        f.write(content)

    return output_path
