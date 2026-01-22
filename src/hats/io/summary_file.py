from pathlib import Path
from typing import Literal

import jinja2
from upath import UPath

from hats.catalog.catalog_collection import CatalogCollection
from hats.io.file_io import get_upath
from hats.loaders.read_hats import read_hats


def write_collection_summary_file(
    collection_path: str | Path | UPath,
    *,
    fmt: Literal["markdown"],
    filename: str | None = None,
    title: str | None = None,
    description: str | None = None,
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
    title : str | None, default=None
        Title of the summary document. By default, generated based on catalog
        name. This default is a subject of frequent changes, do not rely on it.
    description : str | None, default=None
        Description of the catalog. By default, generated based on catalog
        metadata. The default is a subject of frequent changes, do not rely
        on it.
    huggingface_metadata : bool, default=False
        Whether to include Hugging Face specific metadata header in
        the Markdown file, by default False. Supported only when
        `fmt="markdown"`.
    jinja2_template : str, default=NOne
        `jinja2` template string to use for generating the summary file.
        If provided, it would override the default template:
        - `DEFAULT_MD_TEMPLATE` for `fmt="markdown"`.

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

    name = collection.collection_properties.name
    if title is None:
        title = f"{name} HATS catalog"
    if description is None:
        # Should be extended in the future to include more details.
        description = f"This is the `{name}` HATS collection."

    match fmt:
        case "markdown":
            content = generate_markdown_collection_summary(
                collection=collection,
                title=title,
                description=description,
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

    output_path = collection_path / filename

    with output_path.open("w") as f:
        f.write(content)

    return output_path


# Should be extended in the future to include sections like:
# - Load code examples
# - File structure
# - Statistics
# - Column schema
# - Sky maps
# See https://github.com/astronomy-commons/hats/issues/615
DEFAULT_MD_TEMPLATE = """
{%- if huggingface_metadata %}
---
configs:
- config_name: default
  data_dir: {{primary_table}}/dataset
{%- for margin in all_margins %}
- config_name: {{margin}}
  data_dir: {{margin}}/dataset
{%- endfor %}
{%- for index in all_indexes %}
- config_name: {{index}}
  data_dir: {{index}}/dataset
{%- endfor %}
tags:
- astronomy
---
{%- endif %}

# {{title}}

{{description}}
"""


def generate_markdown_collection_summary(
    collection: CatalogCollection,
    *,
    title: str,
    description: str,
    huggingface_metadata: bool,
    jinja2_template: str | None = None,
) -> str:
    """Generate Markdown summary content for a HATS collection.

    Parameters
    ----------
    title : str
        Title of the Markdown document.
    description : str
        Description of the catalog.
    huggingface_metadata : bool
        Whether to include Hugging Face specific metadata header in
        the Markdown file.
    jinja2_template : str | None

    """
    props = collection.collection_properties
    env = jinja2.Environment(undefined=jinja2.StrictUndefined)
    if jinja2_template is None:
        jinja2_template = DEFAULT_MD_TEMPLATE
    template = env.from_string(jinja2_template)

    all_margins = props.all_margins or []
    all_indexes = list((props.all_indexes or {}).values())

    return template.render(
        title=title,
        description=description,
        primary_table=props.hats_primary_table_url,
        all_margins=all_margins,
        all_indexes=all_indexes,
        huggingface_metadata=huggingface_metadata,
    )
