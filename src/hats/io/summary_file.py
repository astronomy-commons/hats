from pathlib import Path
from typing import Literal

import yaml
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
    description : str | None, default=None :
        Description of the catalog. By default, generated based on catalog
        metadata. The default is a subject of frequent changes, do not rely
        on it.
    huggingface_metadata : bool, optional
        Whether to include Hugging Face specific metadata header in
        the markdown file, by default False. Supported only when
        `fmt="markdown"`.

    Returns
    -------
    UPath
        The path to the written summary file.

    Notes
    -----

    1. Not all options are supported for all formats.
    2. Default string generation is a subject of frequent changes, including
       patch releases, so do not rely on them for the reproducible results.
       However, we are trying to make them as useful as possible.
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


def generate_markdown_collection_summary(
    collection: CatalogCollection,
    *,
    title: str,
    description: str,
    huggingface_metadata: bool,
) -> str:
    """Generate markdown summary content for a HATS collection.

    Parameters
    ----------
    title : str
        Title of the markdown document.
    description : str
        Description of the catalog.
    huggingface_metadata : bool
        Whether to include Hugging Face specific metadata header in
        the markdown file.
    """
    snippets = []
    if huggingface_metadata:
        hf_yaml = generate_hugging_face_yaml_metadata(collection)
        snippets.append(f"---\n" f"{hf_yaml}\n" f"---\n")
    snippets.append(f"# {title}")
    snippets.append(f"{description}")

    # Should be extended in the future to include sections like:
    # - Load code examples
    # - File structure
    # - Statistics
    # - Column schema
    # - Sky maps
    # See https://github.com/astronomy-commons/hats/issues/615

    return "\n".join(snippets)


def generate_hugging_face_yaml_metadata(collection: CatalogCollection) -> str:
    """Generate Hugging Face specific YAML "frontmatter" for a HATS collection.

    Parameters
    ----------
    collection : CatalogCollection
        The HATS collection for which to generate the metadata.

    Returns
    -------
    str
        The generated YAML metadata as a string.
    """
    props = collection.collection_properties

    # Configs specify different datasets within a single HF repository.
    # We set default config to be the HATS catalog, and others to be
    # other collection catalogs like margin and index.
    configs = [
        {
            "config_name": "default",
            # We use string concatenation here, because we always need "/", not current
            # OS-specific separator.
            "data_dir": f"{props.hats_primary_table_url}/dataset",
        }
    ]
    if props.all_margins is not None:
        for margin in props.all_margins:
            configs.append(
                {
                    "config_name": margin,
                    "data_dir": f"{margin}/dataset",
                }
            )
    if props.all_indexes is not None:
        for index in props.all_indexes.values():
            configs.append(
                {
                    "config_name": index,
                    "data_dir": f"{index}/dataset",
                }
            )

    data = {
        "configs": configs,
        "tags": ["astronomy"],
    }
    return yaml.dump(data, sort_keys=False, default_flow_style=False)
