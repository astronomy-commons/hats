"""Tests for summary file generation"""

import re
import shutil

import pytest
import yaml

from hats.io.summary_file import generate_markdown_collection_summary, write_collection_summary_file
from hats.loaders import read_hats


def extract_yaml_from_readme(readme_path):
    """Extract YAML frontmatter from a README file.

    The YAML is expected to be between the first two "---" lines.
    """
    content = readme_path.read_text()

    # Use regex to find YAML frontmatter between --- delimiters
    match = re.search(r"^---\s*\n(.*?)\n---", content, re.MULTILINE | re.DOTALL)

    if not match:
        raise ValueError("README file does not contain YAML frontmatter (no closing '---')")

    yaml_content = match.group(1)
    return yaml.safe_load(yaml_content)


def test_write_collection_summary_file_markdown(tmp_path, small_sky_collection_dir):
    """Test writing a basic markdown summary file for a collection"""
    collection_base_dir = tmp_path / "collection"
    shutil.copytree(small_sky_collection_dir, collection_base_dir)

    output_path = write_collection_summary_file(
        collection_base_dir,
        fmt="markdown",
    )

    assert output_path.exists()
    assert output_path.name == "README.md"
    assert output_path.parent == collection_base_dir

    # Check that the file has basic content
    content = output_path.read_text()
    assert "small_sky_o1_collection HATS catalog" in content
    assert "This is the `small_sky_o1_collection` HATS collection." in content


def test_write_collection_summary_file_custom_filename(tmp_path, small_sky_collection_dir):
    """Test writing a summary file with a custom filename"""
    collection_base_dir = tmp_path / "collection"
    shutil.copytree(small_sky_collection_dir, collection_base_dir)

    output_path = write_collection_summary_file(
        collection_base_dir,
        fmt="markdown",
        filename="CUSTOM.md",
    )

    assert output_path.exists()
    assert output_path.name == "CUSTOM.md"
    assert output_path.parent == collection_base_dir


def test_write_collection_summary_file_custom_title_description(tmp_path, small_sky_collection_dir):
    """Test writing a summary file with custom title and description"""
    collection_base_dir = tmp_path / "collection"
    shutil.copytree(small_sky_collection_dir, collection_base_dir)

    custom_title = "My Custom Title"
    custom_description = "This is a custom description for the catalog."

    output_path = write_collection_summary_file(
        collection_base_dir,
        fmt="markdown",
        title=custom_title,
        description=custom_description,
    )

    content = output_path.read_text()
    assert custom_title in content
    assert custom_description in content


def test_write_collection_summary_file_with_huggingface_metadata(tmp_path, small_sky_collection_dir):
    """Test writing a markdown summary file with Hugging Face metadata"""
    collection_base_dir = tmp_path / "collection"
    shutil.copytree(small_sky_collection_dir, collection_base_dir)

    output_path = write_collection_summary_file(
        collection_base_dir,
        fmt="markdown",
        huggingface_metadata=True,
    )

    content = output_path.read_text()
    # Check for YAML frontmatter
    assert content.strip().startswith("---\n")
    assert "configs:" in content
    assert "tags:" in content
    assert "astronomy" in content


def test_write_collection_summary_file_invalid_format(tmp_path, small_sky_collection_dir):
    """Test that invalid format raises an error"""
    collection_base_dir = tmp_path / "collection"
    shutil.copytree(small_sky_collection_dir, collection_base_dir)

    with pytest.raises(ValueError, match="Unsupported format"):
        write_collection_summary_file(
            collection_base_dir,
            fmt="xml",  # Not supported
        )


def test_write_collection_summary_file_unsupported_fits_format(tmp_path, small_sky_collection_dir):
    """Test that fits format raises an error for unsupported format"""
    collection_base_dir = tmp_path / "collection"
    shutil.copytree(small_sky_collection_dir, collection_base_dir)

    with pytest.raises(ValueError, match="Unsupported format"):
        write_collection_summary_file(
            collection_base_dir,
            fmt="fits",  # Not supported
        )


def test_generate_markdown_collection_summary_basic(small_sky_collection_dir):
    """Test generating markdown summary content"""
    collection = read_hats(small_sky_collection_dir)

    content = generate_markdown_collection_summary(
        collection=collection,
        title="Test Title",
        description="Test Description",
        huggingface_metadata=False,
    )

    assert "# Test Title" in content
    assert "Test Description" in content
    assert "---" not in content  # No YAML frontmatter


def test_generate_markdown_collection_summary_with_huggingface(small_sky_collection_dir):
    """Test generating markdown summary content with Hugging Face metadata"""
    collection = read_hats(small_sky_collection_dir)

    content = generate_markdown_collection_summary(
        collection=collection,
        title="Test Title",
        description="Test Description",
        huggingface_metadata=True,
    )

    assert content.strip().startswith("---\n")
    assert "# Test Title" in content
    assert "Test Description" in content


def test_generate_hugging_face_yaml_metadata(small_sky_collection_dir):
    """Test generating Hugging Face YAML metadata for a collection"""
    collection_base_dir = small_sky_collection_dir

    # Generate the summary file with Hugging Face metadata
    readme_path = write_collection_summary_file(
        collection_base_dir,
        fmt="markdown",
        huggingface_metadata=True,
    )

    # Extract YAML from README file
    data = extract_yaml_from_readme(readme_path)

    assert "configs" in data
    assert "tags" in data
    assert "astronomy" in data["tags"]

    # Check configs structure
    configs = data["configs"]
    assert isinstance(configs, list)
    assert len(configs) > 0

    # Check default config
    default_config = configs[0]
    assert default_config["config_name"] == "default"
    assert default_config["data_dir"] == "small_sky_order1/dataset"

    # Check margin configs
    margin_configs = [
        c
        for c in configs
        if c["config_name"] in ["small_sky_order1_margin", "small_sky_order1_margin_10arcs"]
    ]
    assert len(margin_configs) == 2

    # Check index configs
    index_configs = [c for c in configs if c["config_name"] == "small_sky_order1_id_index"]
    assert len(index_configs) == 1
    assert index_configs[0]["data_dir"] == "small_sky_order1_id_index/dataset"


def test_generate_hugging_face_yaml_metadata_no_margins(tmp_path, small_sky_collection_dir):
    """Test generating Hugging Face YAML metadata for a collection without margins"""
    collection_base_dir = tmp_path / "collection"
    shutil.copytree(small_sky_collection_dir, collection_base_dir)

    # Remove margins from collection properties
    collection = read_hats(collection_base_dir)
    collection.collection_properties.all_margins = None
    collection.collection_properties.default_margin = None
    collection.collection_properties.to_properties_file(collection_base_dir)

    # Re-read the collection
    collection = read_hats(collection_base_dir)

    # Generate the summary file with Hugging Face metadata
    readme_path = write_collection_summary_file(
        collection_base_dir,
        fmt="markdown",
        huggingface_metadata=True,
    )

    # Extract YAML from README file
    data = extract_yaml_from_readme(readme_path)

    # Check that only default config exists (no margin configs)
    configs = data["configs"]
    config_names = [c["config_name"] for c in configs]
    assert "default" in config_names
    assert "small_sky_order1_margin" not in config_names


def test_generate_hugging_face_yaml_metadata_no_indexes(tmp_path, small_sky_collection_dir):
    """Test generating Hugging Face YAML metadata for a collection without indexes"""
    collection_base_dir = tmp_path / "collection"
    shutil.copytree(small_sky_collection_dir, collection_base_dir)

    # Remove indexes from collection properties
    collection = read_hats(collection_base_dir)
    collection.collection_properties.all_indexes = None
    collection.collection_properties.default_index = None
    collection.collection_properties.to_properties_file(collection_base_dir)

    # Re-read the collection
    collection = read_hats(collection_base_dir)

    # Generate the summary file with Hugging Face metadata
    readme_path = write_collection_summary_file(
        collection_base_dir,
        fmt="markdown",
        huggingface_metadata=True,
    )

    # Extract YAML from README file
    data = extract_yaml_from_readme(readme_path)

    # Check that only default and margin configs exist (no index configs)
    configs = data["configs"]
    config_names = [c["config_name"] for c in configs]
    assert "default" in config_names
    assert "id" not in config_names


def test_write_collection_summary_file_with_non_collection_raises_error(small_sky_dir):
    """Test that ValueError is raised when a non-collection catalog path is passed"""
    with pytest.raises(ValueError, match="contains a HATS catalog, but not a collection"):
        write_collection_summary_file(
            small_sky_dir,
            fmt="markdown",
        )


def test_load_hats_collection_with_huggingface_datasets(tmp_path, small_sky_collection_dir):
    """Test loading HATS collection configs with Hugging Face datasets library"""
    datasets = pytest.importorskip("datasets")

    collection_base_dir = tmp_path / "collection"
    shutil.copytree(small_sky_collection_dir, collection_base_dir)

    # Generate the summary file with Hugging Face metadata
    write_collection_summary_file(
        collection_base_dir,
        fmt="markdown",
        huggingface_metadata=True,
    )

    # Load the collection to get expected row counts
    collection = read_hats(collection_base_dir)
    collection_props = collection.collection_properties

    # Test default config - load directly from collection directory using metadata
    dataset_default = datasets.load_dataset(
        str(collection_base_dir),
        name="default",
    )
    assert dataset_default is not None
    assert "train" in dataset_default
    assert "ra" in dataset_default["train"].column_names
    assert "dec" in dataset_default["train"].column_names

    # Verify exact row count matches the main catalog
    assert len(dataset_default["train"]) == collection.main_catalog.catalog_info.total_rows

    # Test margin config
    margin_name = collection_props.default_margin
    dataset_margin = datasets.load_dataset(
        str(collection_base_dir),
        name=margin_name,
    )
    assert dataset_margin is not None
    assert "train" in dataset_margin

    # Load margin catalog and verify exact row count
    margin_catalog = read_hats(collection_base_dir / margin_name)
    assert len(dataset_margin["train"]) == margin_catalog.catalog_info.total_rows

    # Test index config
    index_dir = collection_props.all_indexes[collection_props.default_index]
    dataset_index = datasets.load_dataset(
        str(collection_base_dir),
        name=index_dir,
    )
    assert dataset_index is not None
    assert "train" in dataset_index

    # Load index catalog and verify exact row count
    index_catalog = read_hats(collection_base_dir / index_dir)
    assert len(dataset_index["train"]) == index_catalog.catalog_info.total_rows
