import pytest

from hats import read_hats
from hats.catalog import (
    Catalog,
    CatalogCollection,
    CatalogExtension,
    CatalogType,
    CollectionProperties,
    ExtensionProperties,
)


def test_read_extension(small_sky_extension_file, small_sky_o1_with_extension_dir):
    extension = read_hats(small_sky_extension_file)
    assert isinstance(extension, CatalogExtension)
    assert extension.config_path.path == small_sky_extension_file.as_posix()
    assert isinstance(extension.extension_info, ExtensionProperties)
    assert (
        extension.join_catalog_dir.path
        == (small_sky_o1_with_extension_dir / "small_sky_order1_errors").as_posix()
    )
    # The extension is a collection, valid on its own.
    assert isinstance(extension.catalog, CatalogCollection)
    main_catalog = extension.catalog.main_catalog
    assert isinstance(main_catalog, Catalog)
    assert main_catalog.catalog_info.catalog_type == CatalogType.OBJECT
    assert extension.catalog.all_margins == [
        "small_sky_order1_errors_margin",
        "small_sky_order1_errors_margin_10arcs",
    ]


def test_read_collection_extensions(small_sky_o1_with_extension_dir):
    """A collection lists its extensions by name, each described by a file at its root."""
    collection = read_hats(small_sky_o1_with_extension_dir)
    assert collection.all_extensions == ["small_sky_order1_errors"]
    extension_path = collection.get_extension_path("small_sky_order1_errors")
    expected_path = small_sky_o1_with_extension_dir / "small_sky_order1_errors.properties"
    assert extension_path.path == expected_path.as_posix()
    extension = read_hats(extension_path)
    assert isinstance(extension, CatalogExtension)
    assert extension.extension_info.primary_catalog == collection.collection_properties.name


def test_extension_file_elsewhere(tmp_path, small_sky_o1_with_extension_dir, extension_info_data):
    """An extension file can live anywhere, and point to its data by absolute path."""
    data_dir = (small_sky_o1_with_extension_dir / "small_sky_order1_errors").as_posix()
    ExtensionProperties(**(extension_info_data | {"join_catalog": data_dir})).to_properties_file(tmp_path)
    extension = read_hats(tmp_path / "small_sky_order1_errors.properties")
    assert extension.join_catalog_dir.path == data_dir
    assert extension.catalog.main_catalog.catalog_info.catalog_name == "small_sky_order1_errors"
    assert extension.catalog.all_margins == [
        "small_sky_order1_errors_margin",
        "small_sky_order1_errors_margin_10arcs",
    ]


def test_collection_extension_elsewhere(tmp_path, small_sky_o1_with_extension_dir, extension_info_data):
    """A collection can list an extension file that lives outside of it, by absolute path."""
    extension_dir = tmp_path / "extensions"
    extension_dir.mkdir()
    data_dir = (small_sky_o1_with_extension_dir / "small_sky_order1_errors").as_posix()
    ExtensionProperties(**(extension_info_data | {"join_catalog": data_dir})).to_properties_file(
        extension_dir
    )
    extension_file = (extension_dir / "small_sky_order1_errors.properties").as_posix()

    collection_dir = tmp_path / "collection"
    collection_dir.mkdir()
    CollectionProperties(
        name="small_sky_elsewhere",
        hats_primary_table_url=(small_sky_o1_with_extension_dir / "small_sky_order1").as_posix(),
        all_extensions=[extension_file],
    ).to_properties_file(collection_dir)

    extension_path = read_hats(collection_dir).get_extension_path("small_sky_order1_errors")
    assert extension_path.path == extension_file
    assert isinstance(read_hats(extension_path), CatalogExtension)


def test_collection_with_unknown_extension(small_sky_o1_with_extension_dir):
    collection = read_hats(small_sky_o1_with_extension_dir)
    with pytest.raises(ValueError, match="not specified in all_extensions"):
        collection.get_extension_path("small_sky_order1_spectra")


def test_collection_without_extensions(small_sky_collection_dir):
    collection = read_hats(small_sky_collection_dir)
    assert collection.all_extensions is None
    with pytest.raises(ValueError, match="not specified in all_extensions"):
        collection.get_extension_path("small_sky_order1_errors")


def test_extension_data_must_be_catalog(tmp_path, small_sky_o1_with_extension_dir, extension_info_data):
    """The extension data is a catalog or a collection, and not e.g. a margin."""
    margin_dir = (small_sky_o1_with_extension_dir / "small_sky_order1_margin").as_posix()
    ExtensionProperties(**(extension_info_data | {"join_catalog": margin_dir})).to_properties_file(tmp_path)
    with pytest.raises(TypeError, match="not of type"):
        read_hats(tmp_path / "small_sky_order1_errors.properties")
