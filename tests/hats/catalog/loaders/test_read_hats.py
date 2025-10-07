import shutil

import pytest

import hats
from hats.catalog.catalog_collection import CatalogCollection
from hats.catalog.dataset.collection_properties import CollectionProperties
from hats.io.file_io import get_upath_for_protocol
from hats.loaders import read_hats


def test_read_hats_collection(small_sky_collection_dir, small_sky_order1_catalog):
    collection = read_hats(small_sky_collection_dir)
    assert isinstance(collection, CatalogCollection)
    assert collection.collection_path == small_sky_collection_dir
    assert collection.main_catalog_dir == small_sky_collection_dir / "small_sky_order1"
    assert collection.all_margins == ["small_sky_order1_margin"]
    assert collection.default_margin_catalog_dir == small_sky_collection_dir / "small_sky_order1_margin"
    assert collection.all_indexes == {"id": "small_sky_order1_id_index"}
    assert collection.default_index_field == "id"
    assert collection.default_index_catalog_dir == small_sky_collection_dir / "small_sky_order1_id_index"
    assert collection.get_healpix_pixels() == small_sky_order1_catalog.get_healpix_pixels()


def test_read_hats_collection_main_catalog_invalid(small_sky_collection_dir, tmp_path):
    """Test that the main catalog is of the correct `Catalog` type"""
    collection_base_dir = tmp_path / "collection"
    shutil.copytree(small_sky_collection_dir, collection_base_dir)
    assert collection_base_dir.exists()
    collection_properties = CollectionProperties.read_from_dir(collection_base_dir)
    collection_properties.hats_primary_table_url = "small_sky_order1_margin"
    collection_properties.to_properties_file(collection_base_dir)
    with pytest.raises(TypeError):
        read_hats(collection_base_dir)


def test_read_hats_default_margin_not_specified(small_sky_collection_dir, tmp_path):
    collection_base_dir = tmp_path / "collection"
    shutil.copytree(small_sky_collection_dir, collection_base_dir)
    assert collection_base_dir.exists()
    collection_properties = CollectionProperties.read_from_dir(collection_base_dir)
    collection_properties.default_margin = None
    collection_properties.to_properties_file(collection_base_dir)
    collection = read_hats(collection_base_dir)
    assert collection.default_margin_catalog_dir is None


def test_read_hats_default_index_not_specified(small_sky_collection_dir, tmp_path):
    collection_base_dir = tmp_path / "collection"
    shutil.copytree(small_sky_collection_dir, collection_base_dir)
    assert collection_base_dir.exists()
    collection_properties = CollectionProperties.read_from_dir(collection_base_dir)
    collection_properties.default_index = None
    collection_properties.to_properties_file(collection_base_dir)
    collection = read_hats(collection_base_dir)
    assert collection.default_index_catalog_dir is None


def test_read_hats_index_dir_for_field(small_sky_collection_dir, tmp_path):
    collection_base_dir = tmp_path / "collection"
    shutil.copytree(small_sky_collection_dir, collection_base_dir)
    assert collection_base_dir.exists()

    collection = read_hats(collection_base_dir)

    # If no field specified, the default index dir is returned
    assert collection.default_index_field == "id"
    assert collection.get_index_dir_for_field() == collection.get_index_dir_for_field("id")

    # There are indexes but none match the field name
    with pytest.raises(ValueError, match="not specified"):
        collection.get_index_dir_for_field("name")

    # There are no indexes available
    collection_properties = CollectionProperties.read_from_dir(collection_base_dir)
    collection_properties.all_indexes = None
    collection_properties.default_index = None
    collection_properties.to_properties_file(collection_base_dir)
    collection = read_hats(collection_base_dir)
    with pytest.raises(ValueError, match="not specified"):
        collection.get_index_dir_for_field("id")


def test_read_hats_collection_info_only(collection_path):
    with pytest.raises(FileNotFoundError):
        read_hats(collection_path)


def test_read_hats_branches(
    small_sky_dir,
    small_sky_order1_dir,
    association_catalog_path,
    small_sky_source_object_index_dir,
    margin_catalog_path,
    small_sky_source_dir,
    test_data_dir,
):
    read_hats(small_sky_dir)
    read_hats(small_sky_order1_dir)
    read_hats(association_catalog_path)
    read_hats(small_sky_source_object_index_dir)
    read_hats(margin_catalog_path)
    read_hats(small_sky_source_dir)
    read_hats(test_data_dir / "square_map")
    read_hats(test_data_dir / "small_sky_healpix13")


def test_read_hats_initializes_upath_once(small_sky_dir, mocker):
    mock_method = "hats.io.file_io.file_pointer.get_upath_for_protocol"
    # Setting the side effect allows us to run the mocked function's code
    mocked_upath_call = mocker.patch(mock_method, side_effect=get_upath_for_protocol)
    read_hats(small_sky_dir)
    # The construction of the UPath is called once, at the start of `read_hats`
    mocked_upath_call.assert_called_once_with(small_sky_dir)


def test_read_hats_with_s3():
    upath = get_upath_for_protocol("s3://bucket/catalog")
    assert upath.storage_options.get("anon")
    assert upath.storage_options["default_block_size"] == 32 * 1024


def test_read_hats_with_http():
    upath_http = get_upath_for_protocol("http://catalog")
    assert upath_http.fs.block_size == 32 * 1024
    upath_https = get_upath_for_protocol("https://catalog")
    assert upath_https.fs.block_size == 32 * 1024


def test_read_hats_nonstandard_npix_suffix(
    small_sky_npix_alt_suffix_dir,
    small_sky_npix_as_dir_dir,
):
    read_hats(small_sky_npix_alt_suffix_dir)
    read_hats(small_sky_npix_as_dir_dir)


def test_read_hats_original_schema(small_sky_order1_dir):
    cat = hats.read_hats(small_sky_order1_dir)
    assert cat.schema == cat.original_schema
