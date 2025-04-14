import shutil

import pytest

from hats.catalog import Catalog, MarginCatalog
from hats.catalog.catalog_collection import CatalogCollection
from hats.catalog.dataset.collection_properties import CollectionProperties
from hats.catalog.index.index_catalog import IndexCatalog
from hats.io.file_io import get_upath_for_protocol
from hats.loaders import read_hats


def test_read_hats_collection(
    small_sky_collection_dir, small_sky_order1_dir, margin_catalog_path, small_sky_order1_id_index_dir
):
    collection = read_hats(small_sky_collection_dir)
    assert isinstance(collection, CatalogCollection)

    assert isinstance(collection.main_catalog, Catalog)
    main_catalog = read_hats(small_sky_order1_dir)
    assert collection.main_catalog_dir == small_sky_collection_dir / main_catalog.catalog_name
    assert collection.main_catalog.catalog_info == main_catalog.catalog_info
    assert collection.get_healpix_pixels() == main_catalog.get_healpix_pixels()
    assert collection.main_catalog.schema == main_catalog.schema

    assert isinstance(collection.margin_catalog, MarginCatalog)
    margin_catalog = read_hats(margin_catalog_path)
    assert collection.margin_catalog_dir == small_sky_collection_dir / margin_catalog.catalog_name
    assert collection.margin_catalog.catalog_info == margin_catalog.catalog_info
    assert collection.margin_catalog.get_healpix_pixels() == margin_catalog.get_healpix_pixels()
    assert collection.margin_catalog.schema == margin_catalog.schema

    assert isinstance(collection.index_catalog, IndexCatalog)
    index_catalog = read_hats(small_sky_order1_id_index_dir)
    assert collection.default_index_field == "id"
    assert collection.default_index_catalog_dir == small_sky_collection_dir / index_catalog.catalog_name
    assert collection.index_catalog.catalog_info == index_catalog.catalog_info
    assert collection.index_catalog.schema == index_catalog.schema


@pytest.mark.parametrize("incorrect_catalog", ["main_catalog", "margin_catalog", "index_catalog"])
def test_read_hats_collection_catalogs_not_of_correct_type(
    small_sky_collection_dir, incorrect_catalog, tmp_path
):
    """Test that the catalogs need to be of the correct type"""
    collection_base_dir = tmp_path / "collection"
    shutil.copytree(small_sky_collection_dir, collection_base_dir)
    assert collection_base_dir.exists()

    # Modify the collection properties to invalidate a single catalog
    collection_properties = CollectionProperties.read_from_dir(collection_base_dir)
    if incorrect_catalog == "main_catalog":
        collection_properties.hats_primary_table_url = "small_sky_order1_margin"
    elif incorrect_catalog == "margin_catalog":
        collection_properties.default_margin = "small_sky_order1_id_index"
    else:
        collection_properties.all_indexes["id"] = "small_sky_order1_margin"
    collection_properties.to_properties_file(collection_base_dir)

    with pytest.raises(FileNotFoundError):
        read_hats(collection_base_dir)


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


def test_read_hats_initializes_upath_once(small_sky_dir, mocker):
    mock_method = "hats.io.file_io.file_pointer.get_upath_for_protocol"
    # Setting the side effect allows us to run the mocked function's code
    mocked_upath_call = mocker.patch(mock_method, side_effect=get_upath_for_protocol)
    read_hats(small_sky_dir)
    # The construction of the UPath is called once, at the start of `read_hats`
    mocked_upath_call.assert_called_once_with(small_sky_dir)


def test_read_hats_with_s3_anonymous_access():
    upath = get_upath_for_protocol("s3://bucket/catalog")
    assert upath.storage_options.get("anon")


def test_read_hats_nonstandard_npix_suffix(
    small_sky_npix_alt_suffix_dir,
    small_sky_npix_as_dir_dir,
):
    read_hats(small_sky_npix_alt_suffix_dir)
    read_hats(small_sky_npix_as_dir_dir)
