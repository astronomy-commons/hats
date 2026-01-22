"""Tests of CatalogCollection functionality"""

import shutil

import pytest

from hats.catalog.catalog_collection import CatalogCollection
from hats.loaders import read_hats


def test_get_margin_thresholds(small_sky_collection_dir):
    """Test getting margin thresholds for all margin catalogs in the collection"""
    collection = read_hats(small_sky_collection_dir)
    assert isinstance(collection, CatalogCollection)

    # Get margin thresholds
    thresholds = collection.get_margin_thresholds()

    # Verify we have thresholds for all margins
    assert len(thresholds) == 2
    assert "small_sky_order1_margin" in thresholds
    assert "small_sky_order1_margin_10arcs" in thresholds

    # Verify the threshold values match what's in the properties files
    assert thresholds["small_sky_order1_margin"] == 7200.0
    assert thresholds["small_sky_order1_margin_10arcs"] == 10.0


def test_get_margin_thresholds_no_margins(small_sky_dir, tmp_path):
    """Test get_margin_thresholds when no margins are specified in the collection"""
    # Create a minimal collection properties file without any margins
    collection_base_dir = tmp_path / "collection_no_margins"
    collection_base_dir.mkdir()

    # Copy the main catalog
    main_catalog_src = small_sky_dir
    main_catalog_dst = collection_base_dir / "main_catalog"
    shutil.copytree(main_catalog_src, main_catalog_dst)

    # Create a collection properties file with no margins
    collection_props = collection_base_dir / "collection.properties"
    collection_props.write_text("obs_collection=test_no_margins\nhats_primary_table_url=main_catalog\n")

    collection = read_hats(collection_base_dir)
    # Should return empty dict when all_margins is None
    thresholds = collection.get_margin_thresholds()
    assert thresholds == {}


def test_get_margin_thresholds_non_margin_catalog(small_sky_collection_dir, tmp_path):
    """Test error handling when a margin entry points to a non-margin catalog"""
    # Copy the collection to a temp directory so we can modify it
    collection_base_dir = tmp_path / "collection"
    shutil.copytree(small_sky_collection_dir, collection_base_dir)
    assert collection_base_dir.exists()

    # Change one of the margin catalogs' properties to be a non-margin type
    margin_properties_file = collection_base_dir / "small_sky_order1_margin" / "hats.properties"
    properties_content = margin_properties_file.read_text()
    # Change dataproduct_type from margin to object
    modified_content = properties_content.replace("dataproduct_type=margin", "dataproduct_type=object")
    margin_properties_file.write_text(modified_content)

    # Now try to read the collection and get margin thresholds - should raise ValueError
    collection = read_hats(collection_base_dir)
    assert isinstance(collection, CatalogCollection)

    with pytest.raises(ValueError, match="is not a margin catalog"):
        collection.get_margin_thresholds()
