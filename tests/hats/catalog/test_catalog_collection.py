"""Tests of CatalogCollection functionality"""

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
