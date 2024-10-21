import os

import pyarrow as pa
import pytest

from hats.catalog import CatalogType, MarginCatalog, PartitionInfo, TableProperties
from hats.loaders import read_hats
from hats.pixel_math import HealpixPixel


def test_init_catalog(margin_catalog_info, margin_catalog_pixels):
    catalog = MarginCatalog(margin_catalog_info, margin_catalog_pixels)
    assert catalog.catalog_name == margin_catalog_info.catalog_name
    assert catalog.get_healpix_pixels() == margin_catalog_pixels
    assert catalog.catalog_info == margin_catalog_info

    assert len(catalog.get_healpix_pixels()) == len(margin_catalog_pixels)
    for hp_pixel in catalog.get_healpix_pixels():
        assert hp_pixel in margin_catalog_pixels
        assert hp_pixel in catalog.pixel_tree


def test_read_from_file(margin_catalog_path, margin_catalog_pixels, margin_catalog_schema):
    catalog = read_hats(margin_catalog_path)

    assert isinstance(catalog, MarginCatalog)
    assert catalog.on_disk
    assert catalog.catalog_path == margin_catalog_path
    assert len(catalog.get_healpix_pixels()) == len(margin_catalog_pixels)
    assert catalog.get_healpix_pixels() == margin_catalog_pixels

    info = catalog.catalog_info
    assert info.catalog_name == "small_sky_order1_margin"
    assert info.catalog_type == CatalogType.MARGIN
    assert info.ra_column == "ra"
    assert info.dec_column == "dec"
    assert info.primary_catalog == "small_sky_order1"
    assert info.margin_threshold == 7200

    assert isinstance(catalog.schema, pa.Schema)
    assert catalog.schema.equals(margin_catalog_schema)


# pylint: disable=duplicate-code
def test_empty_directory(tmp_path, margin_cache_catalog_info_data, margin_catalog_pixels):
    """Test loading empty or incomplete data"""
    ## Path doesn't exist
    with pytest.raises(FileNotFoundError):
        read_hats(os.path.join("path", "empty"))

    catalog_path = tmp_path / "empty"
    os.makedirs(catalog_path, exist_ok=True)

    ## Path exists but there's nothing there
    with pytest.raises(FileNotFoundError, match="properties file"):
        read_hats(catalog_path)

    ## catalog_info file exists - getting closer
    properties = TableProperties(**margin_cache_catalog_info_data)
    properties.to_properties_file(catalog_path)

    with pytest.raises(FileNotFoundError, match="metadata"):
        read_hats(catalog_path)

    ## Now we create the needed _metadata and everything is right.
    part_info = PartitionInfo.from_healpix(margin_catalog_pixels)
    part_info.write_to_metadata_files(catalog_path=catalog_path)

    with pytest.warns(UserWarning, match="slow"):
        catalog = read_hats(catalog_path)
    assert catalog.catalog_name == margin_cache_catalog_info_data["catalog_name"]


def test_margin_filter(margin_catalog_info, margin_catalog_pixels):
    catalog = MarginCatalog(margin_catalog_info, margin_catalog_pixels)
    pixels = [HealpixPixel(1, 44)]
    filtered_catalog = catalog.filter_from_pixel_list(pixels)
    assert filtered_catalog.get_healpix_pixels() == [
        HealpixPixel(1, 44),
        HealpixPixel(1, 45),
        HealpixPixel(1, 46),
        HealpixPixel(1, 47),
    ]


def test_margin_filter_invalid_size(margin_catalog_info, margin_catalog_pixels):
    margin_catalog_info.margin_threshold = 10000000
    catalog = MarginCatalog(margin_catalog_info, margin_catalog_pixels)
    pixels = [HealpixPixel(1, 44)]
    with pytest.raises(ValueError, match="greater than the size of a pixel"):
        catalog.filter_from_pixel_list(pixels)
