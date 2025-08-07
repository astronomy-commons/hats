import os

import pytest

from hats.catalog.association_catalog.association_catalog import AssociationCatalog
from hats.catalog.dataset.table_properties import TableProperties
from hats.loaders import read_hats
from hats.pixel_math import HealpixPixel


def test_init_catalog(association_catalog_info):
    catalog = AssociationCatalog(association_catalog_info, [HealpixPixel(0, 11)])
    assert catalog.catalog_name == association_catalog_info.catalog_name
    assert catalog.get_healpix_pixels() == [HealpixPixel(0, 11)]
    assert catalog.catalog_info == association_catalog_info
    assert len(catalog.get_healpix_pixels()) == len([HealpixPixel(0, 11)])
    for hp_pixel in catalog.get_healpix_pixels():
        assert hp_pixel in [HealpixPixel(0, 11)]
        assert hp_pixel in catalog.pixel_tree


def test_read_from_file(association_catalog_path):
    catalog = read_hats(association_catalog_path)

    assert isinstance(catalog, AssociationCatalog)
    assert catalog.on_disk
    assert catalog.catalog_path == association_catalog_path
    assert len(catalog.get_healpix_pixels()) == 4
    assert catalog.get_healpix_pixels() == [
        HealpixPixel(1, 44),
        HealpixPixel(1, 45),
        HealpixPixel(1, 46),
        HealpixPixel(1, 47),
    ]

    info = catalog.catalog_info
    assert info.primary_catalog == "small_sky"
    assert info.primary_column == "id"
    assert info.join_catalog == "small_sky_order1"
    assert info.join_column == "id"
    assert info.assn_max_separation == 0


def test_csv_round_trip(tmp_path, association_catalog_info_data):
    """Test loading empty or incomplete data"""
    ## Path doesn't exist
    with pytest.raises(FileNotFoundError):
        read_hats(os.path.join("path", "empty"))

    catalog_path = tmp_path / "empty"
    os.makedirs(catalog_path, exist_ok=True)

    ## Path exists but there's nothing there
    with pytest.raises(FileNotFoundError):
        read_hats(catalog_path)

    ## catalog_info file exists - getting closer
    properties = TableProperties(**association_catalog_info_data)
    properties.to_properties_file(catalog_path)

    with pytest.raises(FileNotFoundError):
        read_hats(catalog_path)

    file_name = catalog_path / "partition_info.csv"
    with open(file_name, "w", encoding="utf-8") as metadata_file:
        # dump some garbage in there - just needs to exist.
        metadata_file.write("Norder,Npix")

    catalog = read_hats(catalog_path)
    assert catalog.catalog_name == association_catalog_info_data["catalog_name"]
