"""Tests of catalog functionality"""

import os

import astropy.units as u
import numpy as np
import pyarrow as pa
import pytest
from astropy.coordinates import SkyCoord
from mocpy import MOC

import hats.pixel_math.healpix_shim as hp
from hats.catalog import Catalog, PartitionInfo, TableProperties
from hats.catalog.healpix_dataset.healpix_dataset import HealpixDataset
from hats.io import paths
from hats.io.file_io import read_fits_image
from hats.loaders import read_hats
from hats.pixel_math import HealpixPixel
from hats.pixel_math.validators import ValidatorsErrors
from hats.pixel_tree.pixel_tree import PixelTree


def test_catalog_load(catalog_info, catalog_pixels):
    catalog = Catalog(catalog_info, catalog_pixels)
    assert catalog.get_healpix_pixels() == catalog_pixels
    assert catalog.catalog_name == catalog_info.catalog_name
    assert catalog_info.total_rows == len(catalog)

    for hp_pixel in catalog_pixels:
        assert hp_pixel in catalog.pixel_tree


def test_partition_info_pixel_input_types(catalog_info, catalog_pixels):
    partition_info = PartitionInfo.from_healpix(catalog_pixels)
    catalog = Catalog(catalog_info, partition_info)
    assert len(catalog.get_healpix_pixels()) == len(catalog_pixels)
    assert len(catalog.pixel_tree.get_healpix_pixels()) == len(catalog_pixels)
    for hp_pixel in catalog_pixels:
        assert hp_pixel in catalog.pixel_tree


def test_tree_pixel_input(catalog_info, catalog_pixels):
    tree = PixelTree.from_healpix(catalog_pixels)
    catalog = Catalog(catalog_info, tree)
    assert len(catalog.get_healpix_pixels()) == len(catalog_pixels)
    assert len(catalog.pixel_tree.get_healpix_pixels()) == len(catalog_pixels)
    for pixel in catalog_pixels:
        assert pixel in catalog.pixel_tree


def test_tree_pixel_input_list(catalog_info, catalog_pixels):
    catalog = Catalog(catalog_info, catalog_pixels)
    assert len(catalog.get_healpix_pixels()) == len(catalog_pixels)
    assert len(catalog.pixel_tree.get_healpix_pixels()) == len(catalog_pixels)
    for pixel in catalog_pixels:
        assert pixel in catalog.pixel_tree


def test_wrong_pixel_input_type(catalog_info):
    with pytest.raises(TypeError):
        Catalog(catalog_info, "test")
    with pytest.raises(TypeError):
        Catalog._get_pixel_tree_from_pixels("test")
    with pytest.raises(TypeError):
        Catalog._get_partition_info_from_pixels("test")


def test_get_pixels_list(catalog_info, catalog_pixels):
    catalog = Catalog(catalog_info, catalog_pixels)
    pixels = catalog.get_healpix_pixels()
    assert pixels == catalog_pixels


def test_load_catalog_small_sky(small_sky_dir, small_sky_schema):
    """Instantiate a catalog with 1 pixel"""
    cat = read_hats(small_sky_dir)

    assert isinstance(cat, Catalog)
    assert cat.catalog_name == "small_sky"
    assert len(cat.get_healpix_pixels()) == 1

    assert isinstance(cat.schema, pa.Schema)
    assert cat.schema.equals(small_sky_schema)


def test_load_catalog_small_sky_order1(small_sky_order1_dir):
    """Instantiate a catalog with 4 pixels"""
    cat = read_hats(small_sky_order1_dir)

    assert isinstance(cat, Catalog)
    assert cat.catalog_name == "small_sky_order1"
    assert len(cat.get_healpix_pixels()) == 4


def test_aggregate_column_statistics(small_sky_order1_dir):
    cat = read_hats(small_sky_order1_dir)

    result_frame = cat.aggregate_column_statistics()
    assert len(result_frame) == 5

    result_frame = cat.aggregate_column_statistics(exclude_hats_columns=False)
    assert len(result_frame) == 9

    result_frame = cat.aggregate_column_statistics(include_columns=["ra", "dec"])
    assert len(result_frame) == 2


def test_load_catalog_small_sky_order1_moc(small_sky_order1_dir):
    """Instantiate a catalog with 4 pixels"""
    cat = read_hats(small_sky_order1_dir)

    assert isinstance(cat, Catalog)
    assert cat.moc is not None
    counts_skymap = read_fits_image(paths.get_point_map_file_pointer(small_sky_order1_dir))
    skymap_order = hp.npix2order(len(counts_skymap))
    assert cat.moc.max_order == skymap_order
    assert np.all(cat.moc.flatten() == np.where(counts_skymap > 0))


def test_load_catalog_small_sky_source(small_sky_source_dir, small_sky_source_schema):
    """Instantiate a source catalog with 14 pixels"""
    cat = read_hats(small_sky_source_dir)

    assert isinstance(cat, Catalog)
    assert cat.catalog_name == "small_sky_source"
    assert len(cat.get_healpix_pixels()) == 14

    assert isinstance(cat.schema, pa.Schema)
    assert cat.schema.equals(small_sky_source_schema)


def test_max_coverage_order(small_sky_order1_catalog):
    assert small_sky_order1_catalog.get_max_coverage_order() >= small_sky_order1_catalog.moc.max_order
    assert (
        small_sky_order1_catalog.get_max_coverage_order()
        >= small_sky_order1_catalog.pixel_tree.get_max_depth()
    )
    high_moc_order = 8
    test_moc = MOC.from_depth29_ranges(
        max_depth=high_moc_order, ranges=small_sky_order1_catalog.moc.to_depth29_ranges
    )
    small_sky_order1_catalog.moc = test_moc
    assert small_sky_order1_catalog.get_max_coverage_order() == high_moc_order
    small_sky_order1_catalog.moc = None
    assert (
        small_sky_order1_catalog.get_max_coverage_order()
        == small_sky_order1_catalog.pixel_tree.get_max_depth()
    )


def test_max_coverage_order_empty_catalog(catalog_info):
    empty_catalog = HealpixDataset(catalog_info, PixelTree.from_healpix([]))
    with pytest.raises(ValueError, match="empty catalog"):
        empty_catalog.get_max_coverage_order()


def test_cone_filter(small_sky_order1_catalog):
    ra = 315
    dec = -66.443
    radius = 0.1

    filtered_catalog = small_sky_order1_catalog.filter_by_cone(ra, dec, radius)
    filtered_pixels = filtered_catalog.get_healpix_pixels()

    assert len(filtered_pixels) == 1
    assert filtered_pixels == [HealpixPixel(1, 44)]

    assert (1, 44) in filtered_catalog.pixel_tree
    assert filtered_catalog.catalog_info.total_rows == 0

    assert filtered_catalog.moc is not None
    cone_moc = MOC.from_cone(
        lon=ra * u.deg,
        lat=dec * u.deg,
        radius=radius * u.arcsec,
        max_depth=small_sky_order1_catalog.get_max_coverage_order(),
    )
    assert filtered_catalog.moc == cone_moc.intersection(small_sky_order1_catalog.moc)


def test_cone_filter_big(small_sky_order1_catalog):
    filtered_catalog = small_sky_order1_catalog.filter_by_cone(315, -66.443, 30 * 3600)
    assert len(filtered_catalog.get_healpix_pixels()) == 4
    assert (1, 44) in filtered_catalog.pixel_tree
    assert (1, 45) in filtered_catalog.pixel_tree
    assert (1, 46) in filtered_catalog.pixel_tree
    assert (1, 47) in filtered_catalog.pixel_tree


def test_cone_filter_multiple_order(catalog_info):
    catalog_pixel_list = [
        HealpixPixel(6, 30),
        HealpixPixel(7, 124),
        HealpixPixel(7, 5000),
    ]
    catalog = Catalog(catalog_info, catalog_pixel_list)
    filtered_catalog = catalog.filter_by_cone(47.1, 6, 30 * 3600)
    assert filtered_catalog.get_healpix_pixels() == [HealpixPixel(6, 30), HealpixPixel(7, 124)]


def test_cone_filter_empty(small_sky_order1_catalog):
    filtered_catalog = small_sky_order1_catalog.filter_by_cone(0, 0, 0.1)
    assert len(filtered_catalog.get_healpix_pixels()) == 0
    assert len(filtered_catalog.pixel_tree) == 0


def test_cone_filter_invalid_cone_center(small_sky_order1_catalog):
    with pytest.raises(ValueError, match=ValidatorsErrors.INVALID_DEC):
        small_sky_order1_catalog.filter_by_cone(0, -100, 0.1)
    with pytest.raises(ValueError, match=ValidatorsErrors.INVALID_DEC):
        small_sky_order1_catalog.filter_by_cone(0, 100, 0.1)
    with pytest.raises(ValueError, match=ValidatorsErrors.INVALID_RADIUS):
        small_sky_order1_catalog.filter_by_cone(0, 10, -1)


def test_polygonal_filter(small_sky_order1_catalog):
    polygon_vertices = [(282, -58), (282, -55), (272, -55), (272, -58)]
    filtered_catalog = small_sky_order1_catalog.filter_by_polygon(polygon_vertices)
    filtered_pixels = filtered_catalog.get_healpix_pixels()
    assert len(filtered_pixels) == 1
    assert filtered_pixels == [HealpixPixel(1, 46)]
    assert (1, 46) in filtered_catalog.pixel_tree
    assert filtered_catalog.catalog_info.total_rows == 0
    assert filtered_catalog.moc is not None
    ra, dec = np.array(polygon_vertices).T
    polygon_moc = MOC.from_polygon(
        lon=ra * u.deg,
        lat=dec * u.deg,
        max_depth=small_sky_order1_catalog.get_max_coverage_order(),
    )
    assert filtered_catalog.moc == polygon_moc.intersection(small_sky_order1_catalog.moc)


def test_polygonal_filter_invalid_coordinate_shape(small_sky_order1_catalog):
    with pytest.raises(ValueError, match="coordinates shape"):
        vertices = [(282, -58, 1), (282, -55, 2), (272, -55, 3)]
        small_sky_order1_catalog.filter_by_polygon(vertices)


def test_polygonal_filter_big(small_sky_order1_catalog):
    polygon_vertices = [(281, -69), (281, -25), (350, -25), (350, -69)]
    filtered_catalog = small_sky_order1_catalog.filter_by_polygon(polygon_vertices)
    assert len(filtered_catalog.get_healpix_pixels()) == 4
    assert (1, 44) in filtered_catalog.pixel_tree
    assert (1, 45) in filtered_catalog.pixel_tree
    assert (1, 46) in filtered_catalog.pixel_tree
    assert (1, 47) in filtered_catalog.pixel_tree


def test_polygonal_filter_multiple_order(catalog_info):
    catalog_pixel_list = [
        HealpixPixel(6, 30),
        HealpixPixel(7, 124),
        HealpixPixel(7, 5000),
    ]
    catalog = Catalog(catalog_info, catalog_pixel_list)
    polygon_vertices = [(47.1, 6), (64.5, 6), (64.5, 6.27), (47.1, 6.27)]
    filtered_catalog = catalog.filter_by_polygon(polygon_vertices)
    assert filtered_catalog.get_healpix_pixels() == [HealpixPixel(6, 30), HealpixPixel(7, 124)]


def test_polygonal_filter_empty(small_sky_order1_catalog):
    polygon_vertices = [(0, 0), (1, 1), (0, 2)]
    filtered_catalog = small_sky_order1_catalog.filter_by_polygon(polygon_vertices)
    assert len(filtered_catalog.get_healpix_pixels()) == 0
    assert len(filtered_catalog.pixel_tree) == 0


def test_polygonal_filter_invalid_coordinates(small_sky_order1_catalog):
    # Declination is over 90 degrees
    polygon_vertices = [(47.1, -100), (64.5, -100), (64.5, 6.27), (47.1, 6.27)]
    with pytest.raises(ValueError, match=ValidatorsErrors.INVALID_DEC):
        small_sky_order1_catalog.filter_by_polygon(polygon_vertices)
    # Right ascension should wrap, it does not throw an error
    polygon_vertices = [(470.1, 6), (470.5, 6), (64.5, 10.27), (47.1, 10.27)]
    small_sky_order1_catalog.filter_by_polygon(polygon_vertices)


def test_polygonal_filter_invalid_polygon(small_sky_order1_catalog):
    # The polygon must have a minimum of 3 vertices
    with pytest.raises(ValueError, match=ValidatorsErrors.INVALID_NUM_VERTICES):
        vertices = [(100.1, -20.3), (100.1, 40.3)]
        small_sky_order1_catalog.filter_by_polygon(vertices[:2])
    # The vertices should not have duplicates
    with pytest.raises(ValueError, match=ValidatorsErrors.DUPLICATE_VERTICES):
        vertices = [(100.1, -20.3), (100.1, -20.3), (280.1, -20.3), (280.1, 40.3)]
        small_sky_order1_catalog.filter_by_polygon(vertices)
    # The polygons should not be on a great circle
    with pytest.raises(ValueError, match=ValidatorsErrors.DEGENERATE_POLYGON):
        vertices = [(100.1, 40.3), (100.1, -20.3), (280.1, -20.3), (280.1, 40.3)]
        small_sky_order1_catalog.filter_by_polygon(vertices)
    with pytest.raises(ValueError, match=ValidatorsErrors.DEGENERATE_POLYGON):
        vertices = [(50.1, 0), (100.1, 0), (150.1, 0), (200.1, 0)]
        small_sky_order1_catalog.filter_by_polygon(vertices)
    with pytest.raises(ValueError, match=ValidatorsErrors.INVALID_CONCAVE_SHAPE):
        vertices = [(45, 30), (60, 60), (90, 45), (60, 50)]
        small_sky_order1_catalog.filter_by_polygon(vertices)


def test_box_filter(small_sky_order1_catalog):
    # The catalog pixels are distributed around the [-90,0] degree range.
    filtered_catalog = small_sky_order1_catalog.filter_by_box(ra=(280, 300), dec=(-30, -20))
    filtered_pixels = filtered_catalog.get_healpix_pixels()

    assert len(filtered_pixels) == 2
    assert filtered_pixels == [HealpixPixel(1, 46), HealpixPixel(1, 47)]

    assert (1, 46) in filtered_catalog.pixel_tree
    assert (1, 47) in filtered_catalog.pixel_tree
    assert len(filtered_catalog.pixel_tree.pixels[1]) == 2
    assert filtered_catalog.catalog_info.total_rows == 0

    # Check that the previous filter is the same as intersecting the ra and dec filters
    assert filtered_catalog.moc is not None
    box_moc = MOC.from_zone(
        # SkyCoord([bottom_left_corner, upper_right_corner])
        SkyCoord([[280, -30], [300, -20]], unit="deg"),
        max_depth=small_sky_order1_catalog.get_max_coverage_order(),
    )
    assert filtered_catalog.moc == box_moc.intersection(small_sky_order1_catalog.moc)


def test_box_filter_wrapped_ra(small_sky_order1_catalog):
    # The catalog pixels are distributed around the [270,0] degree range.
    filtered_catalog = small_sky_order1_catalog.filter_by_box(ra=(-10, 10), dec=(-90, 90))
    filtered_pixels = filtered_catalog.get_healpix_pixels()

    assert len(filtered_pixels) == 2
    assert filtered_pixels == [HealpixPixel(1, 44), HealpixPixel(1, 45)]

    assert (1, 44) in filtered_catalog.pixel_tree
    assert (1, 45) in filtered_catalog.pixel_tree
    assert len(filtered_catalog.pixel_tree.pixels[1]) == 2
    assert filtered_catalog.catalog_info.total_rows == 0


def test_box_filter_ra_boundary(small_sky_order1_catalog):
    dec = (-30, 0)
    filtered_catalog = small_sky_order1_catalog.filter_by_box(ra=(0, 0), dec=dec)
    filtered_pixels = filtered_catalog.get_healpix_pixels()

    assert len(filtered_pixels) == 3
    assert filtered_pixels == [HealpixPixel(1, 45), HealpixPixel(1, 46), HealpixPixel(1, 47)]

    for ra_range in [(0, 360), (360, 0)]:
        catalog = small_sky_order1_catalog.filter_by_box(ra=ra_range, dec=dec)
        assert catalog.get_healpix_pixels() == filtered_catalog.get_healpix_pixels()


def test_box_filter_ra_divisions_edge_cases(small_sky_order1_catalog):
    # In this test we generate RA bands and their complements and compare the amount of
    # pixels from the catalog after filtering. We construct these complement regions in
    # a way that allows us to capture more pixels of the catalog. This is useful to test
    # that wide RA ranges (larger than 180 degrees) are correctly handled.
    dec = (-90, 90)

    def assert_is_subset_of(catalog, catalog_complement):
        pixels_catalog = catalog.get_healpix_pixels()
        pixels_catalog_complement = catalog_complement.get_healpix_pixels()
        assert len(pixels_catalog) < len(pixels_catalog_complement)
        assert all(pixel in pixels_catalog_complement for pixel in pixels_catalog)

    filtered_catalog = small_sky_order1_catalog.filter_by_box(ra=(0, 180), dec=dec)
    filtered_catalog_complement = small_sky_order1_catalog.filter_by_box(ra=(180, 0), dec=dec)
    assert_is_subset_of(filtered_catalog, filtered_catalog_complement)

    filtered_catalog = small_sky_order1_catalog.filter_by_box(ra=(10, 50), dec=dec)
    filtered_catalog_complement = small_sky_order1_catalog.filter_by_box(ra=(50, 10), dec=dec)
    assert_is_subset_of(filtered_catalog, filtered_catalog_complement)

    filtered_catalog = small_sky_order1_catalog.filter_by_box(ra=(10, 220), dec=dec)
    filtered_catalog_complement = small_sky_order1_catalog.filter_by_box(ra=(220, 10), dec=dec)
    assert_is_subset_of(filtered_catalog, filtered_catalog_complement)

    filtered_catalog = small_sky_order1_catalog.filter_by_box(ra=(350, 200), dec=dec)
    filtered_catalog_complement = small_sky_order1_catalog.filter_by_box(ra=(200, 350), dec=dec)
    assert_is_subset_of(filtered_catalog, filtered_catalog_complement)

    filtered_catalog = small_sky_order1_catalog.filter_by_box(ra=(50, 200), dec=dec)
    filtered_catalog_complement = small_sky_order1_catalog.filter_by_box(ra=(200, 50), dec=dec)
    assert_is_subset_of(filtered_catalog, filtered_catalog_complement)


def test_box_filter_empty(small_sky_order1_catalog):
    filtered_catalog = small_sky_order1_catalog.filter_by_box(ra=(40, 50), dec=(10, 20))
    assert len(filtered_catalog.get_healpix_pixels()) == 0
    assert len(filtered_catalog.pixel_tree) == 0


def test_box_filter_invalid_args(small_sky_order1_catalog):
    # Some declination values are out of the [-90,90[ bounds
    with pytest.raises(ValueError, match=ValidatorsErrors.INVALID_DEC):
        small_sky_order1_catalog.filter_by_box(ra=(0, 30), dec=(-100, -70))

    # Declination values should be in ascending order
    with pytest.raises(ValueError, match=ValidatorsErrors.INVALID_RADEC_RANGE):
        small_sky_order1_catalog.filter_by_box(ra=(0, 30), dec=(0, -10))

    # There are ranges are defined with more than 2 values
    with pytest.raises(ValueError, match=ValidatorsErrors.INVALID_RADEC_RANGE):
        small_sky_order1_catalog.filter_by_box(ra=(0, 30), dec=(-40, -30, 10))
    with pytest.raises(ValueError, match=ValidatorsErrors.INVALID_RADEC_RANGE):
        small_sky_order1_catalog.filter_by_box(ra=(0, 30, 40), dec=(-40, 10))

    # The declination values coincide
    with pytest.raises(ValueError, match=ValidatorsErrors.INVALID_RADEC_RANGE):
        small_sky_order1_catalog.filter_by_box(ra=(0, 50), dec=(50, 50))

    # No range values were provided
    with pytest.raises(ValueError, match=ValidatorsErrors.INVALID_RADEC_RANGE):
        small_sky_order1_catalog.filter_by_box(ra=None, dec=None)


def test_empty_directory(tmp_path, catalog_info_data):
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
    properties = TableProperties(**catalog_info_data)
    properties.to_properties_file(catalog_path)

    with pytest.raises(FileNotFoundError):
        read_hats(catalog_path)

    ## Now we create the needed _metadata and everything is right.
    part_info = PartitionInfo.from_healpix([HealpixPixel(0, 11)])
    part_info.write_to_metadata_files(catalog_path=catalog_path)

    with pytest.warns(UserWarning, match="slow"):
        catalog = read_hats(catalog_path)
    assert catalog.catalog_name == "test_name"


@pytest.mark.timeout(20)
def test_generate_negative_tree_pixels(small_sky_order1_catalog):
    """Test generate_negative_tree_pixels on a basic catalog."""
    expected_pixels = [
        HealpixPixel(0, 0),
        HealpixPixel(0, 1),
        HealpixPixel(0, 2),
        HealpixPixel(0, 3),
        HealpixPixel(0, 4),
        HealpixPixel(0, 5),
        HealpixPixel(0, 6),
        HealpixPixel(0, 7),
        HealpixPixel(0, 8),
        HealpixPixel(0, 9),
        HealpixPixel(0, 10),
    ]

    negative_tree = small_sky_order1_catalog.generate_negative_tree_pixels()

    assert negative_tree == expected_pixels


@pytest.mark.timeout(20)
def test_generate_negative_tree_pixels_order_0(small_sky_catalog):
    """Test generate_negative_tree_pixels on a catalog with only order 0 pixels."""
    expected_pixels = [
        HealpixPixel(0, 0),
        HealpixPixel(0, 1),
        HealpixPixel(0, 2),
        HealpixPixel(0, 3),
        HealpixPixel(0, 4),
        HealpixPixel(0, 5),
        HealpixPixel(0, 6),
        HealpixPixel(0, 7),
        HealpixPixel(0, 8),
        HealpixPixel(0, 9),
        HealpixPixel(0, 10),
    ]

    negative_tree = small_sky_catalog.generate_negative_tree_pixels()

    assert negative_tree == expected_pixels


def test_generate_negative_tree_pixels_multi_order(small_sky_order1_catalog):
    """Test generate_negative_tree_pixels on a catalog with
    missing pixels on multiple order.
    """
    # remove one of the order 1 pixels from the catalog.
    nodes = small_sky_order1_catalog.pixel_tree.get_healpix_pixels()
    small_sky_order1_catalog.pixel_tree = PixelTree.from_healpix(nodes[1:])

    expected_pixels = [
        HealpixPixel(0, 0),
        HealpixPixel(0, 1),
        HealpixPixel(0, 2),
        HealpixPixel(0, 3),
        HealpixPixel(0, 4),
        HealpixPixel(0, 5),
        HealpixPixel(0, 6),
        HealpixPixel(0, 7),
        HealpixPixel(0, 8),
        HealpixPixel(0, 9),
        HealpixPixel(0, 10),
        HealpixPixel(1, 44),
    ]

    negative_tree = small_sky_order1_catalog.generate_negative_tree_pixels()

    assert negative_tree == expected_pixels


def test_catalog_len_is_undetermined(small_sky_order1_catalog):
    """Tests that catalogs modified by queries and spatial filters have an undetermined
    number of rows, case in which an error is thrown"""
    with pytest.raises(ValueError, match="undetermined"):
        len(small_sky_order1_catalog.filter_by_cone(0, -80, 1))
    with pytest.raises(ValueError, match="undetermined"):
        vertices = [(300, -50), (300, -55), (272, -55), (272, -50)]
        len(small_sky_order1_catalog.filter_by_polygon(vertices))
    with pytest.raises(ValueError, match="undetermined"):
        len(small_sky_order1_catalog.filter_by_box(ra=(280, 300), dec=(0, 30)))
    with pytest.raises(ValueError, match="undetermined"):
        len(small_sky_order1_catalog.filter_from_pixel_list([HealpixPixel(0, 11)]))
