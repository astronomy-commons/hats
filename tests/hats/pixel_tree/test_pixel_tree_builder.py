import pytest

from hats.pixel_math import HealpixPixel
from hats.pixel_tree.pixel_tree import PixelTree


def assert_pixel_tree_has_nodes_in_catalog(tree, catalog):
    """assert tree contains the same nodes as the catalog"""
    for pixel in catalog.get_healpix_pixels():
        assert tree.contains((pixel.order, pixel.pixel))


def test_pixel_tree_small_sky(small_sky_catalog):
    """test pixel tree on small sky"""
    pixel_tree = PixelTree.from_healpix(small_sky_catalog.get_healpix_pixels())
    assert len(pixel_tree) == len(small_sky_catalog.get_healpix_pixels())
    assert_pixel_tree_has_nodes_in_catalog(pixel_tree, small_sky_catalog)


def test_pixel_tree_small_sky_order1(small_sky_order1_catalog):
    """test pixel tree on small sky order1"""
    pixel_tree = PixelTree.from_healpix(small_sky_order1_catalog.get_healpix_pixels())
    assert_pixel_tree_has_nodes_in_catalog(pixel_tree, small_sky_order1_catalog)


def test_pixel_tree_small_sky_from_list(small_sky_catalog, small_sky_pixels):
    """test pixel tree on small sky"""
    pixel_tree = PixelTree.from_healpix(small_sky_pixels)
    assert len(pixel_tree) == len(small_sky_pixels)
    assert_pixel_tree_has_nodes_in_catalog(pixel_tree, small_sky_catalog)


def test_pixel_tree_small_sky_order1_from_list(small_sky_order1_catalog, small_sky_order1_pixels):
    """test pixel tree on small sky order1"""
    pixel_tree = PixelTree.from_healpix(small_sky_order1_pixels)
    assert_pixel_tree_has_nodes_in_catalog(pixel_tree, small_sky_order1_catalog)


def test_duplicate_pixel_raises_error():
    """test pixel tree raises error with duplicate pixels"""
    partition_info = [
        HealpixPixel(0, 11),
    ]
    PixelTree.from_healpix(partition_info)
    info_with_duplicate = [
        HealpixPixel(0, 11),
        HealpixPixel(0, 11),
    ]
    with pytest.raises(ValueError):
        PixelTree.from_healpix(info_with_duplicate)


def test_pixel_duplicated_at_different_order_raises_error():
    """test pixel tree raises error with duplicate pixels at different orders"""
    info_with_duplicate = [
        HealpixPixel(0, 11),
        HealpixPixel(1, 44),  # overlaps with (0,11)
    ]
    with pytest.raises(ValueError):
        PixelTree.from_healpix(info_with_duplicate)
