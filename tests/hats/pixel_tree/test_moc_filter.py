import numpy as np
from mocpy import MOC

from hats.pixel_math import HealpixPixel
from hats.pixel_tree.moc_filter import filter_by_moc


def test_moc_filter(pixel_tree_2):
    orders = np.array([1, 1, 2])
    pixels = np.array([45, 46, 128])
    moc = MOC.from_healpix_cells(pixels, orders, 2)
    filtered_tree = filter_by_moc(pixel_tree_2, moc)
    assert filtered_tree.get_healpix_pixels() == [
        HealpixPixel(2, 128),
        HealpixPixel(1, 45),
        HealpixPixel(1, 46),
    ]


def test_moc_filter_lower_order(pixel_tree_2):
    orders = np.array([0, 1])
    pixels = np.array([11, 32])
    moc = MOC.from_healpix_cells(pixels, orders, 1)
    filtered_tree = filter_by_moc(pixel_tree_2, moc)
    assert filtered_tree.get_healpix_pixels() == [
        HealpixPixel(2, 128),
        HealpixPixel(2, 130),
        HealpixPixel(2, 131),
        HealpixPixel(1, 44),
        HealpixPixel(1, 45),
        HealpixPixel(1, 46),
    ]


def test_moc_filter_higher_order(pixel_tree_2):
    orders = np.array([1, 3])
    pixels = np.array([40, 520])
    moc = MOC.from_healpix_cells(pixels, orders, 3)
    filtered_tree = filter_by_moc(pixel_tree_2, moc)
    assert filtered_tree.get_healpix_pixels() == [
        HealpixPixel(2, 130),
        HealpixPixel(0, 10),
    ]


def test_moc_filter_empty_moc(pixel_tree_2):
    orders = np.array([])
    pixels = np.array([])
    moc = MOC.from_healpix_cells(pixels, orders, 0)
    filtered_tree = filter_by_moc(pixel_tree_2, moc)
    assert filtered_tree.get_healpix_pixels() == []


def test_moc_filter_empty_result(pixel_tree_2):
    orders = np.array([0])
    pixels = np.array([1])
    moc = MOC.from_healpix_cells(pixels, orders, 0)
    filtered_tree = filter_by_moc(pixel_tree_2, moc)
    assert filtered_tree.get_healpix_pixels() == []
