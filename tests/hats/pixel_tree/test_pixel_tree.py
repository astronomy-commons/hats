import numpy as np

from hats.pixel_math import HealpixPixel
from hats.pixel_math.healpix_pixel import get_higher_order_pixels
from hats.pixel_tree.pixel_tree import PixelTree


def test_pixel_tree_length():
    lengths = [1, 3, 6]
    orders = [1, 2, 7]
    for order in orders:
        for length in lengths:
            hp_pixels = [(order, i) for i in range(length)]
            tree = PixelTree.from_healpix(hp_pixels)
            assert len(tree) == length


def test_get_healpix_pixels(pixel_tree_2, pixel_list_breadth_first):
    assert pixel_tree_2.get_healpix_pixels() == pixel_list_breadth_first


def test_pixel_tree_max_depth(pixel_tree_1, pixel_tree_2, pixel_tree_3):
    assert pixel_tree_1.get_max_depth() == 0
    assert pixel_tree_2.get_max_depth() == 2
    assert pixel_tree_3.get_max_depth() == 1


def test_pixel_tree_different_tree_order(pixel_tree_2):
    pixel_tree_2_order_5 = PixelTree.from_healpix(pixel_tree_2.get_healpix_pixels(), tree_order=5)
    assert pixel_tree_2_order_5.get_max_depth() == 2
    assert pixel_tree_2_order_5.tree_order == 5
    assert pixel_tree_2_order_5.get_healpix_pixels() == pixel_tree_2.get_healpix_pixels()


def test_pixel_tree_to_moc(pixel_tree_2):
    moc = pixel_tree_2.to_moc()
    moc_order_pixels = np.concatenate(
        [
            get_higher_order_pixels(pixel.order, pixel.pixel, moc.max_order - pixel.order)
            for pixel in pixel_tree_2.get_healpix_pixels()
        ]
    )
    assert np.all(moc.flatten() == moc_order_pixels)


def test_pixel_tree_contains():
    tree = PixelTree.from_healpix([(0, 0)])
    assert tree.contains((0, 0))
    assert tree.contains(HealpixPixel(0, 0))
    assert (0, 0) in tree
    assert HealpixPixel(0, 0) in tree
    assert not tree.contains((1, 1))
    assert not tree.contains(HealpixPixel(1, 1))
    assert (1, 1) not in tree
    assert HealpixPixel(1, 1) not in tree
    assert (0, 10) not in tree
