import numpy as np
import pytest

from hats.catalog import Catalog
from hats.pixel_math import HealpixPixel
from hats.pixel_tree.pixel_alignment import PixelAlignment, align_trees, align_with_mocs
from hats.pixel_tree.pixel_tree import PixelTree


def assert_trees_equal(tree1: PixelTree, tree2: PixelTree):
    np.testing.assert_array_equal(tree1.tree, tree2.tree)
    assert tree1.tree_order == tree2.tree_order


def assert_mapping_matches_tree(alignment: PixelAlignment):
    for (_, row), pixel in zip(alignment.pixel_mapping.iterrows(), alignment.pixel_tree.get_healpix_pixels()):
        left_pixel = (
            HealpixPixel(
                row[PixelAlignment.PRIMARY_ORDER_COLUMN_NAME], row[PixelAlignment.PRIMARY_PIXEL_COLUMN_NAME]
            )
            if row[PixelAlignment.PRIMARY_ORDER_COLUMN_NAME] is not None
            else None
        )
        right_pixel = (
            HealpixPixel(
                row[PixelAlignment.JOIN_ORDER_COLUMN_NAME], row[PixelAlignment.JOIN_PIXEL_COLUMN_NAME]
            )
            if row[PixelAlignment.JOIN_ORDER_COLUMN_NAME] is not None
            else None
        )
        aligned_pixel = (
            HealpixPixel(
                row[PixelAlignment.ALIGNED_ORDER_COLUMN_NAME], row[PixelAlignment.ALIGNED_PIXEL_COLUMN_NAME]
            )
            if row[PixelAlignment.ALIGNED_ORDER_COLUMN_NAME] is not None
            else None
        )
        assert pixel == aligned_pixel
        if left_pixel is not None:
            assert aligned_pixel.convert_to_lower_order(aligned_pixel.order - left_pixel.order) == left_pixel
        if right_pixel is not None:
            assert (
                aligned_pixel.convert_to_lower_order(aligned_pixel.order - right_pixel.order) == right_pixel
            )


def test_pixel_tree_alignment_same_tree(pixel_tree_1):
    alignment = align_trees(pixel_tree_1, pixel_tree_1, "inner")
    assert_trees_equal(pixel_tree_1, alignment.pixel_tree)
    assert_mapping_matches_tree(alignment)
    assert alignment.moc is None


def test_pixel_tree_alignment_inner(pixel_tree_2, pixel_tree_3, aligned_trees_2_3_inner):
    alignment = align_trees(pixel_tree_2, pixel_tree_3, "inner")
    assert_trees_equal(alignment.pixel_tree, aligned_trees_2_3_inner)
    assert_mapping_matches_tree(alignment)


def test_pixel_tree_alignment_left(pixel_tree_2, pixel_tree_3, aligned_trees_2_3_left):
    alignment = align_trees(pixel_tree_2, pixel_tree_3, "left")
    assert_trees_equal(alignment.pixel_tree, aligned_trees_2_3_left)
    assert_mapping_matches_tree(alignment)


def test_pixel_tree_alignment_right(pixel_tree_2, pixel_tree_3, aligned_trees_2_3_right):
    alignment = align_trees(pixel_tree_2, pixel_tree_3, "right")
    assert_trees_equal(alignment.pixel_tree, aligned_trees_2_3_right)
    assert_mapping_matches_tree(alignment)


def test_pixel_tree_alignment_outer(pixel_tree_2, pixel_tree_3, aligned_trees_2_3_outer):
    alignment = align_trees(pixel_tree_2, pixel_tree_3, "outer")
    assert_trees_equal(alignment.pixel_tree, aligned_trees_2_3_outer)
    assert_mapping_matches_tree(alignment)


def test_pixel_tree_alignment_with_moc_inner(pixel_tree_2, pixel_tree_3, aligned_trees_2_3_inner):
    moc_pixels = aligned_trees_2_3_inner.get_healpix_pixels()[:-3]
    correct_aligned_tree = PixelTree.from_healpix(moc_pixels)
    moc = correct_aligned_tree.to_moc()
    alignment = align_with_mocs(pixel_tree_2, pixel_tree_3, moc, pixel_tree_3.to_moc())
    assert_trees_equal(alignment.pixel_tree, correct_aligned_tree)
    assert_mapping_matches_tree(alignment)
    assert moc == alignment.moc
    alignment = align_with_mocs(pixel_tree_2, pixel_tree_3, pixel_tree_2.to_moc(), moc)
    assert_trees_equal(alignment.pixel_tree, correct_aligned_tree)
    assert_mapping_matches_tree(alignment)
    assert alignment.moc is not None
    assert moc == alignment.moc


def test_pixel_tree_alignment_with_moc_same(pixel_tree_2, pixel_tree_3, aligned_trees_2_3_inner):
    alignment = align_with_mocs(pixel_tree_2, pixel_tree_3, pixel_tree_2.to_moc(), pixel_tree_3.to_moc())
    assert_trees_equal(alignment.pixel_tree, aligned_trees_2_3_inner)
    assert_mapping_matches_tree(alignment)
    assert alignment.moc == pixel_tree_2.to_moc().intersection(pixel_tree_3.to_moc())


def test_pixel_tree_alignment_with_moc_left(pixel_tree_2, pixel_tree_3, aligned_trees_2_3_left):
    moc_pixels = aligned_trees_2_3_left.get_healpix_pixels()[:-3]
    correct_aligned_tree = PixelTree.from_healpix(moc_pixels)
    moc = correct_aligned_tree.to_moc()

    alignment = align_with_mocs(pixel_tree_2, pixel_tree_3, moc, pixel_tree_3.to_moc(), alignment_type="left")
    assert_trees_equal(alignment.pixel_tree, correct_aligned_tree)
    assert_mapping_matches_tree(alignment)
    assert alignment.moc == moc

    alignment = align_with_mocs(pixel_tree_2, pixel_tree_3, pixel_tree_2.to_moc(), moc, alignment_type="left")
    assert_trees_equal(alignment.pixel_tree, aligned_trees_2_3_left)
    assert_mapping_matches_tree(alignment)
    assert alignment.moc == pixel_tree_2.to_moc()


def test_pixel_tree_alignment_with_moc_right(pixel_tree_2, pixel_tree_3, aligned_trees_2_3_right):
    moc_pixels = aligned_trees_2_3_right.get_healpix_pixels()[:-3]
    correct_aligned_tree = PixelTree.from_healpix(moc_pixels)
    moc = correct_aligned_tree.to_moc()

    alignment = align_with_mocs(
        pixel_tree_2, pixel_tree_3, moc, pixel_tree_3.to_moc(), alignment_type="right"
    )
    assert_trees_equal(alignment.pixel_tree, aligned_trees_2_3_right)
    assert_mapping_matches_tree(alignment)
    assert alignment.moc == pixel_tree_3.to_moc()

    alignment = align_with_mocs(
        pixel_tree_2, pixel_tree_3, pixel_tree_2.to_moc(), moc, alignment_type="right"
    )
    assert_trees_equal(alignment.pixel_tree, correct_aligned_tree)
    assert_mapping_matches_tree(alignment)
    assert alignment.moc == moc


def test_pixel_tree_alignment_with_moc_outer(pixel_tree_2, pixel_tree_3, aligned_trees_2_3_outer):
    moc_pixels = aligned_trees_2_3_outer.get_healpix_pixels()[:-3]
    moc = PixelTree.from_healpix(moc_pixels).to_moc()
    # moc doesn't include pixels (1,45), (1,46), (1,47). Outer align with right tree moc includes 46 and 47
    # from right tree
    left_moc_correct_tree = PixelTree.from_healpix(moc_pixels + [HealpixPixel(1, 46), HealpixPixel(1, 47)])
    alignment = align_with_mocs(
        pixel_tree_2, pixel_tree_3, moc, pixel_tree_3.to_moc(), alignment_type="outer"
    )
    assert_trees_equal(alignment.pixel_tree, left_moc_correct_tree)
    assert_mapping_matches_tree(alignment)
    assert alignment.moc == moc.union(pixel_tree_3.to_moc())

    # moc doesn't include pixels (1,45), (1,46), (1,47). Outer align with left tree moc includes 45 and 46
    # from left tree
    right_moc_correct_tree = PixelTree.from_healpix(moc_pixels + [HealpixPixel(1, 45), HealpixPixel(1, 46)])
    alignment = align_with_mocs(
        pixel_tree_2, pixel_tree_3, pixel_tree_2.to_moc(), moc, alignment_type="outer"
    )
    assert_trees_equal(alignment.pixel_tree, right_moc_correct_tree)
    assert_mapping_matches_tree(alignment)
    assert alignment.moc == pixel_tree_2.to_moc().union(moc)

    both_moc_correct_tree = PixelTree.from_healpix(moc_pixels)
    alignment = align_with_mocs(pixel_tree_2, pixel_tree_3, moc, moc, alignment_type="outer")
    assert_trees_equal(alignment.pixel_tree, both_moc_correct_tree)
    assert_mapping_matches_tree(alignment)
    assert alignment.moc == moc


def test_catalog_align_inner(pixel_tree_2, pixel_tree_3, aligned_trees_2_3_inner, catalog_info):
    left_cat = Catalog(catalog_info, pixel_tree_2)
    right_cat = Catalog(catalog_info, pixel_tree_3)
    alignment = left_cat.align(right_cat)
    assert_trees_equal(alignment.pixel_tree, aligned_trees_2_3_inner)
    assert_mapping_matches_tree(alignment)
    assert alignment.moc == pixel_tree_2.to_moc().intersection(pixel_tree_3.to_moc())

    moc_pixels = aligned_trees_2_3_inner.get_healpix_pixels()[:-3]
    correct_aligned_tree = PixelTree.from_healpix(moc_pixels)
    moc = correct_aligned_tree.to_moc()

    left_cat_with_moc = Catalog(catalog_info, pixel_tree_2, moc=moc)
    alignment = left_cat_with_moc.align(right_cat)
    assert_trees_equal(alignment.pixel_tree, correct_aligned_tree)
    assert_mapping_matches_tree(alignment)
    assert alignment.moc == moc.intersection(right_cat.pixel_tree.to_moc())

    right_cat_with_moc = Catalog(catalog_info, pixel_tree_3, moc=moc)
    alignment = left_cat.align(right_cat_with_moc)
    assert_trees_equal(alignment.pixel_tree, correct_aligned_tree)
    assert_mapping_matches_tree(alignment)
    assert alignment.moc == left_cat.pixel_tree.to_moc().intersection(moc)


def test_catalog_align_left(pixel_tree_2, pixel_tree_3, aligned_trees_2_3_left, catalog_info):
    left_cat = Catalog(catalog_info, pixel_tree_2)
    right_cat = Catalog(catalog_info, pixel_tree_3)
    alignment = left_cat.align(right_cat, alignment_type="left")
    assert_trees_equal(alignment.pixel_tree, aligned_trees_2_3_left)
    assert_mapping_matches_tree(alignment)
    assert alignment.moc == pixel_tree_2.to_moc()

    moc_pixels = aligned_trees_2_3_left.get_healpix_pixels()[:-3]
    correct_aligned_tree = PixelTree.from_healpix(moc_pixels)
    moc = correct_aligned_tree.to_moc()

    left_cat_with_moc = Catalog(catalog_info, pixel_tree_2, moc=moc)
    alignment = left_cat_with_moc.align(right_cat, alignment_type="left")
    assert_trees_equal(alignment.pixel_tree, correct_aligned_tree)
    assert_mapping_matches_tree(alignment)
    assert alignment.moc == moc

    right_cat_with_moc = Catalog(catalog_info, pixel_tree_3, moc=moc)
    alignment = left_cat.align(right_cat_with_moc, alignment_type="left")
    assert_trees_equal(alignment.pixel_tree, aligned_trees_2_3_left)
    assert_mapping_matches_tree(alignment)
    assert alignment.moc == pixel_tree_2.to_moc()


def test_catalog_align_right(pixel_tree_2, pixel_tree_3, aligned_trees_2_3_right, catalog_info):
    left_cat = Catalog(catalog_info, pixel_tree_2)
    right_cat = Catalog(catalog_info, pixel_tree_3)
    alignment = left_cat.align(right_cat, alignment_type="right")
    assert_trees_equal(alignment.pixel_tree, aligned_trees_2_3_right)
    assert_mapping_matches_tree(alignment)
    assert alignment.moc == right_cat.pixel_tree.to_moc()

    moc_pixels = aligned_trees_2_3_right.get_healpix_pixels()[:-3]
    correct_aligned_tree = PixelTree.from_healpix(moc_pixels)
    moc = correct_aligned_tree.to_moc()

    left_cat_with_moc = Catalog(catalog_info, pixel_tree_2, moc=moc)
    alignment = left_cat_with_moc.align(right_cat, alignment_type="right")
    assert_trees_equal(alignment.pixel_tree, aligned_trees_2_3_right)
    assert_mapping_matches_tree(alignment)
    assert alignment.moc == right_cat.pixel_tree.to_moc()

    right_cat_with_moc = Catalog(catalog_info, pixel_tree_3, moc=moc)
    alignment = left_cat.align(right_cat_with_moc, alignment_type="right")
    assert_trees_equal(alignment.pixel_tree, correct_aligned_tree)
    assert_mapping_matches_tree(alignment)
    assert alignment.moc == moc


def test_catalog_align_outer(pixel_tree_2, pixel_tree_3, aligned_trees_2_3_outer, catalog_info):
    left_cat = Catalog(catalog_info, pixel_tree_2)
    right_cat = Catalog(catalog_info, pixel_tree_3)
    alignment = left_cat.align(right_cat, alignment_type="outer")
    assert_trees_equal(alignment.pixel_tree, aligned_trees_2_3_outer)
    assert_mapping_matches_tree(alignment)
    assert alignment.moc == left_cat.pixel_tree.to_moc().union(right_cat.pixel_tree.to_moc())

    moc_pixels = aligned_trees_2_3_outer.get_healpix_pixels()[:-3]
    moc = PixelTree.from_healpix(moc_pixels).to_moc()

    left_moc_correct_tree = PixelTree.from_healpix(moc_pixels + [HealpixPixel(1, 46), HealpixPixel(1, 47)])
    left_cat_with_moc = Catalog(catalog_info, pixel_tree_2, moc=moc)
    alignment = left_cat_with_moc.align(right_cat, alignment_type="outer")
    assert_trees_equal(alignment.pixel_tree, left_moc_correct_tree)
    assert_mapping_matches_tree(alignment)
    assert alignment.moc == moc.union(right_cat.pixel_tree.to_moc())

    right_moc_correct_tree = PixelTree.from_healpix(moc_pixels + [HealpixPixel(1, 45), HealpixPixel(1, 46)])
    right_cat_with_moc = Catalog(catalog_info, pixel_tree_3, moc=moc)
    alignment = left_cat.align(right_cat_with_moc, alignment_type="outer")
    assert_trees_equal(alignment.pixel_tree, right_moc_correct_tree)
    assert_mapping_matches_tree(alignment)
    assert alignment.moc == left_cat.pixel_tree.to_moc().union(moc)

    both_moc_correct_tree = PixelTree.from_healpix(moc_pixels)
    alignment = left_cat_with_moc.align(right_cat_with_moc, alignment_type="outer")
    assert_trees_equal(alignment.pixel_tree, both_moc_correct_tree)
    assert_mapping_matches_tree(alignment)
    assert alignment.moc == moc


@pytest.mark.timeout(20)
def test_outer_align_start_0():
    left_tree = PixelTree.from_healpix([HealpixPixel(0, 0)])
    right_tree = PixelTree.from_healpix([HealpixPixel(1, 1)])
    alignment = align_trees(left_tree, right_tree, alignment_type="outer")
    expected_tree = PixelTree.from_healpix(
        [HealpixPixel(1, 0), HealpixPixel(1, 1), HealpixPixel(1, 2), HealpixPixel(1, 3)]
    )
    assert_trees_equal(alignment.pixel_tree, expected_tree)
    assert_mapping_matches_tree(alignment)
