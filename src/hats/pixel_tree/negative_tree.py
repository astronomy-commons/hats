from typing import List

from hats.pixel_math import HealpixPixel
from hats.pixel_tree import PixelAlignmentType, align_trees
from hats.pixel_tree.pixel_tree import PixelTree


def compute_negative_tree_pixels(tree: PixelTree) -> List[HealpixPixel]:
    """Computes a 'negative pixel tree' consisting of the pixels needed to cover the full sky not in the tree

    Args:
        tree (PixelTree): the input tree to compute the negative of

    Returns (List[HealpixPixel]):
        A list of HEALPix pixels needed to cover the part of the sky not covered by the tree, using the least
        number of pixels possible.
    """
    tree_pixels = set(tree.get_healpix_pixels())
    full_tree = PixelTree.from_healpix([(0, i) for i in range(12)])
    alignment = align_trees(full_tree, tree, alignment_type=PixelAlignmentType.OUTER)
    aligned_tree = alignment.pixel_tree
    return [p for p in aligned_tree.get_healpix_pixels() if p not in tree_pixels]
