import numba
import numpy as np
from mocpy import MOC
from numba import njit

from hats.pixel_tree.pixel_tree import PixelTree


def filter_by_moc(
    tree: PixelTree,
    moc: MOC,
) -> PixelTree:
    """Filters a pixel tree to only include the pixels that overlap with the pixels in the moc

    Args:
        tree (PixelTree): The tree to perform the filtering on
        moc (mocpy.MOC): The moc to use to filter

    Returns:
        A new PixelTree object with only the pixels from the input tree that overlap with the moc.
    """
    moc_ranges = moc.to_depth29_ranges
    # Convert tree intervals to order 29 to match moc intervals
    tree_29_ranges = tree.tree << (2 * (29 - tree.tree_order))
    tree_mask = perform_filter_by_moc(tree_29_ranges, moc_ranges)
    return PixelTree(tree.tree[tree_mask], tree.tree_order)


@njit(
    numba.bool_[::1](
        numba.int64[:, :],
        numba.uint64[:, :],
    )
)
def perform_filter_by_moc(
    tree: np.ndarray,
    moc: np.ndarray,
) -> np.ndarray:  # pragma: no cover
    """Performs filtering with lists of pixel intervals

    Input interval lists must be at the same order.

    Args:
        tree (np.ndarray): Array of pixel intervals to be filtered
        moc (np.ndarray): Array of pixel intervals to be used to filter

    Returns:
        A boolean array of dimension tree.shape[0] which masks which pixels in tree overlap with the pixels in
        moc
    """
    output = np.full(tree.shape[0], fill_value=False, dtype=np.bool_)
    tree_index = 0
    moc_index = 0
    while tree_index < len(tree) and moc_index < len(moc):
        tree_pix = tree[tree_index]
        moc_pix = moc[moc_index]
        if tree_pix[0] >= moc_pix[1]:
            # Don't overlap, tree pixel ahead so move onto next MOC pixel
            moc_index += 1
            continue
        if moc_pix[0] >= tree_pix[1]:
            # Don't overlap, MOC pixel ahead so move onto next tree pixel
            tree_index += 1
            continue
        # Pixels overlap, so include current tree pixel and check next tree pixel
        output[tree_index] = True
        tree_index += 1
    return output
