from typing import List

import numpy as np

from hats.pixel_math.healpix_pixel import HealpixPixel


def get_pixel_argsort(pixels: List[HealpixPixel]):
    """Perform an indirect sort of a list of HealpixPixels.

    This will order the pixels according to a breadth-first traversal of the healpix
    pixel hierarchy, not merely by increasing order by Norder/Npix (depth-first ordering).
    This is similar to ordering fully by _healpix_29.

    Args:
        pixels (List[HealpixPixel]): array to sort

    Returns:
        array of indices that sort the pixels in breadth-first order.
    """
    if pixels is None or len(pixels) == 0:
        return []
    # Construct a parallel list of exploded, high order pixels.
    highest_order = np.max(pixels).order

    constant_breadth_pixel = [pixel.pixel * (4 ** (highest_order - pixel.order)) for pixel in pixels]

    # Get the argsort of the higher order array.
    return np.argsort(constant_breadth_pixel, kind="stable")


def sort_pixels(pixels: List[HealpixPixel]):
    """Perform an sort of a list of HealpixPixels.

    This will order the pixels according to a breadth-first traversal of the healpix
    pixel hierarchy, not merely by increasing order by Norder/Npix (depth-first ordering).
    This is similar to ordering fully by _healpix_29.

    Args:
        pixels (List[HealpixPixel]): array to sort

    Returns:
        pixels sorted in breadth-first order
    """
    if pixels is None or len(pixels) == 0:
        return []
    argsort = get_pixel_argsort(pixels)
    return np.array(pixels)[argsort]


def get_pixels_from_intervals(intervals: np.ndarray, tree_order: int) -> np.ndarray:
    """Computes an array of HEALPix [order, pixel] for an array of intervals

    Args:
        intervals (np.ndarray): Array of intervals of the start and end pixel numbers of HEALPix pixels.
            Must be NESTED numbering scheme.
        tree_order (int): The order of the pixel numbers in the interval array

    Returns (np.ndarray):
        An array of [order, pixel] in NESTED numbering scheme for each interval in the array.
    """
    if intervals.shape[0] == 0:
        return np.empty((0, 2), dtype=np.int64)
    orders = np.full(intervals.shape[0], fill_value=-1)
    pixels = np.full(intervals.shape[0], fill_value=-1)
    # alignment uses (-1, -1) as a missing pixel, so we can't use the HEALPix math on these elements
    non_negative_mask = intervals.T[0] >= 0
    start_intervals = intervals.T[0][non_negative_mask]
    end_intervals = intervals.T[1][non_negative_mask]
    orders[non_negative_mask] = tree_order - (np.int64(np.log2(end_intervals - start_intervals)) >> 1)
    pixels[non_negative_mask] = start_intervals >> 2 * (tree_order - orders[non_negative_mask])
    return np.array([orders, pixels]).T
