from __future__ import annotations

from typing import List

import numpy as np

import hats.pixel_math.healpix_shim as hp

SPATIAL_INDEX_COLUMN = "_healpix_29"
SPATIAL_INDEX_ORDER = 29


def compute_spatial_index(ra_values: List[float], dec_values: List[float]) -> np.ndarray:
    """Compute the healpix index field.

    Args:
        ra_values (List[float]): celestial coordinates, right ascension in degrees
        dec_values (List[float]): celestial coordinates, declination in degrees
    Returns:
        one-dimensional numpy array of int64s with healpix NESTED pixel numbers at order 29
    Raises:
        ValueError: if the length of the input lists don't match.
    """
    if len(ra_values) != len(dec_values):
        raise ValueError("ra and dec arrays should have the same length")

    mapped_pixels = hp.ang2pix(2**SPATIAL_INDEX_ORDER, ra_values, dec_values, nest=True, lonlat=True)

    return mapped_pixels


def spatial_index_to_healpix(ids: List[int], target_order: int = SPATIAL_INDEX_ORDER) -> np.ndarray:
    """Convert _healpix_29 values to the healpix pixel at the specified order

    Args:
        ids (List[int64]): list of well-formatted _healpix_29 values
        target_order (int64): Defaults to `SPATIAL_INDEX_ORDER`.
            The order of the pixel to get from the healpix index.
    Returns:
        numpy array of target_order pixels from the healpix index
    """
    delta_order = SPATIAL_INDEX_ORDER - target_order
    return np.array(ids) >> (2 * delta_order)


def healpix_to_spatial_index(order: int | List[int], pixel: int | List[int]) -> np.int64 | np.ndarray:
    """Convert a healpix pixel to the healpix index

    This maps the healpix pixel to the lowest pixel number within that pixel at order 29.

    Useful for operations such as filtering by _healpix_29.

    Args:
        order (int64 | List[int64]): order of pixel to convert
        pixel (int64 | List[int64]): pixel number in nested ordering of pixel to convert
    Returns:
        healpix index or numpy array of healpix indices
    """
    order = np.int64(order)
    pixel = np.int64(pixel)
    pixel_higher_order = pixel * (4 ** (SPATIAL_INDEX_ORDER - order))
    return pixel_higher_order
