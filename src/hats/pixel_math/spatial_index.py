from __future__ import annotations

import numpy as np
import pandas as pd

import hats.pixel_math.healpix_shim as hp

SPATIAL_INDEX_COLUMN = "_healpix_29"
SPATIAL_INDEX_ORDER = 29


def compute_spatial_index(
    ra_values: float | list[float],
    dec_values: float | list[float],
    spatial_index_order: int = SPATIAL_INDEX_ORDER,
) -> np.ndarray:
    """Compute the healpix index field.

    Parameters
    ----------
    ra_values : float | list[float]
        celestial coordinates, right ascension in degrees
    dec_values : float | list[float]
        celestial coordinates, declination in degrees
    spatial_index_order: int
        (Default value = SPATIAL_INDEX_ORDER = 29) order to use for spatial index

    Returns
    -------
    np.ndarray
        HEALPix pixel indices at specified order, for all coordinates provided.

    Raises
    ------
    ValueError
        if the length of the input lists don't match.
    """
    if pd.api.types.is_list_like(ra_values) or pd.api.types.is_list_like(dec_values):
        if not (pd.api.types.is_list_like(ra_values) and pd.api.types.is_list_like(dec_values)):
            raise ValueError("ra and dec cannot be mix of array and scalar")
        if len(ra_values) != len(dec_values):
            raise ValueError("ra and dec arrays should have the same length")

    return hp.radec2pix(spatial_index_order, ra_values, dec_values)


def spatial_index_to_healpix(
    ids: list[int], target_order: int = SPATIAL_INDEX_ORDER, spatial_index_order: int = SPATIAL_INDEX_ORDER
) -> np.ndarray:
    """Convert healpix index values to the healpix pixel at the specified order

    Parameters
    ----------
    ids : list[int]
        list of well-formatted _healpix_29 values
    target_order : int
        Defaults to `SPATIAL_INDEX_ORDER`. The order of the pixel to get from the healpix index.
    spatial_index_order: int
        (Default value = SPATIAL_INDEX_ORDER = 29) order to use for spatial index

    Returns
    -------
    np.ndarray
        numpy array of target_order pixels from the healpix index
    """
    delta_order = spatial_index_order - target_order
    return np.array(ids) >> (2 * delta_order)


def healpix_to_spatial_index(
    order: int | list[int], pixel: int | list[int], spatial_index_order: int = SPATIAL_INDEX_ORDER
) -> np.int64 | np.ndarray:
    """Convert a healpix pixel to the healpix index

    This maps the healpix pixel to the lowest pixel number within that pixel at the specified healpix order.

    Useful for operations such as filtering by _healpix_29.

    Parameters
    ----------
    order : int | list[int]
        order of pixel to convert
    pixel : int | list[int]
        pixel number in nested ordering of pixel to convert
    spatial_index_order: int
        (Default value = SPATIAL_INDEX_ORDER = 29) order to use for spatial index

    Returns
    -------
    np.int64 | np.ndarray
        healpix index or numpy array of healpix indices
    """
    order = np.int64(order)
    pixel = np.int64(pixel)
    pixel_higher_order = pixel * (4 ** (spatial_index_order - order))
    return pixel_higher_order
