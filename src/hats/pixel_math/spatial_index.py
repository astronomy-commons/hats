from __future__ import annotations

from numbers import Integral

import numpy as np
import pandas as pd
import pyarrow as pa
from numpy.typing import ArrayLike

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
    ids: ArrayLike,
    target_order: int = SPATIAL_INDEX_ORDER,
    spatial_index_order: int = SPATIAL_INDEX_ORDER,
) -> np.ndarray:
    """Convert healpix index values to the healpix pixel at the specified order

    Parameters
    ----------
    ids : ArrayLike
        list (or array) of well-formatted _healpix_29 values
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


def split_to_row_groups(
    table: pa.Table, row_group_kwargs: dict | None, pixel_order: int | None = None
) -> list[pa.Table]:
    """Split the pixel table into its row group chunks according to the specified splitting strategy.

    Parameters
    ----------
    table : pa.Table
        Pixel table.
    row_group_kwargs : dict
        if "num_rows" (int >= 1) in row_group_kwargs, limit each chunk to a maximum of this many rows.
            e.g. if the input table has 32 rows and num_rows == 10, then the chunks will have
            [10, 10, 10, 2] rows.
        if "subtile_order_delta" (int >= 0) in row_group_kwargs, create row groups corresponding to angular
            proximity, approximated by HEALPix pixels.
            If subtile_order_delta == 0, then each row group contains objects in the same HEALPix pixel of
            order pixel_order. If subtile_order_delta == 1, then the row groups are split by HEALPix pixels
            at order pixel_order + 1, etc. Higher numbers correspond to a finer grid (smaller pixels, fewer
            rows per pixel).
    pixel_order : int | None
        The HEALPix order to split when using subtile_order_delta.

    Returns
    -------
    split_tables : list[pa.Table]
        The input table split into row group chunks.
    """
    if (row_group_kwargs is None) or (row_group_kwargs == {}):
        return [table]
    if ("num_rows" in row_group_kwargs) and ("subtile_order_delta" in row_group_kwargs):
        raise ValueError(
            "row_group_kwargs contains conflicting keys. choose only one: num_rows or subtile_order_delta"
        )
    if "num_rows" in row_group_kwargs:
        chunk_size = row_group_kwargs["num_rows"]
        if not (isinstance(chunk_size, Integral) and (chunk_size >= 1)):
            raise ValueError("num_rows should be an integer >= 1")
        return [table.slice(i, chunk_size) for i in range(0, len(table), chunk_size)]
    if "subtile_order_delta" in row_group_kwargs:
        if not (
            isinstance(row_group_kwargs["subtile_order_delta"], Integral)
            and (row_group_kwargs["subtile_order_delta"] >= 0)
        ):
            raise ValueError("subtile_order_delta should be an integer >= 0")
        if not (isinstance(pixel_order, Integral) and (pixel_order >= 0)):
            raise ValueError("pixel_order should be an integer >= 0")
        if SPATIAL_INDEX_COLUMN not in table.schema.names:
            raise ValueError(
                "table has no spatial index column. You can generate one using compute_spatial_index()."
            )
        split_tables = []
        parent_pixels = table[SPATIAL_INDEX_COLUMN].to_numpy()
        target_order = row_group_kwargs["subtile_order_delta"] + pixel_order
        child_pixs = spatial_index_to_healpix(parent_pixels, target_order=target_order)
        for child_pix in np.unique(child_pixs):
            indices = np.where(child_pixs == child_pix)[0]
            row_group = table.take(pa.array(indices))
            split_tables.append(row_group)
        return split_tables
    # no valid keys found
    raise ValueError(
        "no valid keys in row_group_kwargs. valid options are `num_rows` [int >= 1] or `subtile_order_delta` [int >= 0]."
    )
