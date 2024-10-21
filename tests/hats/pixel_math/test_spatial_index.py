"""Test construction (and de-construction) of the healpix-based spatial index"""

import numpy as np
import numpy.testing as npt
import pytest

import hats.pixel_math.healpix_shim as hp
from hats.pixel_math.spatial_index import (
    SPATIAL_INDEX_ORDER,
    compute_spatial_index,
    healpix_to_spatial_index,
    spatial_index_to_healpix,
)


def test_single():
    """Single point. Adheres to specification."""
    result = compute_spatial_index([5], [5])
    expected = hp.ang2pix(
        2**SPATIAL_INDEX_ORDER,
        [5],
        [5],
        nest=True,
        lonlat=True,
    )

    npt.assert_array_equal(result, expected)


def test_jagged_list():
    """Arrays of mismatched lengths."""
    with pytest.raises(ValueError):
        compute_spatial_index([5, 1, 5], [5])


def test_short_list():
    """Multiple points that will sit in the same higher-order-pixel."""
    ra = [5, 1, 5]
    dec = [5, 1, 5]
    result = compute_spatial_index(ra, dec)
    expected = hp.ang2pix(
        2**SPATIAL_INDEX_ORDER,
        ra,
        dec,
        nest=True,
        lonlat=True,
    )
    npt.assert_array_equal(result, expected)


def test_list():
    """Multiple points that will sit in the same higher-order-pixel."""
    ra = [5, 5, 5, 1, 5, 5, 5, 1, 5]
    dec = [5, 5, 5, 1, 5, 5, 5, 1, 5]
    result = compute_spatial_index(ra, dec)
    expected = hp.ang2pix(
        2**SPATIAL_INDEX_ORDER,
        ra,
        dec,
        nest=True,
        lonlat=True,
    )
    npt.assert_array_equal(result, expected)


def test_load():
    """Generate a kinda big array and make sure the method completes in under a second.
    If this method is failing due to timeouts, please refactor to keep within the time limit.
    """
    rng = np.random.default_rng(seed=800)
    test_num = 1_000_000

    ra_arr = rng.random(test_num)
    dec_arr = rng.random(test_num)
    result = compute_spatial_index(ra_arr, dec_arr)

    assert len(result) == test_num


def test_spatial_index_to_healpix():
    """Test the inverse operation"""
    ids = [
        3458764513820540924,
        3458764513820540924,
        3458764513820540924,
        3138264513820540924,  # out of sequence
        3458764513820540924,
        3458764513820540924,
        3458764513820540924,
        3138264513820540924,  # out of sequence
        3458764513820540924,
    ]

    result = spatial_index_to_healpix(ids)

    expected = [
        3458764513820540924,
        3458764513820540924,
        3458764513820540924,
        3138264513820540924,  # out of sequence
        3458764513820540924,
        3458764513820540924,
        3458764513820540924,
        3138264513820540924,  # out of sequence
        3458764513820540924,
    ]

    npt.assert_array_equal(result, expected)


def test_spatial_index_to_healpix_low_order():
    """Test the inverse operation"""
    ids = [
        3458764513820540924,
        3458764513820540924,
        3458764513820540924,
        3138264513820540924,  # out of sequence
        3458764513820540924,
        3458764513820540924,
        3458764513820540924,
        3138264513820540924,  # out of sequence
        3458764513820540924,
    ]

    result = spatial_index_to_healpix(ids, target_order=4)

    expected = [i >> (2 * (29 - 4)) for i in ids]

    npt.assert_array_equal(result, expected)


def test_healpix_to_spatial_index_single():
    orders = [3, 3, 4, 1]
    pixels = [0, 12, 1231, 11]
    pixels_at_high_order = [p * (4 ** (SPATIAL_INDEX_ORDER - o)) for o, p in zip(orders, pixels)]
    lon, lat = hp.pix2ang(
        [2**SPATIAL_INDEX_ORDER] * len(orders), pixels_at_high_order, nest=True, lonlat=True
    )
    actual_spatial_indices = compute_spatial_index(lon, lat)
    test_spatial_indices = [healpix_to_spatial_index(o, p) for o, p in zip(orders, pixels)]
    assert np.all(test_spatial_indices == actual_spatial_indices)


def test_healpix_to_spatial_index_array():
    orders = [3, 3, 4, 1]
    pixels = [0, 12, 1231, 11]
    pixels_at_high_order = [p * (4 ** (SPATIAL_INDEX_ORDER - o)) for o, p in zip(orders, pixels)]
    lon, lat = hp.pix2ang(
        [2**SPATIAL_INDEX_ORDER] * len(orders), pixels_at_high_order, nest=True, lonlat=True
    )
    actual_spatial_indices = compute_spatial_index(lon, lat)
    test_spatial_indices = healpix_to_spatial_index(orders, pixels)
    assert np.all(test_spatial_indices == actual_spatial_indices)
