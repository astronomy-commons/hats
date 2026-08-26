"""Test construction (and de-construction) of the healpix-based spatial index"""

import numpy as np
import numpy.testing as npt
import pyarrow as pa
import pytest

import hats.pixel_math.healpix_shim as hp
from hats.pixel_math.spatial_index import (
    SPATIAL_INDEX_COLUMN,
    SPATIAL_INDEX_ORDER,
    compute_spatial_index,
    healpix_to_spatial_index,
    spatial_index_to_healpix,
    split_to_row_groups,
)


def test_single_array():
    """Single point. Adheres to specification."""
    result = compute_spatial_index([5], [5])
    expected = hp.radec2pix(SPATIAL_INDEX_ORDER, [5], [5])

    npt.assert_array_equal(result, expected)


def test_single_scalar():
    """Single point. Adheres to specification."""
    result = compute_spatial_index(5, 5)
    assert result == 1370628467894962607

    expected = hp.radec2pix(SPATIAL_INDEX_ORDER, [5], [5])
    npt.assert_array_equal(result, expected)


def test_jagged_list():
    """Arrays of mismatched lengths."""
    with pytest.raises(ValueError, match="should have the same length"):
        compute_spatial_index([5, 1, 5], [5])


def test_mixed_list():
    """Mix of array and scalar values."""
    with pytest.raises(ValueError, match="mix of array and scalar"):
        compute_spatial_index([5, 1, 5], 5)

    with pytest.raises(ValueError, match="mix of array and scalar"):
        compute_spatial_index(5, [5, 1, 5])


def test_short_list():
    """Multiple points that will sit in the same higher-order-pixel."""
    ra = [5, 1, 5]
    dec = [5, 1, 5]
    result = compute_spatial_index(ra, dec)
    expected = hp.radec2pix(SPATIAL_INDEX_ORDER, ra, dec)
    npt.assert_array_equal(result, expected)


def test_list():
    """Multiple points that will sit in the same higher-order-pixel."""
    ra = [5, 5, 5, 1, 5, 5, 5, 1, 5]
    dec = [5, 5, 5, 1, 5, 5, 5, 1, 5]
    result = compute_spatial_index(ra, dec)
    expected = hp.radec2pix(SPATIAL_INDEX_ORDER, ra, dec)
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

    ra = [45.0, 45.0, 0.0, 225.0]
    dec = [7.11477952e-08, 1.94712207e01, 1.44775123e01, 4.18103150e01]

    actual_spatial_indices = compute_spatial_index(ra, dec)
    test_spatial_indices = [healpix_to_spatial_index(o, p) for o, p in zip(orders, pixels)]
    assert np.all(test_spatial_indices == actual_spatial_indices)


def test_healpix_to_spatial_index_array():
    orders = [3, 3, 4, 1]
    pixels = [0, 12, 1231, 11]

    ra = [45.0, 45.0, 0.0, 225.0]
    dec = [7.11477952e-08, 1.94712207e01, 1.44775123e01, 4.18103150e01]
    actual_spatial_indices = compute_spatial_index(ra, dec)
    test_spatial_indices = healpix_to_spatial_index(orders, pixels)
    assert np.all(test_spatial_indices == actual_spatial_indices)


def test_split_to_row_groups():
    # table with no spatial index
    table = pa.Table.from_arrays([[1.0] * 27], names=["data"])
    # table with spatial index
    # data | pixel index, order = (0, 1, 2)
    # -----|-------------------------------
    #  1.0 | (0, 0, 0)
    #  2.0 | (0, 1, 4)
    #  3.0 | (0, 2, 8)
    #  4.0 | (0, 2, 9)
    data = [1.0, 2.0, 3.0, 4.0]
    orders = [2, 2, 2, 2]
    pixels = [0, 4, 8, 9]
    spatial_index = healpix_to_spatial_index(orders, pixels, spatial_index_order=SPATIAL_INDEX_ORDER)
    spatial_table = pa.Table.from_arrays([data, spatial_index], names=["data", SPATIAL_INDEX_COLUMN])

    # row_group_kwargs is None
    split_tables = split_to_row_groups(table, None, None)
    assert [len(t) for t in split_tables] == [27]

    # row_group_kwargs = dict()
    split_tables = split_to_row_groups(table, {}, None)
    assert [len(t) for t in split_tables] == [27]

    # row_group_kwargs = {"unused": 1234}
    split_tables = split_to_row_groups(table, {"unused": 1234}, None)
    assert [len(t) for t in split_tables] == [27]

    # row_group_kwargs["num_rows"] == 10
    split_tables = split_to_row_groups(table, {"num_rows": 10}, None)
    assert [len(t) for t in split_tables] == [10, 10, 7]

    # row_group_kwargs["num_rows"] == 1000
    split_tables = split_to_row_groups(table, {"num_rows": 1000}, None)
    assert [len(t) for t in split_tables] == [27]

    # row_group_kwargs["num_rows"] == 0
    with pytest.raises(ValueError, match="num_rows should be an integer >= 1"):
        split_tables = split_to_row_groups(table, {"num_rows": 0}, None)

    # row_group_kwargs["num_rows"] < 0
    with pytest.raises(ValueError, match="num_rows should be an integer >= 1"):
        split_tables = split_to_row_groups(table, {"num_rows": -0}, None)

    # row_group_kwargs["num_rows"] is not an integer
    with pytest.raises(ValueError, match="num_rows should be an integer >= 1"):
        split_tables = split_to_row_groups(table, {"num_rows": 0.123}, None)

    # row_group_kwargs["subtile_order_delta"] < 0
    with pytest.raises(ValueError, match="subtile_order_delta should be an integer >= 0"):
        split_tables = split_to_row_groups(table, {"subtile_order_delta": -1}, None)

    # row_group_kwargs["subtile_order_delta"] is not an integer
    with pytest.raises(ValueError, match="subtile_order_delta should be an integer >= 0"):
        split_tables = split_to_row_groups(table, {"subtile_order_delta": 0.123}, None)

    # row_group_kwargs["subtile_order_delta"] == 0, pixel_order = 0
    # rows split at order 0 => all rows in the same group
    split_tables = split_to_row_groups(spatial_table, {"subtile_order_delta": 0}, 0)
    assert [len(t) for t in split_tables] == [4]

    # row_group_kwargs["subtile_order_delta"] == 0, pixel_order = 1
    # rows split at order 1
    split_tables = split_to_row_groups(spatial_table, {"subtile_order_delta": 0}, 1)
    assert [len(t) for t in split_tables] == [1, 1, 2]

    # row_group_kwargs["subtile_order_delta"] == 0, pixel_order = 2
    # rows split at order 2
    split_tables = split_to_row_groups(spatial_table, {"subtile_order_delta": 0}, 2)
    assert [len(t) for t in split_tables] == [1, 1, 1, 1]

    # row_group_kwargs["subtile_order_delta"] == 1, pixel_order = 0
    # rows split at order 1
    split_tables = split_to_row_groups(spatial_table, {"subtile_order_delta": 1}, 0)
    assert [len(t) for t in split_tables] == [1, 1, 2]

    # row_group_kwargs["subtile_order_delta"] == 0, pixel_order is None
    with pytest.raises(ValueError, match="pixel_order should be an integer >= 0"):
        split_tables = split_to_row_groups(spatial_table, {"subtile_order_delta": 0}, None)

    # row_group_kwargs["subtile_order_delta"] == 0, pixel_order < 0
    with pytest.raises(ValueError, match="pixel_order should be an integer >= 0"):
        split_tables = split_to_row_groups(spatial_table, {"subtile_order_delta": 0}, -1)

    # row_group_kwargs["subtile_order_delta"] == 0, pixel_order is not an integer
    with pytest.raises(ValueError, match="pixel_order should be an integer >= 0"):
        split_tables = split_to_row_groups(spatial_table, {"subtile_order_delta": 0}, 0.123)

    # row_group_kwargs["subtile_order_delta"] == 0 but there's no spatial index column
    with pytest.raises(ValueError, match="table has no spatial index column"):
        split_tables = split_to_row_groups(table, {"subtile_order_delta": 0}, 0)

    # row_group_kwargs["num_rows"] == 1 and row_group_kwargs["subtile_order_delta"] == 0
    # (num_rows takes precedence over subtile_order_delta)
    split_tables = split_to_row_groups(spatial_table, {"num_rows": 1, "subtile_order_delta": 0}, 0)
    assert [len(t) for t in split_tables] == [1, 1, 1, 1]
