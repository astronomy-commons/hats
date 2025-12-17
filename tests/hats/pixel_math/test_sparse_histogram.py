"""Test sparse histogram behavior."""

import numpy as np
import numpy.testing as npt
import pytest
from numpy import frombuffer

import hats.pixel_math.healpix_shim as hp
from hats.pixel_math.sparse_histogram import (
    HistogramAggregator,
    SparseHistogram,
    supplemental_count_histogram,
)


def test_make_empty():
    """Tests the initialization of an empty histogram at the specified order"""
    histogram = SparseHistogram([], [], 5)
    expected_hist = np.zeros(hp.order2npix(5))
    npt.assert_array_equal(expected_hist, histogram.to_array())


def test_sparse_histogram_error():
    """Tests error when trying to initialize a histogram with mismatched indexes and counts"""
    with pytest.raises(ValueError, match="must be same length"):
        SparseHistogram([], [5], 5)


def test_sparse_histogram_eq():
    """Tests the use of the equality operator with histograms"""
    histogram = SparseHistogram([11], [131], 0)

    assert histogram == SparseHistogram([11], [131], 0)
    assert histogram != SparseHistogram([11], [132], 0)
    assert histogram != ([11], [131], 0)


def test_sparse_histogram_str():
    """Tests histogram string output"""
    histogram = SparseHistogram([11], [131], 0)

    str_repr = str(histogram)
    assert str_repr.startswith("Histogram")
    assert "[131]" in str_repr
    assert "[11]" in str_repr


def test_read_write_round_trip(tmp_path):
    """Test that we can read what we write into a histogram file."""
    histogram = SparseHistogram([11], [131], 0)

    # Write as a sparse array
    file_name = tmp_path / "round_trip_sparse.npz"
    histogram.to_file(file_name)
    read_histogram = SparseHistogram.from_file(file_name)
    npt.assert_array_equal(read_histogram.to_array(), histogram.to_array())

    # Write as a dense 1-d numpy array
    file_name = tmp_path / "round_trip_dense.npz"
    histogram.to_dense_file(file_name)
    with open(file_name, "rb") as file_handle:
        read_histogram = frombuffer(file_handle.read(), dtype=np.int64)
    npt.assert_array_equal(read_histogram, histogram.to_array())


def test_add_same_order():
    """Test that we can add two histograms created from the same order, and get
    the expected results."""
    partial_histogram_left = SparseHistogram([11], [131], 0)

    partial_histogram_right = SparseHistogram([10, 11], [4, 15], 0)

    total_histogram = HistogramAggregator(0)
    total_histogram.add(partial_histogram_left)
    total_histogram.add(partial_histogram_right)

    expected = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 146]
    npt.assert_array_equal(total_histogram.full_histogram, expected)

    sparse = total_histogram.to_sparse()
    assert sparse == SparseHistogram([10, 11], [4, 146], 0)


def test_add_different_order():
    """Test that we can NOT add histograms of different healpix orders."""
    partial_histogram_left = SparseHistogram([11], [131], 0)
    partial_histogram_right = SparseHistogram([10, 11], [4, 15], 1)

    total_histogram = HistogramAggregator(0)
    total_histogram.add(partial_histogram_left)
    with pytest.raises(ValueError, match="partials have incompatible sizes"):
        total_histogram.add(partial_histogram_right)


def test_add_different_type():
    """Test that we can NOT add histograms of different healpix orders."""
    partial_histogram_left = SparseHistogram([11], [131], 0)

    total_histogram = HistogramAggregator(0)
    total_histogram.add(partial_histogram_left)

    total_histogram.add(None)
    total_histogram.add(SparseHistogram([], [], 0))

    with pytest.raises(ValueError, match="addends should be SparseHistogram"):
        total_histogram.add(5)

    with pytest.raises(ValueError, match="addends should be SparseHistogram"):
        total_histogram.add(([1, 2, 3, 4, 5], [1, 2, 3, 4, 5], 0))


def test_supplemental_count_histogram():
    mapped_pixels = [44, 45, 44, 44, 9, 10]
    supplemental_count = [19, 45, 98, 104, 56, 78]

    total_histogram_row_count = HistogramAggregator(2)
    total_histogram_suppl = HistogramAggregator(2)

    row_count, supplemental_sum = supplemental_count_histogram(
        mapped_pixels=mapped_pixels, supplemental_count=supplemental_count, highest_order=2
    )
    assert row_count == SparseHistogram([9, 10, 44, 45], [1, 1, 3, 1], 2)
    assert supplemental_sum == SparseHistogram([9, 10, 44, 45], [56, 78, 221, 45], 2)

    total_histogram_row_count.add(row_count)
    total_histogram_suppl.add(supplemental_sum)

    assert total_histogram_row_count.to_sparse() == SparseHistogram([9, 10, 44, 45], [1, 1, 3, 1], 2)
    assert total_histogram_suppl.to_sparse() == SparseHistogram([9, 10, 44, 45], [56, 78, 221, 45], 2)

    total_histogram_row_count.add(row_count)
    total_histogram_suppl.add(supplemental_sum)

    assert total_histogram_row_count.to_sparse() == SparseHistogram(
        [9, 10, 44, 45], np.array([1, 1, 3, 1]) * 2, 2
    )
    assert total_histogram_suppl.to_sparse() == SparseHistogram(
        [9, 10, 44, 45], np.array([56, 78, 221, 45]) * 2, 2
    )


def test_supplemental_count_histogram_edge():
    mapped_pixels = [44, 45, 44, 44, 9, 10]
    supplemental_count = [19, 45, 98, 104, 56, 78]

    row_count, supplemental_sum = supplemental_count_histogram(
        mapped_pixels=mapped_pixels, supplemental_count=None, highest_order=2
    )
    assert row_count == SparseHistogram([9, 10, 44, 45], [1, 1, 3, 1], 2)
    assert supplemental_sum is None

    with pytest.raises(ValueError, match="must be the same length"):
        supplemental_count_histogram(
            mapped_pixels=mapped_pixels, supplemental_count=supplemental_count[:-2], highest_order=2
        )
