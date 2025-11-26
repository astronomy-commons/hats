"""Tests of histogram calculations"""

import numpy as np
import numpy.testing as npt
import pandas as pd
import pytest

import hats.pixel_math as hist
import hats.pixel_math.healpix_shim as hp


def test_small_sky_same_pixel():
    """Test partitioning two objects into the same large bucket"""

    data = pd.DataFrame(
        data=[[700, 282.5, -58.5], [701, 299.5, -48.5]],
        columns=["id", "ra", "dec"],
    )

    result = hist.generate_histogram(
        data=data,
        highest_order=0,
    )

    assert len(result) == 12

    expected = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2]
    npt.assert_array_equal(result, expected)
    assert (result == expected).all()


def test_column_names_error():
    """Test with non-default column names (without specifying column names)"""

    data = pd.DataFrame(
        data=[[700, 282.5, -58.5], [701, 299.5, -48.5]],
        columns=["id", "ra_mean", "dec_mean"],
    )

    with pytest.raises(ValueError, match="Invalid column names"):
        hist.generate_histogram(
            data=data,
            highest_order=0,
        )


def test_column_names():
    """Test with non-default column names"""
    data = pd.DataFrame(
        data=[[700, 282.5, -58.5], [701, 299.5, -48.5]],
        columns=["id", "ra_mean", "dec_mean"],
    )

    result = hist.generate_histogram(
        data=data,
        highest_order=0,
        ra_column="ra_mean",
        dec_column="dec_mean",
    )

    assert len(result) == 12

    expected = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2]
    npt.assert_array_equal(result, expected)
    assert (result == expected).all()


def test_alignment_wrong_size():
    """Check that the method raises error when the input histogram is not the expected size."""
    initial_histogram = np.asarray([0, 0, 0, 0, 0, 0, 0, 0, 0, 131])
    with pytest.raises(ValueError, match="histogram is not the right size"):
        hist.generate_alignment(initial_histogram, highest_order=0, threshold=250)


def test_alignment_exceeds_threshold_order0():
    """Check that the method raises error when some pixel exceeds the threshold."""
    initial_histogram = np.asarray([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 131])
    with pytest.raises(ValueError, match="exceeds threshold"):
        hist.generate_alignment(initial_histogram, highest_order=0, threshold=20)


def test_alignment_lowest_order_too_large():
    """Check that the method raises error when some pixel exceeds the threshold."""
    initial_histogram = hist.empty_histogram(1)
    with pytest.raises(ValueError, match="lowest_order"):
        hist.generate_alignment(initial_histogram, highest_order=1, lowest_order=2, threshold=20)


def test_alignment_exceeds_threshold_order2():
    """Check that the method raises error when some pixel exceeds the threshold."""
    initial_histogram = hist.empty_histogram(2)
    filled_pixels = [4, 11, 14, 13, 5, 7, 8, 9, 11, 23, 4, 4, 17, 0, 1, 0]
    initial_histogram[176:] = filled_pixels[:]
    with pytest.raises(ValueError, match="exceeds threshold"):
        hist.generate_alignment(initial_histogram, highest_order=2, threshold=20)


@pytest.mark.parametrize("drop_empty_siblings", [True, False])
def test_alignment_small_sky_order0(drop_empty_siblings):
    """Create alignment from small sky's distribution at order 0"""
    initial_histogram = np.asarray([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 131])
    result = hist.generate_alignment(
        initial_histogram, highest_order=0, threshold=250, drop_empty_siblings=drop_empty_siblings
    )

    expected = np.full(12, None)
    expected[11] = (0, 11, 131)

    npt.assert_array_equal(result, expected)


@pytest.mark.parametrize("drop_empty_siblings", [True, False])
def test_alignment_small_sky_order1(drop_empty_siblings):
    """Create alignment from small sky's distribution at order 1"""
    initial_histogram = hist.empty_histogram(1)
    filled_pixels = [42, 29, 42, 18]
    initial_histogram[44:] = filled_pixels[:]
    result = hist.generate_alignment(
        initial_histogram, highest_order=1, threshold=250, drop_empty_siblings=drop_empty_siblings
    )

    expected = np.full(48, None)
    expected[44:] = [(0, 11, 131), (0, 11, 131), (0, 11, 131), (0, 11, 131)]

    npt.assert_array_equal(result, expected)


def test_alignment_small_sky_order1_empty_siblings():
    """Create alignment from small sky's distribution at order 1"""
    initial_histogram = hist.empty_histogram(1)
    initial_histogram[44] = 100
    result = hist.generate_alignment(
        initial_histogram, highest_order=1, threshold=250, drop_empty_siblings=True
    )

    expected = np.full(48, None)
    expected[44] = (1, 44, 100)

    npt.assert_array_equal(result, expected)


@pytest.mark.parametrize("drop_empty_siblings", [True, False])
def test_alignment_small_sky_order2(drop_empty_siblings):
    """Create alignment from small sky's distribution at order 2"""
    initial_histogram = hist.empty_histogram(2)
    filled_pixels = [4, 11, 14, 13, 5, 7, 8, 9, 11, 23, 4, 4, 17, 0, 1, 0]
    initial_histogram[176:] = filled_pixels[:]
    result = hist.generate_alignment(
        initial_histogram, highest_order=2, threshold=250, drop_empty_siblings=drop_empty_siblings
    )

    expected = np.full(hp.order2npix(2), None)
    tuples = [
        (0, 11, 131),
        (0, 11, 131),
        (0, 11, 131),
        (0, 11, 131),
        (0, 11, 131),
        (0, 11, 131),
        (0, 11, 131),
        (0, 11, 131),
        (0, 11, 131),
        (0, 11, 131),
        (0, 11, 131),
        (0, 11, 131),
        (0, 11, 131),
        (0, 11, 131),
        (0, 11, 131),
        (0, 11, 131),
    ]
    expected[176:192] = tuples

    npt.assert_array_equal(result, expected)


@pytest.mark.parametrize("drop_empty_siblings", [True, False])
@pytest.mark.timeout(20)
def test_alignment_even_sky(drop_empty_siblings):
    """Create alignment from an even distribution at order 7"""
    initial_histogram = np.full(hp.order2npix(7), 40)
    result = hist.generate_alignment(
        initial_histogram, highest_order=7, threshold=1_000, drop_empty_siblings=drop_empty_siblings
    )
    # everything maps to order 5, given the density
    for mapping in result:
        assert mapping[0] == 5

    result = hist.generate_alignment(
        initial_histogram,
        highest_order=7,
        lowest_order=7,
        threshold=1_000,
        drop_empty_siblings=drop_empty_siblings,
    )
    # everything maps to order 7 (would be 5, but lowest of 7 is enforced)
    for mapping in result:
        assert mapping[0] == 7


def test_incremental_alignment():
    """Create alignment for existing catalog, considering new incoming data"""
    existing_pixels = [(1, 44)]

    increment_histogram = hist.empty_histogram(2)
    # Increment counts for the children pix of (1,44):
    increment_histogram[176:180] = [42, 30, 21, 12]
    # Counts for some points out of (1,44):
    increment_histogram[180:182] = [5, 8]

    result = hist.generate_incremental_alignment(
        increment_histogram, existing_pixels=existing_pixels, highest_order=2, lowest_order=0
    )

    expected = np.full(hp.order2npix(2), None)
    # We expect the existing pixel (1, 44) to have the new counts
    expected[176:180] = [(1, 44, 42 + 30 + 21 + 12)] * 4
    # The data that falls out of (1,44) will be assigned a pixel at order 1.
    # since, even though lowest_order=0, pixel (0,11) would overlap with (1,44).
    expected[180:184] = [(1, 45, 5 + 8)] * 4
    npt.assert_array_equal(result, expected)


def test_incremental_alignment_higher_order():
    """Create alignment for existing catalog, considering new incoming data"""
    highest_order = 7
    existing_pixels = [(highest_order, pix) for pix in range(0, 49152)]

    increment_histogram = hist.empty_histogram(highest_order)
    # Increment counts for bits on the end that are currently empty
    ## 0, 1, 2, 3: will combine into an order 6, with sum 45
    ## 4, 5, 6: still empty
    ## 7: single order 7 pixel
    increment_histogram[-8:] = [12, 11, 10, 12, 0, 0, 0, 47]

    result = hist.generate_incremental_alignment(
        increment_histogram,
        existing_pixels=existing_pixels,
        highest_order=highest_order,
        lowest_order=0,
        threshold=50,
    )

    expected = np.full(hp.order2npix(highest_order), None)
    expected[-8:-4] = [(6, 49150, 45)] * 4
    expected[-1] = (7, 196607, 47)

    ## existing pixels unchanged, 5 new pixels added:
    assert np.count_nonzero(result) == 5
    npt.assert_array_equal(result, expected)


def test_incremental_alignment_highest_order_invalid():
    with pytest.raises(ValueError, match="existing catalog maximum order"):
        # existing catalog max_order=1 while highest_order=0
        hist.generate_incremental_alignment(
            hist.empty_histogram(0), existing_pixels=[(1, 45)], highest_order=0
        )


def test_generate_alignment_mem_size():
    """Create alignment based on memory size histogram"""
    initial_row_count_histogram = hist.empty_histogram(2)
    filled_pixels = [11_000, 11, 14, 13, 5, 7, 8, 9, 11, 23, 4, 4, 17, 0, 1, 0]
    initial_row_count_histogram[176:] = filled_pixels[:]

    initial_mem_size_histogram = hist.empty_histogram(2)
    filled_mem_sizes = [
        4_000,
        1_000,
        1_000,
        1_000,
        5_000,
        7_000,
        8_000,
        9_000,
        1_000,
        2_000,
        4_000,
        4_000,
        7_000,
        0,
        1_000,
        0,
    ]
    initial_mem_size_histogram[176:] = filled_mem_sizes[:]

    # Generate alignment based on memory size thresholding.
    result = hist.generate_alignment(
        initial_row_count_histogram,
        highest_order=2,
        threshold=10_000,
        mem_size_histogram=initial_mem_size_histogram,
    )

    expected = np.full(hp.order2npix(2), None)
    tuples = [
        (1, 44, 11038),
        (1, 44, 11038),
        (1, 44, 11038),
        (1, 44, 11038),
        (2, 180, 5),
        (2, 181, 7),
        (2, 182, 8),
        (2, 183, 9),
        (2, 184, 11),
        (2, 185, 23),
        (2, 186, 4),
        (2, 187, 4),
        (1, 47, 18),
        (1, 47, 18),
        (1, 47, 18),
        (1, 47, 18),
    ]
    expected[176:192] = tuples

    npt.assert_array_equal(result, expected)


def test_generate_alignment_mem_size_dropping_siblings():
    """Create alignment based on memory size histogram"""
    initial_row_count_histogram = hist.empty_histogram(2)
    filled_pixels = [0, 0, 14, 0, 5, 7, 8, 9, 11, 23, 4, 4, 17, 0, 1, 0]
    initial_row_count_histogram[176:] = filled_pixels[:]

    initial_mem_size_histogram = hist.empty_histogram(2)
    filled_mem_sizes = [
        0,
        0,
        4_000,
        0,
        5_000,
        7_000,
        8_000,
        9_000,
        1_000,
        2_000,
        4_000,
        4_000,
        7_000,
        0,
        1_000,
        0,
    ]
    initial_mem_size_histogram[176:] = filled_mem_sizes[:]

    # Generate alignment based on memory size thresholding.
    result = hist.generate_alignment(
        initial_row_count_histogram,
        highest_order=2,
        threshold=10_000,
        drop_empty_siblings=True,
        mem_size_histogram=initial_mem_size_histogram,
    )

    expected = np.full(hp.order2npix(2), None)
    tuples = [
        None,
        None,
        (2, 178, 14),
        None,
        (2, 180, 5),
        (2, 181, 7),
        (2, 182, 8),
        (2, 183, 9),
        (2, 184, 11),
        (2, 185, 23),
        (2, 186, 4),
        (2, 187, 4),
        (1, 47, 18),
        (1, 47, 18),
        (1, 47, 18),
        (1, 47, 18),
    ]
    expected[176:192] = tuples

    npt.assert_array_equal(result, expected)


def test_generate_alignment_mem_size_exceeds_threshold():
    """Create alignment based on memory size histogram"""
    initial_row_count_histogram = hist.empty_histogram(2)
    filled_pixels = [4, 11, 14, 13, 5, 7, 8, 9, 11, 23, 4, 4, 17, 0, 1, 0]
    initial_row_count_histogram[176:] = filled_pixels[:]

    initial_mem_size_histogram = hist.empty_histogram(2)
    filled_mem_sizes = [
        40_000,
        1_000,
        1_000,
        1_000,
        5_000,
        7_000,
        8_000,
        9_000,
        1_000,
        2_000,
        4_000,
        4_000,
        7_000,
        0,
        1_000,
        0,
    ]
    initial_mem_size_histogram[176:] = filled_mem_sizes[:]

    # We raise an error if any pixel exceeds the threshold.
    with pytest.raises(ValueError, match="exceeds threshold"):
        hist.generate_alignment(
            initial_row_count_histogram,
            highest_order=2,
            threshold=5_000,
            mem_size_histogram=initial_mem_size_histogram,
        )
