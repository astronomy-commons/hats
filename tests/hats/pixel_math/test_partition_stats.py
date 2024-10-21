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
