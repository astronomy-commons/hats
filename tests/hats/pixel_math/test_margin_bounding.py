"""Tests of margin bounding utility functions"""

import numpy as np
import numpy.testing as npt
import pytest

import hats.pixel_math as pm


def test_check_margin_bounds():
    """Make sure check_margin_bounds works properly"""

    ras = np.array([42.4704538, 56.25, 56.25, 50.61225197])
    decs = np.array([1.4593925, 9.6, 10.0, 14.4767556])

    checks = pm.check_margin_bounds(ras, decs, 3, 4, 360.0)

    expected = np.array([False, True, False, True])

    npt.assert_array_equal(checks, expected)


def test_check_margin_bounds_low_order():
    """Make sure check_margin_bounds works when using a low order healpixel"""

    ras = np.array(
        [
            142.4704538,
            45.09,
            45.2,
            37.31343956517391,
            42.649354753311535,
            32.62796809350278,
            39.89468227954832,
            27.718121934039974,
        ]
    )

    decs = np.array(
        [
            1.4593925,
            0.0,
            0.0,
            6.566326903165274,
            2.005185097251452,
            10.597884275167646,
            4.465967883812584,
            14.959672304191956,
        ]
    )

    checks = pm.check_margin_bounds(ras, decs, 0, 4, 360.0)

    expected = np.array([False, True, False, True, True, True, True, True])

    npt.assert_array_equal(checks, expected)


def test_check_polar_margin_bounds_north():
    """Make sure check_polar_margin_bounds works at the north pole"""
    order = 0
    pix = 1

    ras = np.array([89, -179, -45])
    decs = np.array([89.9, 89.9, 85.0])

    vals = pm.check_margin_bounds(ras, decs, order, pix, 360.0)

    expected = np.array([True, True, False])

    npt.assert_array_equal(vals, expected)


def test_check_polar_margin_bounds_south():
    """Make sure check_polar_margin_bounds works at the south pole"""
    order = 0
    pix = 9

    ras = np.array([89, -179, -45])
    decs = np.array([-89.9, -89.9, -85.0])

    vals = pm.check_margin_bounds(ras, decs, order, pix, 360.0)

    expected = np.array([True, True, False])

    npt.assert_array_equal(vals, expected)


def test_check_margin_bounds_uneven():
    """Ensure check_margin_bounds fails when ra and dec arrays are unbalanced."""

    ras = np.array([42.4704538, 56.25, 56.25, 50.61225197])
    decs = np.array([1.4593925, 9.6, 10.0])

    with pytest.raises(ValueError, match="length of r_asc"):
        pm.check_margin_bounds(ras, decs, 3, 4, 360.0)


def test_check_margin_bounds_empty():
    """Ensure check_margin_bounds works when passed empty coordinate arrays."""

    ras = np.array([])
    decs = np.array([])

    checks = pm.check_margin_bounds(ras, decs, 3, 4, 360.0)

    expected = np.array([])

    npt.assert_array_equal(checks, expected)
