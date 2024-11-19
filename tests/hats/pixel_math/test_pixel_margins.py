"""Tests of pixel margin utility functions"""

import numpy as np
import numpy.testing as npt

import hats.pixel_math.pixel_margins as pm


def test_get_margin():
    """Check that the code works in the standard case."""
    margins = pm.get_margin(2, 2, 2)

    expected = np.array(
        [
            1141,
            1143,
            1149,
            1151,
            1237,
            128,
            129,
            132,
            133,
            144,
            48,
            50,
            56,
            58,
            26,
            10,
            11,
            14,
            15,
            1119,
        ]
    )
    expected.sort()

    assert len(margins) == 20

    npt.assert_array_equal(margins, expected)


def test_polar_edge():
    """Check that the code works when trying to find margins around the north pole."""
    margins = pm.get_margin(2, 5, 2)

    expected = np.array(
        [
            69,
            71,
            77,
            79,
            101,
            112,
            113,
            116,
            117,
            442,
            426,
            427,
            430,
            431,
            1530,
            1531,
            1534,
            1535,
            1519,
        ]
    )
    expected.sort()

    assert len(margins) == 19

    npt.assert_array_equal(margins, expected)


def test_polar_edge_south():
    """Check that the code works when trying to find margins around the south pole."""
    margins = pm.get_margin(1, 35, 2)

    expected = np.array(
        [
            549,
            551,
            557,
            559,
            261,
            272,
            273,
            276,
            277,
            0,
            352,
            354,
            360,
            362,
            330,
            538,
            539,
            542,
            543,
            527,
        ]
    )
    expected.sort()
    assert len(margins) == 20

    npt.assert_array_equal(margins, expected)
