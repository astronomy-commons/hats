import numpy as np
from numpy.testing import assert_allclose, assert_array_equal

from hats.pixel_math import healpix_shim as hps


def test_avgsize2mindist2avgsize():
    """Test that avgsize2mindist is inverse of mindist2avgsize"""
    avgsize = np.logspace(-5, 5, 21)
    assert_allclose(hps.mindist2avgsize(hps.avgsize2mindist(avgsize)), avgsize, rtol=1e-12)


def test_mindist2avgsize2mindist():
    """Test that mindist2avgsize is inverse of avgsize2mindist"""
    mindist = np.logspace(-3.2, 2.8, 21)
    assert_allclose(hps.avgsize2mindist(hps.mindist2avgsize(mindist)), mindist, rtol=1e-12)


def test_order2avgsize2order():
    """Test that avgsize2order is inverse of hps.nside2resol(hps.order2nside, arcmin=True)"""
    order = np.arange(20)
    nside = hps.order2nside(order)
    assert_array_equal(hps.avgsize2order(hps.nside2resol(nside, arcmin=True)), order)


def test_margin2order():
    """Test margin2order for some pre-computed values"""
    margin_thr_arcmin = np.array([1 / 60, 10 / 60, 1, 5, 60])
    orders = np.array([17, 13, 11, 8, 5])
    assert_array_equal(hps.margin2order(margin_thr_arcmin), orders)
