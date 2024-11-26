import cdshealpix
import numpy as np
import pytest
from astropy.coordinates import Latitude, Longitude
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
    assert_array_equal(hps.avgsize2order(hps.order2resol(order, arcmin=True)), order)


def test_margin2order():
    """Test margin2order for some pre-computed values"""
    margin_thr_arcmin = np.array([1 / 60, 10 / 60, 1, 5, 60])
    orders = np.array([17, 13, 11, 8, 5])
    assert_array_equal(hps.margin2order(margin_thr_arcmin), orders)


def test_order2mindist():
    """Test order2mindist for some pre-computed values"""
    orders = np.array([17, 13, 11, 8, 5])
    min_distances = np.array([0.01677, 0.268, 1.07, 8.588, 68.7])
    assert_allclose(hps.order2mindist(orders), min_distances, rtol=1e-2)

    assert_allclose(hps.order2mindist(17), 0.01677, rtol=1e-2)


def test_ang2vec():
    """Tests conversion of ra/dec to unit cartesian vectors"""
    ra = [230.14467816, 110.40507118, 9.41764689, 245.5553117]
    dec = [38.78080888, 17.09584081, -28.6352765, 5.41803306]
    ra_rad = np.radians(ra)
    dec_rad = np.radians(dec)
    x = np.cos(ra_rad) * np.cos(dec_rad)
    y = np.sin(ra_rad) * np.cos(dec_rad)
    z = np.sin(dec_rad)
    actual = np.asarray([x, y, z]).T
    assert_array_equal(actual, hps.ang2vec(ra, dec))


def test_npix2order():
    orders = [0, 1, 5, 10, 20, 29]
    npix = [12 * (4**order) for order in orders]
    test_orders = [hps.npix2order(x) for x in npix]
    assert test_orders == orders


def test_npix2order_invalid():
    npixs = [-10, 0, 11, 13, 47, 49, 100000, 100000000000000000]
    for npix in npixs:
        with pytest.raises(ValueError, match="Invalid"):
            hps.npix2order(npix)


def test_order2nside():
    orders = [0, 1, 5, 10, 20, 29]
    expected_nsides = [2**x for x in orders]
    test_nsides = [hps.order2nside(o) for o in orders]
    assert test_nsides == expected_nsides


def test_order2nside_invalid():
    orders = [-1000, -1, 30, 4000]
    for order in orders:
        with pytest.raises(ValueError, match="Invalid"):
            hps.order2nside(order)


def test_order2npix():
    orders = [0, 1, 5, 10, 20, 29]
    npix = [12 * (4**order) for order in orders]
    test_npix = [hps.order2npix(o) for o in orders]
    assert test_npix == npix


def test_order2npix_invalid():
    orders = [-1000, -1, 30, 4000]
    for order in orders:
        with pytest.raises(ValueError, match="Invalid"):
            hps.order2npix(order)


def test_order2pixarea():
    orders = [0, 1, 5, 10, 20, 29]
    npix = [12 * (4**order) for order in orders]
    pix_area_expected = [4 * np.pi / x for x in npix]
    pix_area_test = [hps.order2pixarea(order) for order in orders]
    assert pix_area_test == pix_area_expected


def test_order2pixarea_degrees():
    orders = [0, 1, 5, 10, 20, 29]
    npix = [12 * (4**order) for order in orders]
    pix_area_expected = [np.rad2deg(np.rad2deg(4 * np.pi / x)) for x in npix]
    pix_area_test = [hps.order2pixarea(order, degrees=True) for order in orders]
    assert pix_area_test == pix_area_expected


def test_order2resol():
    orders = [0, 1, 5, 10, 20, 29]
    resol_expected = [np.sqrt(hps.order2pixarea(order)) for order in orders]
    resol_test = [hps.order2resol(order) for order in orders]
    assert resol_test == resol_expected


def test_order2resol_arcmin():
    orders = [0, 1, 5, 10, 20, 29]
    resol_expected = [np.rad2deg(np.sqrt(hps.order2pixarea(order))) * 60 for order in orders]
    resol_test = [hps.order2resol(order, arcmin=True) for order in orders]
    assert resol_test == resol_expected


def test_radec2pix_lonlat():
    orders = [0, 1, 5, 10, 20, 29]
    ras = np.arange(-180.0, 180.0, 10.0)
    decs = np.arange(-90.0, 90.0, 180 // len(ras))
    for order in orders:
        expected_pixels = cdshealpix.lonlat_to_healpix(
            Longitude(ras, unit="deg"), Latitude(decs, unit="deg"), order
        )
        pixels = hps.radec2pix(order, ras, decs)
        assert np.all(pixels == expected_pixels)


def test_radec2pix_invalid():
    orders = [0, 1, 5, 10, 20, 29]
    invalid_orders = [-1000, -1, 30, 40]
    ras = np.arange(-4000.0, 1000.0, 100.0)
    decs = np.arange(-1000.0, 1000.0, 2000.0 // len(ras))
    for order in invalid_orders:
        with pytest.raises(ValueError, match="Invalid"):
            hps.radec2pix(order, ras, decs)
    for order in orders:
        with pytest.raises(ValueError, match="angle"):
            hps.radec2pix(order, ras, decs)
