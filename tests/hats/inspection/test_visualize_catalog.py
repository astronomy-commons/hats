import numpy as np
import astropy.units as u
import matplotlib.pyplot as plt
import healpy as hp
import pytest
from astropy.coordinates import Angle, SkyCoord
from mocpy import WCS
from mocpy.moc.plot.fill import compute_healpix_vertices

from hats.catalog import Catalog
from hats.inspection import plot_pixel_list, plot_pixels, plot_points
from hats.inspection.visualize_catalog import plot_healpix_map
from hats.loaders import read_hats

# pylint: disable=no-member


def test_plot_healpix_pixels():
    order = 3
    length = 10
    ipix = np.arange(length)
    map = np.arange(length)
    depth = np.full(length, fill_value=order)
    fig, ax = plot_healpix_map(map, ipix=ipix, depth=depth)
    assert len(ax.collections) == 1
    col = ax.collections[0]
    paths = col.get_paths()
    assert len(paths) == length
    wcs = WCS(
        fig,
        fov=(320 * u.deg, 160 * u.deg),
        center=SkyCoord(0, 0, unit="deg", frame="icrs"),
        coordsys="icrs",
        rotation=Angle(0, u.degree),
        projection="MOL",
    ).w
    for path, ipix in zip(paths, np.arange(len(map))):
        verts, codes = compute_healpix_vertices(order, np.array([ipix]), wcs)
        np.testing.assert_array_equal(path.vertices, verts)
        np.testing.assert_array_equal(path.codes, codes)


def test_plot_healpix_pixels_different_order():
    order = 6
    length = 1000
    ipix = np.arange(length)
    map = np.arange(length)
    depth = np.full(length, fill_value=order)
    fig, ax = plot_healpix_map(map, ipix=ipix, depth=depth)
    assert len(ax.collections) == 1
    col = ax.collections[0]
    paths = col.get_paths()
    assert len(paths) == length
    wcs = WCS(
        fig,
        fov=(320 * u.deg, 160 * u.deg),
        center=SkyCoord(0, 0, unit="deg", frame="icrs"),
        coordsys="icrs",
        rotation=Angle(0, u.degree),
        projection="MOL",
    ).w
    for path, ipix in zip(paths, np.arange(len(map))):
        verts, codes = compute_healpix_vertices(order, np.array([ipix]), wcs)
        np.testing.assert_array_equal(path.vertices, verts)
        np.testing.assert_array_equal(path.codes, codes)


def test_order_0_pixels_split_to_order_3():
    map_value = 0.5
    order_0_pix = 4
    ipix = np.array([order_0_pix])
    map = np.array([map_value])
    depth = np.array([0])
    fig, ax = plot_healpix_map(map, ipix=ipix, depth=depth)
    assert len(ax.collections) == 1
    col = ax.collections[0]
    paths = col.get_paths()
    length = 4**3
    order3_ipix = np.arange(length * order_0_pix, length * (order_0_pix + 1))
    plt.show()
    assert len(paths) == length
    wcs = WCS(
        fig,
        fov=(320 * u.deg, 160 * u.deg),
        center=SkyCoord(0, 0, unit="deg", frame="icrs"),
        coordsys="icrs",
        rotation=Angle(0, u.degree),
        projection="MOL",
    ).w
    for path, ipix in zip(paths, order3_ipix):
        verts, codes = compute_healpix_vertices(3, np.array([ipix]), wcs)
        np.testing.assert_array_equal(path.vertices, verts)
        np.testing.assert_array_equal(path.codes, codes)


def test_plot_healpix_map():
    order = 3
    map = np.arange(12 * 4**order)
    fig, ax = plot_healpix_map(map)
    assert len(ax.collections) == 1
    col = ax.collections[0]
    paths = col.get_paths()
    wcs = WCS(
        fig,
        fov=(320 * u.deg, 160 * u.deg),
        center=SkyCoord(0, 0, unit="deg", frame="icrs"),
        coordsys="icrs",
        rotation=Angle(0, u.degree),
        projection="MOL",
    ).w
    plt.show()
    for path, ipix in zip(paths, np.arange(len(map))):
        verts, codes = compute_healpix_vertices(order, np.array([ipix]), wcs)
        np.testing.assert_array_equal(path.vertices, verts)
        np.testing.assert_array_equal(path.codes, codes)


def test_plot_pixel_map_order1(small_sky_order1_dir, mocker):
    """Basic test that map data can be generated"""
    # mocker.patch("healpy.mollview")

    cat = read_hats(small_sky_order1_dir)
    plot_pixels(cat, fov=(10 * u.deg, 10 * u.deg), center=(SkyCoord(300 * u.deg, -50 * u.deg)))
    plt.show()
    # hp.mollview.assert_called_once()
    #
    # hp.mollview.reset_mock()
    #
    # plot_points(cat)
    # hp.mollview.assert_called_once()


def test_visualize_in_memory_catalogs(catalog_info, catalog_pixels, mocker):
    """Test behavior of visualization methods for non-on-disk catalogs and pixel data."""
    mocker.patch("healpy.mollview")
    catalog = Catalog(catalog_info, catalog_pixels)
    plot_pixels(catalog)
    hp.mollview.assert_called_once()

    hp.mollview.reset_mock()
    plot_pixel_list(catalog_pixels, plot_title="My special catalog")
    hp.mollview.assert_called_once()

    hp.mollview.reset_mock()
    with pytest.raises(ValueError, match="on disk catalog required"):
        plot_points(catalog)
    hp.mollview.assert_not_called()
