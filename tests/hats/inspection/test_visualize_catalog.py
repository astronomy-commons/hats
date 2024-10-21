import healpy as hp
import pytest

from hats.catalog import Catalog
from hats.inspection import plot_pixel_list, plot_pixels, plot_points
from hats.loaders import read_hats

# pylint: disable=no-member


@pytest.mark.timeout(10)
def test_generate_projections(small_sky_dir, mocker):
    """Basic test that map data can be generated"""

    cat = read_hats(small_sky_dir)

    projection = "moll"
    mocker.patch("healpy.mollview")
    plot_pixels(cat, projection=projection)
    hp.mollview.assert_called_once()
    hp.mollview.reset_mock()
    plot_points(cat, projection=projection)
    hp.mollview.assert_called_once()

    projection = "gnom"
    mocker.patch("healpy.gnomview")
    plot_pixels(cat, projection=projection)
    hp.gnomview.assert_called_once()
    hp.gnomview.reset_mock()
    plot_points(cat, projection=projection)
    hp.gnomview.assert_called_once()

    projection = "cart"
    mocker.patch("healpy.cartview")
    plot_pixels(cat, projection=projection)
    hp.cartview.assert_called_once()
    hp.cartview.reset_mock()
    plot_points(cat, projection=projection)
    hp.cartview.assert_called_once()

    projection = "orth"
    mocker.patch("healpy.orthview")
    plot_pixels(cat, projection=projection)
    hp.orthview.assert_called_once()
    hp.orthview.reset_mock()
    plot_points(cat, projection=projection)
    hp.orthview.assert_called_once()


def test_generate_map_unknown_projection(small_sky_dir):
    """Test for error with unknown projection type"""

    cat = read_hats(small_sky_dir)
    with pytest.raises(NotImplementedError):
        plot_pixels(cat, projection=None)

    with pytest.raises(NotImplementedError):
        plot_pixels(cat, projection="")

    with pytest.raises(NotImplementedError):
        plot_pixels(cat, projection="projection")


def test_plot_pixel_map_order1(small_sky_order1_dir, mocker):
    """Basic test that map data can be generated"""
    mocker.patch("healpy.mollview")

    cat = read_hats(small_sky_order1_dir)
    plot_pixels(cat)
    hp.mollview.assert_called_once()

    hp.mollview.reset_mock()

    plot_points(cat)
    hp.mollview.assert_called_once()


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
