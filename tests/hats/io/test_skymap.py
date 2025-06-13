import numpy as np
import pytest

import hats.pixel_math.healpix_shim as hp
from hats import read_hats
from hats.io.file_io import read_fits_image
from hats.io.skymap import read_skymap, write_skymap


def test_write_skymap_roundtrip(tmp_path):
    """Test the reading/writing of a catalog point map"""
    dense = np.arange(0, 3072)

    # Writes a single, non-sampled, skymap
    write_skymap(dense, tmp_path)
    skymap_path = tmp_path / "skymap.fits"
    assert skymap_path.exists()
    skymap_order1_path = tmp_path / "skymap.1.fits"
    assert not skymap_order1_path.exists()
    counts_skymap = read_fits_image(tmp_path / "skymap.fits")
    np.testing.assert_array_equal(counts_skymap, dense)


@pytest.mark.parametrize("orders", [[], [2], [1, 2], [2, 1], [1], [0], [0, 1, 2, 3, 4]])
def test_write_sampled_skymaps_roundtrip(tmp_path, orders):
    """Test the reading/writing of a catalog point map"""
    dense = np.arange(0, 3072)
    assert sum(dense) == 4_717_056

    write_skymap(dense, tmp_path, orders)
    skymap_path = tmp_path / "skymap.fits"
    assert skymap_path.exists()
    counts_skymap = read_fits_image(tmp_path / "skymap.fits")
    np.testing.assert_array_equal(counts_skymap, dense)

    for order in range(0, 5):
        skymap_atorder_path = tmp_path / f"skymap.{order}.fits"
        if order in orders:
            assert skymap_atorder_path.exists()
            read_histogram = read_fits_image(skymap_atorder_path)
            assert hp.npix2order(len(read_histogram)) == order
            # Check that the contents have same overall count, and we're summing quadratically.
            assert sum(read_histogram) == 4_717_056
            pixel_size = 4 ** (4 - order)
            assert read_histogram[-1] == sum(dense[-1 * pixel_size :])
        else:
            assert not skymap_atorder_path.exists()


def test_write_sampled_skymaps_error(tmp_path):
    """Test the reading/writing of a catalog point map"""
    dense = np.arange(0, 3072)

    with pytest.raises(ValueError, match="order should be less"):
        write_skymap(dense, tmp_path, 13)


def test_read_alt_skymap(small_sky_source_dir, mocker):
    """Test that we're reading the file we expect to read, and get the
    appropriate length for the returned skymap histogram."""
    catalog = read_hats(small_sky_source_dir)

    mock_method = "hats.io.file_io.read_fits_image"
    # Setting the side effect allows us to run the mocked function's code
    mocked_read_fits_call = mocker.patch(mock_method, side_effect=read_fits_image)

    ## Requesting no order should use the default skymap (order 6)
    skymap = read_skymap(catalog, None)
    assert len(skymap) == 49152

    mocked_read_fits_call.assert_called_once()
    path_arg = mocked_read_fits_call.call_args.args[0]
    assert str(small_sky_source_dir / "skymap.fits") == str(path_arg)

    mocked_read_fits_call.reset_mock()

    ## Requesting available alt value should use that skymap
    skymap = read_skymap(catalog, 4)
    assert len(skymap) == 3_072

    mocked_read_fits_call.assert_called_once()
    path_arg = mocked_read_fits_call.call_args.args[0]
    assert str(small_sky_source_dir / "skymap.4.fits") == str(path_arg)

    mocked_read_fits_call.reset_mock()

    ## Requesting unavailable alt value should use next-highest alt skymap
    skymap = read_skymap(catalog, 3)
    assert len(skymap) == 768

    mocked_read_fits_call.assert_called_once()
    path_arg = mocked_read_fits_call.call_args.args[0]
    assert str(small_sky_source_dir / "skymap.4.fits") == str(path_arg)

    mocked_read_fits_call.reset_mock()

    ## Requesting too-high alt-value should use the default skymap
    skymap = read_skymap(catalog, 5)
    assert len(skymap) == 12_288

    mocked_read_fits_call.assert_called_once()
    path_arg = mocked_read_fits_call.call_args.args[0]
    assert str(small_sky_source_dir / "skymap.fits") == str(path_arg)

    mocked_read_fits_call.reset_mock()


def test_read_noalt_skymap(small_sky_source_dir, mocker):
    """Test that we're reading the file we expect to read, and get the
    appropriate length for the returned skymap histogram."""
    catalog = read_hats(small_sky_source_dir)
    ## Alternate sky map orders are only used when present in the properties file.
    catalog.catalog_info.skymap_alt_orders = None

    mock_method = "hats.io.file_io.read_fits_image"
    # Setting the side effect allows us to run the mocked function's code
    mocked_read_fits_call = mocker.patch(mock_method, side_effect=read_fits_image)

    ## Requesting no order should use the default skymap (order 6)
    skymap = read_skymap(catalog, None)
    assert len(skymap) == 49_152

    mocked_read_fits_call.assert_called_once()
    path_arg = mocked_read_fits_call.call_args.args[0]
    assert str(small_sky_source_dir / "skymap.fits") == str(path_arg)

    mocked_read_fits_call.reset_mock()

    ## Requesting (otherwise-)available alt value will still use default
    skymap = read_skymap(catalog, 4)
    assert len(skymap) == 3_072

    mocked_read_fits_call.assert_called_once()
    path_arg = mocked_read_fits_call.call_args.args[0]
    assert str(small_sky_source_dir / "skymap.fits") == str(path_arg)

    mocked_read_fits_call.reset_mock()

    ## Requesting waaay too-high order should error.
    with pytest.raises(ValueError, match="order should be less"):
        skymap = read_skymap(catalog, 13)
