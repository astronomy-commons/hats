"""Test pixel path creation"""

import os

import pytest

from hats.io import paths
from hats.pixel_math.healpix_pixel import INVALID_PIXEL, HealpixPixel


def test_pixel_directory():
    """Simple case with sensical inputs"""
    expected = os.path.join(os.sep, "foo", "dataset", "Norder=0", "Dir=0")
    result = paths.pixel_directory("/foo", 0, 5)
    assert str(result) == str(expected)


def test_pixel_directory_number():
    """Simple case with sensical inputs"""
    expected = os.path.join(os.sep, "foo", "dataset", "Norder=0", "Dir=0")
    result = paths.pixel_directory("/foo", pixel_order=0, pixel_number=5, directory_number=0)
    assert str(result) == expected

    result = paths.pixel_directory("/foo", pixel_order=0, directory_number=0)
    assert str(result) == expected


def test_pixel_directory_missing():
    """Simple case with missing inputs"""
    with pytest.raises(ValueError):
        paths.pixel_directory("/foo", 0)


def test_pixel_directory_nonint():
    """Simple case with non-integer inputs"""
    with pytest.raises(ValueError):
        paths.pixel_directory("/foo", "zero", "five")


def test_pixel_catalog_file():
    """Simple case with sensical inputs"""
    expected = os.path.join(os.sep, "foo", "dataset", "Norder=0", "Dir=0", "Npix=5.parquet")
    result = paths.pixel_catalog_file("/foo", HealpixPixel(0, 5))
    assert str(result) == expected


def test_pixel_catalog_file_w_query_params():
    expected = (
        "https://foo/dataset/Norder=0/Dir=0/Npix=5.parquet?columns=ID%2CRA%2CDEC%2Cr_auto&filters=r_auto%3C13"
    )
    query_params = {"columns": ["ID", "RA", "DEC", "r_auto"], "filters": ["r_auto<13"]}
    result = paths.pixel_catalog_file("https://foo", HealpixPixel(0, 5), query_params=query_params)
    assert expected == str(result)


def test_pixel_catalog_file_nonint():
    """Simple case with non-integer inputs"""
    with pytest.raises(AttributeError):
        paths.pixel_catalog_file("/foo", "zero", "five")


def test_new_pixel_catalog_file():
    """Simple cases with sensical inputs"""
    expected = os.path.join(os.sep, "foo", "dataset", "Norder=0", "Dir=0", "Npix=5.parquet")
    result = paths.new_pixel_catalog_file("/foo", HealpixPixel(0, 5), create_dirs=False)
    assert str(result) == expected

    expected = os.path.join(os.sep, "foo", "dataset", "Norder=0", "Dir=0", "Npix=5.parq")
    result = paths.new_pixel_catalog_file("/foo", HealpixPixel(0, 5), npix_suffix=".parq", create_dirs=False)
    assert str(result) == expected

    expected = os.path.join(os.sep, "foo", "dataset", "Norder=0", "Dir=0", "Npix=5", "Npix=5.parquet")
    result = paths.new_pixel_catalog_file("/foo", HealpixPixel(0, 5), npix_suffix="/", create_dirs=False)
    assert str(result) == expected

    expected = os.path.join(os.sep, "foo", "dataset", "Norder=0", "Dir=0", "Npix=5", "concat.parquet")
    result = paths.new_pixel_catalog_file(
        "/foo", HealpixPixel(0, 5), npix_suffix="/", npix_parquet_name="concat.parquet", create_dirs=False
    )
    assert str(result) == expected

    # npix_parquet_name is ignored, if npix_suffix != '/'
    expected = os.path.join(os.sep, "foo", "dataset", "Norder=0", "Dir=0", "Npix=5.parquet")
    result = paths.new_pixel_catalog_file(
        "/foo", HealpixPixel(0, 5), npix_parquet_name="concat.parquet", create_dirs=False
    )
    assert str(result) == expected


def test_new_pixel_catalog_file_create_dirs(tmp_path):
    """Make sure that we're creating parent directories, and pixel directories."""
    start_path = tmp_path / "parquet"
    result = paths.new_pixel_catalog_file(start_path, HealpixPixel(0, 5), create_dirs=True)

    expected_file = tmp_path / "parquet" / "dataset" / "Norder=0" / "Dir=0" / "Npix=5.parquet"
    expected_dir = tmp_path / "parquet" / "dataset" / "Norder=0" / "Dir=0"

    assert expected_dir.exists()
    assert not expected_file.exists()

    assert str(result) == str(expected_file)

    start_path = tmp_path / "dir"
    result = paths.new_pixel_catalog_file(
        start_path, HealpixPixel(0, 5), npix_suffix="/", npix_parquet_name="concat.parquet", create_dirs=True
    )

    expected_file = tmp_path / "dir" / "dataset" / "Norder=0" / "Dir=0" / "Npix=5" / "concat.parquet"
    expected_dir = tmp_path / "dir" / "dataset" / "Norder=0" / "Dir=0" / "Npix=5"

    assert expected_dir.exists()
    assert not expected_file.exists()

    assert str(result) == str(expected_file)


def test_dict_to_query_urlparams():
    expected = "?columns=ID%2CRA%2CDEC%2Cr_auto&filters=r_auto%3C13"
    query_params = {"columns": ["ID", "RA", "DEC", "r_auto"], "filters": ["r_auto<13"]}
    result = paths.dict_to_query_urlparams(query_params)
    assert result == expected

    expected = "?columns=ID%2CRA%2CDEC%2Cr_auto&filters=r_auto%3C13"
    query_params = {"columns": [" ID", "RA ", "DEC ", "r_auto"], "filters": ["r_auto < 13"]}
    result = paths.dict_to_query_urlparams(query_params)
    assert result == expected

    result = paths.dict_to_query_urlparams({})
    assert result == ""

    result = paths.dict_to_query_urlparams(None)
    assert result == ""

    result = paths.dict_to_query_urlparams({"": ""})
    assert result == ""

    result = paths.dict_to_query_urlparams({None: ""})
    assert result == ""

    result = paths.dict_to_query_urlparams({"": "nonempty"})
    assert result == ""


def test_get_healpix_from_path():
    expected = HealpixPixel(5, 34)

    # Test a few ways we could represent the path.
    result = paths.get_healpix_from_path("/foo/dataset/Norder=5/Dir=0/Npix=34.parquet")
    assert result == expected

    result = paths.get_healpix_from_path("C:\\foo\\dataset\\Norder=5\\Dir=0\\Npix=34.parquet")
    assert result == expected

    result = paths.get_healpix_from_path("Norder=5/Dir=0/Npix=34.pq")
    assert result == expected


def test_get_healpix_from_path_invalid():
    expected = INVALID_PIXEL

    # Test a few ways we could represent the path.
    result = paths.get_healpix_from_path("")
    assert result == expected

    result = paths.get_healpix_from_path("NORDER=5/Dir=0/PIXEL=34.pq")
    assert result == expected
