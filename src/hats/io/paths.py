"""Methods for creating partitioned data paths"""

from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import urlencode

from fsspec.implementations.http import HTTPFileSystem
from upath import UPath

from hats.io.file_io.file_pointer import get_upath
from hats.pixel_math.healpix_pixel import INVALID_PIXEL, HealpixPixel

PARTITION_ORDER = "Norder"
PARTITION_DIR = "Dir"
PARTITION_PIXEL = "Npix"

MARGIN_ORDER = "margin_Norder"
MARGIN_DIR = "margin_Dir"
MARGIN_PIXEL = "margin_Npix"

HIVE_COLUMNS = [
    PARTITION_ORDER,
    PARTITION_DIR,
    PARTITION_PIXEL,
    MARGIN_ORDER,
    MARGIN_DIR,
    MARGIN_PIXEL,
]

DATASET_DIR = "dataset"
PARTITION_INFO_FILENAME = "partition_info.csv"
PARTITION_JOIN_INFO_FILENAME = "partition_join_info.csv"
PARQUET_METADATA_FILENAME = "_metadata"
PARQUET_COMMON_METADATA_FILENAME = "_common_metadata"
DATA_THUMBNAIL_FILENAME = "data_thumbnail.parquet"
POINT_MAP_FILENAME = "point_map.fits"
SKYMAP_FILENAME = "skymap.fits"


def pixel_directory(
    catalog_base_dir: str | Path | UPath | None,
    pixel_order: int,
    pixel_number: int | None = None,
    directory_number: int | None = None,
) -> UPath:
    """Create path pointer for a pixel directory. This will not create the directory.

    One of pixel_number or directory_number is required. The directory name will
    take the HiPS standard form of::

        <catalog_base_dir>/dataset/Norder=<pixel_order>/Dir=<directory number>

    Where the directory number is calculated using integer division as::

        (pixel_number/10000)*10000

    Args:
        catalog_base_dir (UPath): base directory of the catalog (includes catalog name)
        pixel_order (int): the healpix order of the pixel
        directory_number (int): directory number
        pixel_number (int): the healpix pixel
    Returns:
        UPath directory name
    """
    norder = int(pixel_order)
    if directory_number is not None:
        ndir = directory_number
    elif pixel_number is not None:
        npix = int(pixel_number)
        ndir = int(npix / 10_000) * 10_000
    else:
        raise ValueError("One of pixel_number or directory_number is required to create pixel directory")

    return (
        get_upath(catalog_base_dir) / DATASET_DIR / f"{PARTITION_ORDER}={norder}" / f"{PARTITION_DIR}={ndir}"
    )


def get_healpix_from_path(path: str) -> HealpixPixel:
    """Find the `pixel_order` and `pixel_number` from a string like the
    following::

        Norder=<pixel_order>/Dir=<directory number>/Npix=<pixel_number>.parquet

    NB: This expects the format generated by the `pixel_catalog_file` method

    Args:
        path (str): path to parse

    Returns:
        Constructed HealpixPixel object representing the pixel in the path.
        `INVALID_PIXEL` if the path doesn't match the expected pattern for any reason.
    """
    healpix_path_pattern = re.compile(r".*Norder=(\d*).*Npix=(\d*).*")
    match = healpix_path_pattern.match(path)
    if not match:
        return INVALID_PIXEL
    order, pixel = match.groups()
    return HealpixPixel(int(order), int(pixel))


def dict_to_query_urlparams(query_params: dict | None = None) -> str:
    """Converts a dictionary to a url query parameter string

    Args:
        query_params (dict): dictionary of query parameters.
    Returns:
        query parameter string to append to a url
    """

    if not query_params:
        return ""

    query = {}
    for key, value in query_params.items():
        if not all([key, value]):
            continue
        if isinstance(value, list):
            value = ",".join(value).replace(" ", "")
        query[key] = value

    if not query:
        return ""

    # Build the query string and add the "?" prefix
    url_params = "?" + urlencode(query, doseq=True)
    return url_params


def pixel_catalog_file(
    catalog_base_dir: str | Path | UPath | None,
    pixel: HealpixPixel,
    query_params: dict | None = None,
    npix_suffix: str = ".parquet",
) -> UPath:
    """Create path *pointer* for a pixel catalog file. This will not create the directory
    or file.

    The catalog file name will take the HiPS standard form of::

        <catalog_base_dir>/Norder=<pixel_order>/Dir=<directory number>/Npix=<pixel_number>.parquet

    Where the directory number is calculated using integer division as::

        (pixel_number/10000)*10000

    Args:
        catalog_base_dir (UPath): base directory of the catalog (includes catalog name)
        pixel (HealpixPixel): the healpix pixel to create path to
        query_params (dict): Params to append to URL. Ex: {'cols': ['ra', 'dec'], 'fltrs': ['r>=10', 'g<18']}
    Returns:
        string catalog file name
    """
    catalog_base_dir = get_upath(catalog_base_dir)
    suffix = npix_suffix if npix_suffix not in ["/", "\\"] else ""

    url_params = ""
    if isinstance(catalog_base_dir.fs, HTTPFileSystem) and query_params:
        url_params = dict_to_query_urlparams(query_params)

    return (
        catalog_base_dir
        / DATASET_DIR
        / f"{PARTITION_ORDER}={pixel.order}"
        / f"{PARTITION_DIR}={pixel.dir}"
        / f"{PARTITION_PIXEL}={pixel.pixel}{suffix}{url_params}"
    )


def get_partition_info_pointer(catalog_base_dir: str | Path | UPath) -> UPath:
    """Get file pointer to `partition_info.csv` metadata file

    Args:
        catalog_base_dir: pointer to base catalog directory
    Returns:
        File Pointer to the catalog's `partition_info.csv` file
    """
    return get_upath(catalog_base_dir) / PARTITION_INFO_FILENAME


def get_common_metadata_pointer(catalog_base_dir: str | Path | UPath) -> UPath:
    """Get file pointer to `_common_metadata` parquet metadata file

    Args:
        catalog_base_dir: pointer to base catalog directory
    Returns:
        File Pointer to the catalog's `_common_metadata` file
    """
    return get_upath(catalog_base_dir) / DATASET_DIR / PARQUET_COMMON_METADATA_FILENAME


def get_parquet_metadata_pointer(catalog_base_dir: str | Path | UPath) -> UPath:
    """Get file pointer to `_metadata` parquet metadata file

    Args:
        catalog_base_dir: pointer to base catalog directory
    Returns:
        File Pointer to the catalog's `_metadata` file
    """
    return get_upath(catalog_base_dir) / DATASET_DIR / PARQUET_METADATA_FILENAME


def get_data_thumbnail_pointer(catalog_base_dir: str | Path | UPath) -> UPath:
    """Get file pointer to `data_thumbnail` parquet file

    Args:
        catalog_base_dir: pointer to base catalog directory
    Returns:
        File Pointer to the catalog's `data_thumbnail` file
    """
    return get_upath(catalog_base_dir) / DATASET_DIR / DATA_THUMBNAIL_FILENAME


def get_point_map_file_pointer(catalog_base_dir: str | Path | UPath) -> UPath:
    """Get file pointer to `point_map.fits` FITS image file.

    Args:
        catalog_base_dir: pointer to base catalog directory
    Returns:
        File Pointer to the catalog's `point_map.fits` FITS image file.
    """
    return get_upath(catalog_base_dir) / POINT_MAP_FILENAME


def get_skymap_file_pointer(catalog_base_dir: str | Path | UPath, order: int | None = None) -> UPath:
    """Get file pointer to `skymap.fits` or `skymap.K.fits` FITS image file.

    Args:
        catalog_base_dir: pointer to base catalog directory
        order: healpix order of the desired down-sampled skymap
    Returns:
        File Pointer to the FITS image file.
    """
    if order is not None and order >= 0:
        return get_upath(catalog_base_dir) / f"skymap.{order}.fits"
    return get_upath(catalog_base_dir) / SKYMAP_FILENAME


def get_partition_join_info_pointer(catalog_base_dir: str | Path | UPath) -> UPath:
    """Get file pointer to `partition_join_info.csv` association metadata file

    Args:
        catalog_base_dir: pointer to base catalog directory
    Returns:
        File Pointer to the catalog's `partition_join_info.csv` association metadata file
    """
    return get_upath(catalog_base_dir) / PARTITION_JOIN_INFO_FILENAME
