from __future__ import annotations

import math

import astropy.units as u
import cdshealpix
import numpy as np
from astropy.coordinates import Latitude, Longitude, SkyCoord

# pylint: disable=missing-function-docstring

## Arithmetic conversions


MAX_HEALPIX_ORDER = 29


def is_order_valid(order: int) -> bool:
    return np.all(0 <= order) and np.all(order <= MAX_HEALPIX_ORDER)


def npix2order(npix: int) -> int:
    if npix <= 0:
        raise ValueError("Invalid value for npix")
    order = int(math.log2(npix / 12)) >> 1
    if not is_order_valid(order) or not 12 * (1 << (2 * order)) == npix:
        raise ValueError("Invalid value for npix")
    return order


def order2nside(order: int) -> int:
    if not is_order_valid(order):
        raise ValueError("Invalid value for order")
    return 1 << order


def order2npix(order: int) -> int:
    if not is_order_valid(order):
        raise ValueError("Invalid value for order")
    return 12 * (1 << (2 * order))


def order2resol(order: int, arcmin: bool = False) -> float:
    resol_rad = np.sqrt(order2pixarea(order))

    if arcmin:
        return np.rad2deg(resol_rad) * 60

    return resol_rad


def order2pixarea(order: int, degrees: bool = False) -> float:
    npix = order2npix(order)
    pix_area_rad = 4 * np.pi / npix
    if degrees:
        return pix_area_rad * (180 / np.pi) * (180 / np.pi)
    return pix_area_rad


def radec2pix(order: int, ra: float, dec: float) -> np.ndarray[np.int64]:
    if not is_order_valid(order):
        raise ValueError("Invalid value for order")

    ra = Longitude(ra, unit="deg")
    dec = Latitude(dec, unit="deg")

    return cdshealpix.lonlat_to_healpix(ra, dec, order).astype(np.int64)


## Coordinate conversion


def ang2vec(ra, dec, **kwargs) -> np.ndarray:
    """Converts ra and dec to cartesian coordinates on the unit sphere"""
    coords = SkyCoord(ra=ra * u.deg, dec=dec * u.deg, **kwargs).cartesian
    return np.array([coords.x.value, coords.y.value, coords.z.value]).T


## Custom functions


def avgsize2mindist(avg_size: np.ndarray) -> np.ndarray:
    """Get the minimum distance between pixels for a given average size

    We don't have the precise geometry of the healpix grid yet,
    so we are using average_size / mininimum_distance = 1.6
    as a rough estimate.

    Parameters
    ----------
    avg_size : np.ndarray of float
        The average size of a healpix pixel

    Returns
    -------
    np.ndarray of float
        The minimum distance between pixels for the given average size
    """
    return avg_size / 1.6


def mindist2avgsize(mindist: np.ndarray) -> np.ndarray:
    """Get the average size for a given minimum distance between pixels

    We don't have the precise geometry of the healpix grid yet,
    so we are using average_size / mininimum_distance = 1.6
    as a rough estimate.

    Parameters
    ----------
    mindist : np.ndarray of float
        The minimum distance between pixels

    Returns
    -------
    np.ndarray of float
        The average size of a healpix pixel for the given minimum distance
        between pixels.
    """
    return mindist * 1.6


def avgsize2order(avg_size_arcmin: np.ndarray) -> np.ndarray:
    """Get the largest order with average healpix size larger than avg_size_arcmin

    Parameters
    ----------
    avg_size_arcmin : np.ndarray of float
        The average size of a healpix pixel in arcminutes

    Returns
    -------
    np.ndarray of int
        The largest healpix order for which the average size is larger than avg_size_arcmin
    """
    # resolution = sqrt(4pi / (12 * 4**order)) => order = log2(sqrt(4pi / 12) / resolution)
    order_float = np.log2(np.sqrt(np.pi / 3) / np.radians(avg_size_arcmin / 60.0))
    return np.array(order_float, dtype=int)


def margin2order(margin_thr_arcmin: np.ndarray) -> np.ndarray:
    """Get the largest order for which distance between pixels is less than margin_thr_arcmin

    We don't have the precise geometry of the healpix grid yet,
    we are using average_size / mininimum_distance = 1.6
    as a rough estimate.

    Parameters
    ----------
    margin_thr_arcmin : np.ndarray of float
        The minimum distance between pixels in arcminutes

    Returns
    -------
    np.ndarray of int
        The largest healpix order for which the distance between pixels is less than margin_thr_arcmin
    """
    avg_size_arcmin = mindist2avgsize(margin_thr_arcmin)
    return avgsize2order(avg_size_arcmin)


def order2mindist(order: np.ndarray | int) -> np.ndarray | float:
    """Get the estimated minimum distance between pixels at a given order.

    We don't have the precise geometry of the healpix grid yet,
    we are using average_size / mininimum_distance = 1.6
    as a rough estimate.

    Parameters
    ----------
    order : np.ndarray of int or a single scalar int
        The healpix order

    Returns
    -------
    np.ndarray of float or a single scalar float
        The minimum distance between pixels in arcminutes
    """
    pixel_avgsize = order2resol(order, arcmin=True)
    return avgsize2mindist(pixel_avgsize)
