from __future__ import annotations

from collections.abc import Iterable

import astropy.units as u
import numpy as np
from astropy.coordinates import SkyCoord
from mocpy import MOC

from hats.pixel_math import HealpixPixel
from hats.pixel_math.validators import (
    validate_box,
    validate_declination_values,
    validate_polygon,
    validate_radius,
)


def wrap_ra_angles(ra: np.ndarray | Iterable | int | float) -> np.ndarray:
    """Wraps angles to the [0,360] degree range.

    Parameters
    ----------
    ra : ndarray | Iterable | int | float
        Right ascension values, in degrees

    Returns
    -------
    np.ndarray
        A numpy array of right ascension values, wrapped to the [0,360] degree range.
    """
    return np.asarray(ra, dtype=float) % 360


def pixel_list_to_moc(pixels: list[HealpixPixel]):
    """Create a MOC representation of the requested pixels.

    Parameters
    ----------
    pixels : list[HealpixPixels]
        the pixels to include

    Returns
    -------
    MOC
        a MOC object that covers the specified pixels
    """
    orders = np.array([p.order for p in pixels])
    pixel_inds = np.array([p.pixel for p in pixels])
    max_order = np.max(orders) if len(orders) > 0 else 0
    return MOC.from_healpix_cells(ipix=pixel_inds, depth=orders, max_depth=max_order)


def cone_to_moc(ra: float, dec: float, radius_arcsec: float, max_depth: int):
    """Create a MOC representation of a cone, at a particular pixel depth.

    Parameters
    ----------
    ra : float
        Right ascension of the center of the cone, in degrees
    dec : float
        Declination of the center of the cone, in degrees
    radius_arcsec : float
        Radius of the cone, in arcseconds
    max_depth : int
        highest HEALPix order to use in MOC

    Returns
    -------
    MOC
        a MOC object that covers the specified cone
    """
    validate_radius(radius_arcsec)
    validate_declination_values(dec)
    return MOC.from_cone(
        lon=ra * u.deg,
        lat=dec * u.deg,
        radius=radius_arcsec * u.arcsec,
        max_depth=max_depth,
    )


def box_to_moc(ra: tuple[float, float], dec: tuple[float, float], max_depth: int):
    """Create a MOC representation of a box, at a particular pixel depth.

    The right ascension edges follow great arc circles and the declination edges
    follow small arc circles.

    Parameters
    ----------
    ra : tuple[float, float]
        Right ascension range, in degrees
    dec : tuple[float, float]
        Declination range, in degrees
    max_depth : int
        highest HEALPix order to use in MOC

    Returns
    -------
    MOC
        a MOC object that covers the specified box
    """
    ra = tuple(wrap_ra_angles(ra)) if ra else None
    validate_box(ra, dec)

    bottom_left_corner = [ra[0], min(dec)]
    upper_right_corner = [ra[1], max(dec)]
    box_coords = SkyCoord([bottom_left_corner, upper_right_corner], unit="deg")
    return MOC.from_zone(box_coords, max_depth=max_depth)


def polygon_to_moc(vertices: list[tuple[float, float]], max_depth):
    """Create a MOC representation of a polygon, at a particular pixel depth.

    The right ascension edges follow great arc circles and the declination edges
    follow small arc circles.

    Parameters
    ----------
    vertices : list[tuple[float, float]]
        The list of vertex coordinates for the polygon, (ra, dec), in degrees.
    max_depth : int
        highest HEALPix order to use in MOC

    Returns
    -------
    MOC
        a MOC object that covers the specified polygon
    """
    validate_polygon(vertices)
    return MOC.from_polygon_skycoord(SkyCoord(vertices, unit="deg"), max_depth=max_depth)
