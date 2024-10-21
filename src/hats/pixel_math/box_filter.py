from __future__ import annotations

from collections.abc import Iterable
from typing import List, Tuple

import numpy as np
from mocpy import MOC

import hats.pixel_math.healpix_shim as hp
from hats.pixel_math.polygon_filter import SphericalCoordinates


def generate_box_moc(ra: Tuple[float, float] | None, dec: Tuple[float, float] | None, order: int) -> MOC:
    """Generates a MOC object that covers the specified box area

    Args:
        ra (Tuple[float, float]): Right ascension range, in [0,360] degrees
        dec (Tuple[float, float]): Declination range, in [-90,90] degrees
        order (int): Maximum order of the moc to generate the box at

    Returns:
        a MOC object that covers the specified box
    """
    filter_moc = None
    ra_search_moc, dec_search_moc = None, None

    if ra is not None:
        ra_search_moc = _generate_ra_strip_moc(ra, order)
        filter_moc = ra_search_moc
    if dec is not None:
        dec_search_moc = _generate_dec_strip_moc(dec, order)
        filter_moc = dec_search_moc
    if ra_search_moc is not None and dec_search_moc is not None:
        filter_moc = ra_search_moc.intersection(dec_search_moc)
    return filter_moc


def wrap_ra_angles(ra: np.ndarray | Iterable | int | float) -> np.ndarray:
    """Wraps angles to the [0,360] degree range.

    Args:
        ra (ndarray | Iterable | int | float): Right ascension values, in degrees

    Returns:
        A numpy array of right ascension values, wrapped to the [0,360] degree range.
    """
    return np.asarray(ra, dtype=float) % 360


def _generate_ra_strip_moc(ra_range: Tuple[float, float], order: int) -> MOC:
    """Generates a pixel_tree filled with leaf nodes at a given order that overlap with the ra region."""
    # Subdivide polygons (if needed) in two smaller polygons of at most 180 degrees
    division_ra = _get_division_ra(ra_range)

    if division_ra is not None:
        pixels_in_range = _get_pixels_for_subpolygons(
            [
                # North Pole
                [(0, 90), (ra_range[0], 0), (division_ra, 0)],
                [(0, 90), (division_ra, 0), (ra_range[1], 0)],
                # South Pole
                [(ra_range[0], 0), (0, -90), (division_ra, 0)],
                [(division_ra, 0), (0, -90), (ra_range[1], 0)],
            ],
            order,
        )
    else:
        # We assume the polygon of smallest area by default
        # e.g. for ra_ranges of [10,50], [200,10] or [350,10]
        pixels_in_range = _get_pixels_for_subpolygons(
            [
                # North Pole
                [(0, 90), (ra_range[0], 0), (ra_range[1], 0)],
                # South Pole
                [(ra_range[0], 0), (0, -90), (ra_range[1], 0)],
            ],
            order,
        )

    orders = np.full(pixels_in_range.shape, fill_value=order)
    return MOC.from_healpix_cells(ipix=pixels_in_range, depth=orders, max_depth=order)


def _generate_dec_strip_moc(dec_range: Tuple[float, float], order: int) -> MOC:
    """Generates a pixel_tree filled with leaf nodes at a given order that overlap with the dec region."""
    nside = hp.order2nside(order)
    # Convert declination values to colatitudes, in radians, and revert their order
    colat_rad = np.sort(np.radians([90 - dec if dec > 0 else 90 + abs(dec) for dec in dec_range]))
    # Figure out which pixels in nested order are contained in the declination band
    pixels_in_range = hp.ring2nest(
        nside, hp.query_strip(nside, theta1=colat_rad[0], theta2=colat_rad[1], inclusive=True)
    )
    orders = np.full(pixels_in_range.shape, fill_value=order)
    return MOC.from_healpix_cells(ipix=pixels_in_range, depth=orders, max_depth=order)


def _get_division_ra(ra_range: Tuple[float, float]) -> float | None:
    """Gets the division angle for the right ascension region. This is crucial for the edge
    cases where we need to split up polygons wider than 180 degrees into smaller polygons."""
    division_ra = None
    if ra_range[0] > ra_range[1] and 360 - ra_range[0] + ra_range[1] >= 180:
        # e.g. edge case of [350, 170] or [180, 0], we want the wider polygon
        division_ra = (ra_range[1] - 360 + ra_range[0]) / 2
    elif ra_range[0] < ra_range[1] and ra_range[1] - ra_range[0] >= 180:
        # e.g. edge case of [10, 200] or [0, 180], we want the wider polygon
        division_ra = ra_range[0] + (ra_range[1] - ra_range[0]) / 2
    return division_ra


def _get_pixels_for_subpolygons(polygons: List[List[SphericalCoordinates]], order: int) -> np.ndarray:
    """Gets the unique pixels for a set of sub-polygons."""
    nside = hp.order2nside(order)
    all_polygon_pixels = []
    for vertices in polygons:
        vertices = hp.ang2vec(*np.array(vertices).T, lonlat=True)
        pixels = hp.query_polygon(nside, vertices, inclusive=True, nest=True)
        all_polygon_pixels.append(pixels)
    return np.unique(np.concatenate(all_polygon_pixels, 0))
