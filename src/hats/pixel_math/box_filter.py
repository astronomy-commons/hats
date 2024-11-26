from __future__ import annotations

from collections.abc import Iterable
from typing import Tuple

import numpy as np
from astropy.coordinates import SkyCoord
from mocpy import MOC


def generate_box_moc(ra: Tuple[float, float], dec: Tuple[float, float], order: int) -> MOC:
    """Generates a MOC object that covers the specified box. A box is delimited
    by right ascension and declination ranges. The right ascension edges follow
    great arc circles and the declination edges follow small arc circles.

    Args:
        ra (Tuple[float, float]): Right ascension range, in [0,360] degrees
        dec (Tuple[float, float]): Declination range, in [-90,90] degrees
        order (int): Maximum order of the moc to generate the box at

    Returns:
        a MOC object that covers the specified box
    """
    bottom_left_corner = [ra[0], min(dec)]
    upper_right_corner = [ra[1], max(dec)]
    box_coords = SkyCoord([bottom_left_corner, upper_right_corner], unit="deg")
    return MOC.from_zone(box_coords, max_depth=order)


def wrap_ra_angles(ra: np.ndarray | Iterable | int | float) -> np.ndarray:
    """Wraps angles to the [0,360] degree range.

    Args:
        ra (ndarray | Iterable | int | float): Right ascension values, in degrees

    Returns:
        A numpy array of right ascension values, wrapped to the [0,360] degree range.
    """
    return np.asarray(ra, dtype=float) % 360
