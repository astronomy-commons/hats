from __future__ import annotations

from typing import Tuple

import numpy as np
from mocpy import MOC
from typing_extensions import TypeAlias

import hats.pixel_math.healpix_shim as hp

# Pair of spherical sky coordinates (ra, dec)
SphericalCoordinates: TypeAlias = Tuple[float, float]

# Sky coordinates on the unit sphere, in cartesian representation (x,y,z)
CartesianCoordinates: TypeAlias = Tuple[float, float, float]


def generate_polygon_moc(vertices: np.array, order: int) -> MOC:
    """Generates a moc filled with leaf nodes at a given order that overlap within
    a polygon. Vertices is an array of cartesian coordinates, in representation (x,y,z)
    and shape (Num vertices, 3), representing the vertices of the polygon."""
    polygon_pixels = hp.query_polygon(hp.order2nside(order), vertices, inclusive=True, nest=True)
    polygon_orders = np.full(len(polygon_pixels), fill_value=order)
    return MOC.from_healpix_cells(ipix=polygon_pixels, depth=polygon_orders, max_depth=order)
