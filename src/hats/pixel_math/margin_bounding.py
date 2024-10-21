"""Utilities to build bounding boxes around healpixels that include a neighor margin."""

import numpy as np
from astropy.coordinates import SkyCoord
from numba import njit

import hats.pixel_math.healpix_shim as hp


def check_margin_bounds(r_asc, dec, pixel_order, pixel, margin_threshold, step=100, chunk_size=1000):
    # pylint: disable=too-many-locals
    """Check a set of points in ra and dec space to see if they fall within the
        `margin_threshold` of a given healpixel.

    Args:
        r_asc (:obj:`np.array`): one dimmensional array representing the ra of a
            given set of points.
        dec (:obj:`np.array`): one dimmensional array representing the dec of a
            given set of points.
        pixel_order (int): the order of the healpixel to be checked against.
        pixel (int): the healpixel to be checked against.
        margin_threshold (double): the size of the margin cache in arcseconds.
        step (int): the amount of samples we'll get from the healpixel boundary
            to test the datapoints against. the higher the step the more
            granular the point checking and therefore the more accurate, however
            using a smaller step value might be helpful for super large datasets
            to save compute time if accuracy is less important.
        chunk_size (int): the size of the chunk of data points we process on a single
            thread at any given time. We only process a certain amount of the data
            at once as trying to check all data points at once can lead to
            memory issues.
    Returns:
        :obj:`numpy.array` of boolean values corresponding to the ra and dec
        coordinates checked against whether a given point is within any of the
        provided polygons.
    """
    num_points = len(r_asc)
    if num_points != len(dec):
        raise ValueError(
            f"length of r_asc ({num_points}) is different \
                from length of dec ({len(dec)})"
        )

    if num_points == 0:
        return np.array([])

    bounds = hp.vec2dir(hp.boundaries(2**pixel_order, pixel, step=step, nest=True), lonlat=True)
    margin_check = np.full((num_points,), False)

    distances = _get_boundary_point_distances(pixel_order, pixel, step)
    margin_threshold_deg = margin_threshold / 3600.0

    num_bounds = step * 4
    ra_chunks = np.array_split(r_asc, chunk_size)
    dec_chunks = np.array_split(dec, chunk_size)
    index = 0
    for chunk_index, ra_chunk in enumerate(ra_chunks):
        dec_chunk = dec_chunks[chunk_index]

        chunk_len = len(ra_chunk)

        if chunk_len > 0:
            ra_matrix = np.repeat(np.reshape([ra_chunk], (-1, 1)), num_bounds, axis=1)
            dec_matrix = np.repeat(np.reshape([dec_chunk], (-1, 1)), num_bounds, axis=1)

            bounds_ra_matrix = np.repeat(np.array([bounds[0]]), len(ra_chunk), axis=0)
            bounds_dec_matrix = np.repeat(np.array([bounds[1]]), len(dec_chunk), axis=0)

            points_coords = SkyCoord(ra=ra_matrix, dec=dec_matrix, unit="deg")
            bounds_coords = SkyCoord(ra=bounds_ra_matrix, dec=bounds_dec_matrix, unit="deg")

            separations = points_coords.separation(bounds_coords).deg

            bisectors = np.apply_along_axis(
                _find_minimum_distance,
                1,
                separations,
                distances=distances,
                margin_threshold=margin_threshold_deg,
            )
            margin_check[index : index + chunk_len] = bisectors <= margin_threshold_deg
            index += chunk_len

            del ra_matrix, dec_matrix, bounds_ra_matrix, bounds_dec_matrix
            del points_coords, bounds_coords, separations

    del distances

    return margin_check


# numba jit compiler doesn't count for coverage tests, so we'll set no cover.
@njit("double(double[:], double[:], double)")
def _find_minimum_distance(separations, distances, margin_threshold):  # pragma: no cover
    """Find the minimum distance between a given datapoint and a healpixel"""
    minimum_index = np.argmin(separations)

    if separations[minimum_index] <= margin_threshold:
        return separations[minimum_index]

    plus_one, minus_one = minimum_index + 1, minimum_index - 1
    num_seps = len(separations)
    if minimum_index == 0:
        minus_one = num_seps - 1
    if minimum_index == num_seps - 1:
        plus_one = 0

    if separations[minus_one] <= separations[plus_one]:
        other_index = minus_one
    else:
        other_index = plus_one

    side_x = np.radians(separations[minimum_index])
    side_y = np.radians(separations[other_index])
    if (0 in (minimum_index, other_index)) and (num_seps - 1 in (minimum_index, other_index)):
        side_z = distances[-1]
    else:
        side_z = distances[min(minimum_index, other_index)]
    side_z *= np.pi / 180.0

    ang = np.arccos((np.cos(side_y) - (np.cos(side_x) * np.cos(side_z))) / (np.sin(side_x) * np.sin(side_z)))
    bisector = np.degrees(np.arcsin(np.sin(ang) * np.sin(side_x)))

    return bisector


def _get_boundary_point_distances(order, pixel, step):
    """Get the distance of the segments between healpixel boundary points"""
    boundary_points = hp.vec2dir(hp.boundaries(2**order, pixel, step=step, nest=True), lonlat=True)

    # shift forward all the coordinates by one
    shift_ra = np.roll(boundary_points[0], 1)
    shift_dec = np.roll(boundary_points[1], 1)

    coord_set_1 = SkyCoord(ra=boundary_points[0], dec=boundary_points[1], unit="deg")
    coord_set_2 = SkyCoord(ra=shift_ra, dec=shift_dec, unit="deg")

    separations = coord_set_1.separation(coord_set_2).degree
    return separations
