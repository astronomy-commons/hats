"""Utilities for find the pixels of higher orders that surround a given healpixel."""

import numpy as np

import hats.pixel_math.healpix_shim as hp

# see the documentation for get_edge() about this variable
_edge_vectors = [
    np.asarray([1, 3]),  # NE edge
    np.asarray([1]),  # E corner
    np.asarray([0, 1]),  # SE edge
    np.asarray([0]),  # S corner
    np.asarray([0, 2]),  # SW edge
    np.asarray([2]),  # W corner
    np.asarray([2, 3]),  # NW edge
    np.asarray([3]),  # N corner
]

# cache used by get_margin()
_suffixes = {}


def get_edge(d_order, pix, edge):
    """Get all the pixels at order kk+dk bordering pixel pix.
    See hats/pixel_math/README.md for more info.

    Args:
        dk (int): the change in k that we wish to find the margins for.
        pix (int): the healpix pixel to find margin pixels of.
        edge (int): the edge we want to find for the given pixel. (0-7)

            - 0: NE edge
            - 1: E corner
            - 2: SE edge
            - 3: S corner
            - 4: SW edge
            - 5: W corner
            - 6: NW edge
            - 7: N corner
    Returns:
        one-dimensional numpy array of long integers, filled with the healpix pixels
        at order kk+dk that border the given edge of pix.
    """
    if edge < 0 or edge > 7:
        raise ValueError("edge can only be values between 0 and 7 (see docstring)")

    if (edge, d_order) in _suffixes:
        pixel_edge = _suffixes[(edge, d_order)]
    else:
        # generate and cache the suffix:

        # generate all combinations of i,j,k,... suffixes for the requested edge
        # See https://stackoverflow.com/a/35608701
        grid = np.array(np.meshgrid(*[_edge_vectors[edge]] * d_order))
        pixel_edge = grid.T.reshape(-1, d_order)
        # bit-shift each suffix by the required number of bits
        pixel_edge <<= np.arange(2 * (d_order - 1), -2, -2)
        # sum them up row-wise, generating the suffix
        pixel_edge = pixel_edge.sum(axis=1)
        # cache for further reuse
        _suffixes[(edge, d_order)] = pixel_edge

    # append the 'pix' preffix
    pixel_edge = (pix << 2 * d_order) + pixel_edge
    return pixel_edge


def get_margin(order, pix, d_order):
    """Get all the pixels at order order+dk bordering pixel pix.
    See hats/pixel_math/README.md for more info.

    Args:
        order (int): the healpix order of pix.
        pix (int): the healpix pixel to find margin pixels of.
        d_order (int): the change in k that we wish to find the margins for.
            Must be greater than kk.
    Returns:
        one-dimensional numpy array of long integers, filled with the healpix pixels
        at order kk+dk that border pix.
    """
    if d_order < 1:
        raise ValueError("dk must be greater than order")

    nside = hp.order2nside(order)

    # get all neighboring pixels
    neighbors = hp.get_all_neighbours(nside, pix, nest=True)

    # get the healpix faces IDs of pix and the neighboring pixels
    _, _, pix_face = hp.pix2xyf(nside, pix, nest=True)
    _, _, faces = hp.pix2xyf(nside, neighbors, nest=True)

    # indices which tell get_edge() which edge/vertex to return
    # for a given pixel. The default order is compatible with the
    # order returned by hp.get_all_neighbours().
    which = np.arange(8)

    # northern hemisphere; 90deg cw rotation for every +1 face increment
    if pix_face < 4:
        mask = faces < 4
        which[mask] += 2 * (faces - pix_face)[mask]
        which %= 8
    # southern hemisphere; 90deg ccw rotation for every +1 face increment
    elif pix_face >= 8:
        mask = faces >= 8
        which[mask] -= 2 * (faces - pix_face)[mask]
        which %= 8

    # get all edges/vertices
    # (making sure we skip -1 entries, for pixels with seven neighbors)
    margins = []
    for edge, pixel in zip(which, neighbors):
        if pixel != -1:
            margins.append(get_edge(d_order, pixel, edge))
    margins = np.concatenate(margins)
    return margins
