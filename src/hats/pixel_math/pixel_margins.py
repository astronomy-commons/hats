"""Utilities for find the pixels of higher orders that surround a given healpixel."""

import cdshealpix


def get_margin(order, pixel, delta_order):
    """Get all the pixels at order order+delta_order bordering pixel pixel.

    Args:
        order (int): the healpix order of pixel.
        pix (int): the healpix pixel to find margin pixels of.
        delta_order (int): the change in order that we wish to find the margins for.
    Returns:
        one-dimensional numpy array of integers, filled with the healpix pixels
        at order `order+delta_order` that border pixel.
    """
    edges, corners = cdshealpix.external_neighbours(ipix=pixel, depth=order, delta_depth=delta_order)

    margins = []
    margins.extend(edges[0])
    margins.extend(corners[0])
    margins.sort()
    margins = [int(pix) for pix in margins if pix >= 0]
    return margins
