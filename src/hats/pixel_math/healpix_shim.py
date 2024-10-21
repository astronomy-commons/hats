import healpy as hp
import numpy as np

# pylint: disable=missing-function-docstring

## Arithmetic conversions


def nside2order(param):
    return hp.nside2order(param)


def order2nside(param):
    return hp.order2nside(param)


def npix2nside(param):
    return hp.npix2nside(param)


def nside2npix(param):
    return hp.nside2npix(param)


def order2npix(param):
    return hp.order2npix(param)


def npix2order(param):
    return hp.npix2order(param)


def nside2resol(*args, **kwargs):
    return hp.nside2resol(*args, **kwargs)


def nside2pixarea(*args, **kwargs):
    return hp.nside2pixarea(*args, **kwargs)


def ang2pix(*args, **kwargs):
    return hp.ang2pix(*args, **kwargs)


def ring2nest(*args, **kwargs):
    return hp.ring2nest(*args, **kwargs)


def unseen_pixel():
    return hp.pixelfunc.UNSEEN


## Query


def query_strip(*args, **kwargs):
    return hp.query_strip(*args, **kwargs)


def query_polygon(*args, **kwargs):
    return hp.query_polygon(*args, **kwargs)


def boundaries(*args, **kwargs):
    return hp.boundaries(*args, **kwargs)


def get_all_neighbours(*args, **kwargs):
    return hp.get_all_neighbours(*args, **kwargs)


## Coordinate conversion


def ang2vec(*args, **kwargs):
    return hp.ang2vec(*args, **kwargs)


def pix2ang(*args, **kwargs):
    return hp.pix2ang(*args, **kwargs)


def vec2dir(*args, **kwargs):
    return hp.vec2dir(*args, **kwargs)


def pix2xyf(*args, **kwargs):
    return hp.pix2xyf(*args, **kwargs)


## FITS
def read_map(*args, **kwargs):
    return hp.read_map(*args, **kwargs)


def write_map(*args, **kwargs):
    return hp.write_map(*args, **kwargs)


## Custom functions


def avgsize2mindist(avg_size: np.ndarray) -> np.ndarray:
    """Get the minimum distance between pixels for a given average size

    Average size is a "resolution" in healpy terms, i.e. given by
    `healpy.nside2resol`.

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

    Average size is a "resolution" in healpy terms, i.e. given by
    `healpy.nside2resol`.

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

    "Average" size is healpy's "resolution", so this function is
    reverse to healpy.nside2resol(healpy.order2nside(order))..

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
