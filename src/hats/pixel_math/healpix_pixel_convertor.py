from __future__ import annotations

from hats.pixel_math.healpix_pixel import HealpixPixel


def get_healpix_pixel(pixel: HealpixPixel | tuple[int, int]) -> HealpixPixel:
    """Function to convert argument of either HealpixPixel or a tuple of (order, pixel) to a
    HealpixPixel

    Parameters
    ----------
    pixel : HealpixPixel | tuple[int, int]
        an object to be converted to a HealpixPixel object

    Returns
    -------
    HealpixPixel
        the pixel
    """
    if isinstance(pixel, tuple):
        if len(pixel) != 2:
            raise ValueError("Tuple must contain two values: HEALPix order and HEALPix pixel number")
        return HealpixPixel(order=pixel[0], pixel=pixel[1])
    if isinstance(pixel, HealpixPixel):
        return pixel
    raise TypeError("pixel must either be of type `HealpixPixel` or tuple (order, pixel)")


def get_healpix_tuple(pixel: HealpixPixel | tuple[int, int]) -> tuple[int, int]:
    """Function to convert argument of either HealpixPixel or a tuple of (order, pixel) to a
    tuple of (order, pixel)

    Parameters
    ----------
    pixel : HealpixPixel | tuple[int, int]
        an object to be converted to a HealpixPixel object

    Returns
    -------
    tuple[int, int]
        tuples representing order and pixel data
    """
    if isinstance(pixel, tuple):
        if len(pixel) != 2:
            raise ValueError("Tuple must contain two values: HEALPix order and HEALPix pixel number")
        return pixel
    if isinstance(pixel, HealpixPixel):
        return (pixel.order, pixel.pixel)
    raise TypeError("pixel must either be of type `HealpixPixel` or tuple (order, pixel)")
