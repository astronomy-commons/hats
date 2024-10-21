from __future__ import annotations

from typing import Tuple, Union

from hats.pixel_math.healpix_pixel import HealpixPixel

HealpixInputTypes = Union[HealpixPixel, Tuple[int, int]]


def get_healpix_pixel(pixel: HealpixInputTypes) -> HealpixPixel:
    """Function to convert argument of either HealpixPixel or a tuple of (order, pixel) to a
    HealpixPixel

    Args:
        pixel: an object to be converted to a HealpixPixel object
    """

    if isinstance(pixel, tuple):
        if len(pixel) != 2:
            raise ValueError("Tuple must contain two values: HEALPix order and HEALPix pixel number")
        return HealpixPixel(order=pixel[0], pixel=pixel[1])
    if isinstance(pixel, HealpixPixel):
        return pixel
    raise TypeError("pixel must either be of type `HealpixPixel` or tuple (order, pixel)")


def get_healpix_tuple(pixel: HealpixInputTypes) -> Tuple[int, int]:
    """Function to convert argument of either HealpixPixel or a tuple of (order, pixel) to a
    tuple of (order, pixel)

    Args:
        pixel: an object to be converted to a HealpixPixel object
    """

    if isinstance(pixel, tuple):
        if len(pixel) != 2:
            raise ValueError("Tuple must contain two values: HEALPix order and HEALPix pixel number")
        return pixel
    if isinstance(pixel, HealpixPixel):
        return (pixel.order, pixel.pixel)
    raise TypeError("pixel must either be of type `HealpixPixel` or tuple (order, pixel)")
