from __future__ import annotations

from dataclasses import dataclass
from typing import List

import numpy as np

from hats.pixel_math.spatial_index import SPATIAL_INDEX_ORDER


@dataclass(eq=True, frozen=True, order=True)
class HealpixPixel:
    """A HEALPix pixel, represented by an order and pixel number in NESTED ordering scheme

    see https://lambda.gsfc.nasa.gov/toolbox/pixelcoords.html for more information
    """

    order: int
    pixel: int

    def __post_init__(self) -> None:
        """Initialize a HEALPix pixel
        Args:
            order: HEALPix order
            pixel: HEALPix pixel number in NESTED ordering scheme
        """
        if self.order > SPATIAL_INDEX_ORDER:
            raise ValueError(f"HEALPix order cannot be greater than {SPATIAL_INDEX_ORDER}")

    def __str__(self) -> str:
        return f"Order: {self.order}, Pixel: {self.pixel}"

    def __repr__(self):
        return self.__str__()

    def __getitem__(self, key: int) -> int:
        if key == 0:
            return self.order
        if key == 1:
            return self.pixel
        raise IndexError("Invalid healpix index")

    def convert_to_lower_order(self, delta_order: int) -> HealpixPixel:
        """Returns the HEALPix pixel that contains the pixel at a lower order

        Args:
            delta_order: the difference in order to be subtracted from the current order to generate
                the pixel at a lower order. Must be non-negative

        Returns:
            A new `HealpixPixel` at the current order - `delta_order` which contains the current
            pixel

        Raises:
            ValueError: If delta_order is greater than the current order, a pixel cannot be
                generated at a negative order. Or if delta_order is negative

        """
        new_pixel = get_lower_order_pixel(self.order, self.pixel, delta_order)
        new_order = self.order - delta_order
        return HealpixPixel(new_order, new_pixel)

    def convert_to_higher_order(self, delta_order: int) -> List[HealpixPixel]:
        """Returns a list of HEALPix pixels making up the current pixel at a higher order

        Args:
            delta_order: the difference in order to be added to the current order to generate the
                pixels at a higher order. Must be non-negative

        Returns:
            A new `HealpixPixel` at the current order - `delta_order` which contains the current
            pixel

        Raises:
            ValueError: If delta_order + current order is greater than the maximum HEALPix order,
                pixels cannot be  generated. Or if delta_order is negative
        """
        new_pixels = get_higher_order_pixels(self.order, self.pixel, delta_order)
        new_order = self.order + delta_order
        pixels = [HealpixPixel(new_order, new_pixel) for new_pixel in new_pixels]
        return pixels

    @property
    def dir(self) -> int:
        """Directory number for the pixel.

        This is necessary for file systems that limit to 10,000 subdirectories.
        The directory name will take the HiPS standard form of::

            <catalog_base_dir>/Norder=<pixel_order>/Dir=<directory number>

        Where the directory number is calculated using integer division as::

            (pixel_number/10000)*10000
        """
        return int(self.pixel / 10_000) * 10_000


INVALID_PIXEL = HealpixPixel(-1, -1)


def get_lower_order_pixel(order: int, pixel: int, delta_order: int) -> int:
    """Returns the pixel number at a lower order

    Args:
        order (int): the order of the pixel
        pixel (int): the pixel number of the pixel in NESTED ordering
        delta_order (int): the change in order to the new lower order

    Returns:
        The pixel number at order (order - delta_order) for the pixel that contains the given pixel
    """
    if order - delta_order < 0:
        raise ValueError("Pixel Order cannot be below zero")
    if delta_order < 0:
        raise ValueError("delta order cannot be below zero")
    new_pixel = pixel >> (2 * delta_order)
    return new_pixel


def get_higher_order_pixels(order: int, pixel: int, delta_order: int) -> np.ndarray:
    """Returns the pixel numbers at a higher order

    Args:
        order (int): the order of the pixel
        pixel (int): the pixel number of the pixel in NESTED ordering
        delta_order (int): the change in order to the new higher order

    Returns:
        The list of pixel numbers at order (order + delta_order) for the pixels contained by the given pixel
    """
    if order == -1:
        return np.arange(0, 12)
    if order + delta_order > SPATIAL_INDEX_ORDER:
        raise ValueError(f"Pixel Order cannot be above maximum order {SPATIAL_INDEX_ORDER}")
    if delta_order < 0:
        raise ValueError("delta order cannot be below zero")
    new_pixels = np.arange(pixel << (2 * delta_order), (pixel + 1) << (2 * delta_order))
    return new_pixels
