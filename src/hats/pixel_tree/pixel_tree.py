from __future__ import annotations

from collections.abc import Sequence
from typing import List

import numpy as np
from mocpy import MOC

from hats.pixel_math import HealpixInputTypes, HealpixPixel
from hats.pixel_math.healpix_pixel_convertor import get_healpix_tuple
from hats.pixel_math.healpix_pixel_function import get_pixels_from_intervals


class PixelTree:
    """Sparse Quadtree of HEALPix pixels that make up the HATS catalog

    This class stores each node in the tree, with leaf nodes corresponding to pixels with data
    files.

    There are a number of methods in this class which allow for quickly navigating through the
    tree and performing operations to filter the pixels in the catalog.

    Attributes:
        pixels: Nested dictionary of pixel nodes stored in the tree. Indexed by HEALPix
            order then pixel number
    """

    def __init__(self, tree: np.ndarray, order: int) -> None:
        """Initialises a tree object from the nodes in the tree

        Args:
            tree (np.ndarray): sorted array of intervals that represent each pixel in the tree
            order (int): HEALPix order of the pixel numbers in the intervals
        """
        self.tree_order = order
        self.tree = tree

        if not np.all((self.tree.T[0, 1:] - self.tree.T[1, :-1]) >= 0):
            raise ValueError("Invalid Catalog: Tree contains overlapping pixels")

        self.pixels = get_pixels_from_intervals(self.tree, self.tree_order)

    def __len__(self):
        """Gets the number of nodes in the tree

        Returns:
            The number of nodes in the tree
        """
        return len(self.tree)

    def contains(self, pixel: HealpixInputTypes) -> bool:
        """Check if tree contains a node at a given order and pixel

        Args:
            pixel: HEALPix pixel to check. Either of type `HealpixPixel`
                or a tuple of (order, pixel)

        Returns:
            True if the tree contains the pixel, False if not
        """
        (order, pixel) = get_healpix_tuple(pixel)
        if order > self.tree_order:
            return False
        d_order = self.tree_order - order
        pixel_at_tree_order = pixel << 2 * d_order
        index = np.searchsorted(self.tree.T[1], pixel_at_tree_order, side="right")
        if index >= len(self.pixels):
            return False
        is_same_order = self.pixels[index][0] == order
        return pixel_at_tree_order == self.tree[index][0] and is_same_order

    def __contains__(self, item):
        return self.contains(item)

    def get_max_depth(self) -> int:
        """Get the max depth (or highest healpix order) represented in the list of pixels.

        Returns:
            max depth (or highest healpix order) of the pixels in the tree
        """
        return np.max(self.pixels.T[0])

    def get_healpix_pixels(self) -> List[HealpixPixel]:
        """Creates a list of HealpixPixels in the tree

        Returns (List[HealpixPixel]):
            A list of the HEALPix pixels in the tree
        """
        return [HealpixPixel(p[0], p[1]) for p in self.pixels]

    def to_moc(self) -> MOC:
        """Returns the MOC object that covers the same pixels as the tree"""
        return MOC.from_healpix_cells(self.pixels.T[1], self.pixels.T[0], self.tree_order)

    @classmethod
    def from_healpix(cls, healpix_pixels: Sequence[HealpixInputTypes], tree_order=None) -> PixelTree:
        """Build a tree from a list of constituent healpix pixels

        Args:
            healpix_pixels: list of healpix pixels
            tree_order (int): (Default = None) order to generate the tree at. If None, will use the highest
                order from input pixels

        Returns:
            The pixel tree with the leaf pixels specified in the list
        """
        if len(healpix_pixels) == 0:
            return PixelTree(np.empty((0, 2), dtype=np.int64), 0)

        pixel_tuples = [get_healpix_tuple(p) for p in healpix_pixels]
        pixel_array = np.array(pixel_tuples).T
        orders = pixel_array[0]
        pixels = pixel_array[1]
        max_order = np.max(orders) if tree_order is None else tree_order
        starts = pixels * 4 ** (max_order - orders)
        ends = (pixels + 1) * 4 ** (max_order - orders)
        result = np.vstack((starts, ends)).T
        result.sort(axis=0)
        return cls(result, max_order)
