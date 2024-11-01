"""Container class to hold catalog metadata and partition iteration"""

from __future__ import annotations

from typing import List

from hats.catalog.healpix_dataset.healpix_dataset import HealpixDataset
from hats.pixel_math import HealpixPixel
from hats.pixel_tree.negative_tree import compute_negative_tree_pixels


class Catalog(HealpixDataset):
    """A HATS Catalog with data stored in a HEALPix Hive partitioned structure

    Catalogs of this type are partitioned spatially, contain `partition_info` metadata specifying
    the pixels in Catalog, and on disk conform to the parquet partitioning structure
    `Norder=/Dir=/Npix=.parquet`
    """

    def generate_negative_tree_pixels(self) -> List[HealpixPixel]:
        """Get the leaf nodes at each healpix order that have zero catalog data.

        For example, if an example catalog only had data points in pixel 0 at
        order 0, then this method would return order 0's pixels 1 through 11.
        Used for getting full coverage on margin caches.

        Returns:
            List of HealpixPixels representing the 'negative tree' for the catalog.
        """
        return compute_negative_tree_pixels(self.pixel_tree)
