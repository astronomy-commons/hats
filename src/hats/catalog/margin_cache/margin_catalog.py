from __future__ import annotations

from mocpy import MOC
from typing_extensions import Self

import hats.pixel_math.healpix_shim as hp
from hats.catalog.healpix_dataset.healpix_dataset import HealpixDataset
from hats.pixel_tree.moc_utils import copy_moc


class MarginCatalog(HealpixDataset):
    """A HATS Catalog used to contain the 'margin' of another HATS catalog.

    Catalogs of this type are used alongside a primary catalog, and contains the margin points for each
    HEALPix pixel - any points that are within a certain distance from the HEALPix pixel boundary. This is
    used to ensure spatial operations such as crossmatching can be performed efficiently while maintaining
    accuracy.
    """

    def filter_by_moc(self, moc: MOC) -> Self:
        """Filter the pixels in the margin catalog to only include the margin pixels that overlap with the moc

        For the case of margin pixels, this includes any pixels whose margin areas may overlap with the moc.
        This is not always done with a high accuracy, but always includes any pixels that will overlap,
        and may include extra partitions that do not.

        Args:
            moc (mocpy.MOC): the moc to filter by

        Returns:
            A new margin catalog with only the pixels that overlap or that have margin area that overlap with
            the moc. Note that we reset the total_rows to None, as updating would require a scan over the new
            pixel sizes.
        """
        max_order = moc.max_order
        max_order_size = hp.nside2resol(2**max_order, arcmin=True)
        if self.catalog_info.margin_threshold > max_order_size * 60:
            raise ValueError(
                f"Cannot Filter Margin: Margin size {self.catalog_info.margin_threshold} is "
                f"greater than the size of a pixel at the highest order {max_order}."
            )
        search_moc = copy_moc(moc).add_neighbours()
        return super().filter_by_moc(search_moc)
