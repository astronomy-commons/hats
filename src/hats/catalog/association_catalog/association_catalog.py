from __future__ import annotations

from typing import Union

import pandas as pd
import pyarrow as pa
from mocpy import MOC

from hats.catalog.association_catalog.partition_join_info import PartitionJoinInfo
from hats.catalog.dataset.table_properties import TableProperties
from hats.catalog.healpix_dataset.healpix_dataset import HealpixDataset, PixelInputTypes


class AssociationCatalog(HealpixDataset):
    """A HATS Catalog for enabling fast joins between two HATS catalogs

    Catalogs of this type are partitioned based on the partitioning of the left catalog.
    The `partition_join_info` metadata file specifies all pairs of pixels in the Association
    Catalog, corresponding to each pair of partitions in each catalog that contain rows to join.
    """

    JoinPixelInputTypes = Union[list, pd.DataFrame, PartitionJoinInfo]

    def __init__(
        self,
        catalog_info: TableProperties,
        pixels: PixelInputTypes,
        join_pixels: JoinPixelInputTypes,
        catalog_path=None,
        moc: MOC | None = None,
        schema: pa.Schema | None = None,
    ) -> None:
        super().__init__(catalog_info, pixels, catalog_path, moc=moc, schema=schema)
        self.join_info = self._get_partition_join_info_from_pixels(join_pixels)

    def get_join_pixels(self) -> pd.DataFrame:
        """Get join pixels listing all pairs of pixels from left and right catalogs that contain
        matching association rows

        Returns:
            pd.DataFrame with each row being a pair of pixels from the primary and join catalogs
        """
        return self.join_info.data_frame

    @staticmethod
    def _get_partition_join_info_from_pixels(
        join_pixels: JoinPixelInputTypes,
    ) -> PartitionJoinInfo:
        if isinstance(join_pixels, PartitionJoinInfo):
            return join_pixels
        if isinstance(join_pixels, pd.DataFrame):
            return PartitionJoinInfo(join_pixels)
        raise TypeError("join_pixels must be of type PartitionJoinInfo or DataFrame")
