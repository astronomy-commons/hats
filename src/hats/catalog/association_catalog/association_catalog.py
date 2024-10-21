from __future__ import annotations

from pathlib import Path
from typing import Tuple, Union

import pandas as pd
import pyarrow as pa
from mocpy import MOC
from upath import UPath

from hats.catalog.association_catalog.partition_join_info import PartitionJoinInfo
from hats.catalog.dataset.table_properties import TableProperties
from hats.catalog.healpix_dataset.healpix_dataset import HealpixDataset, PixelInputTypes
from hats.io import file_io, paths


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

    @classmethod
    def _read_args(
        cls, catalog_base_dir: str | Path | UPath
    ) -> Tuple[TableProperties, PixelInputTypes, JoinPixelInputTypes]:  # type: ignore[override]
        args = super()._read_args(catalog_base_dir)
        partition_join_info = PartitionJoinInfo.read_from_dir(catalog_base_dir)
        return args + (partition_join_info,)

    @classmethod
    def _check_files_exist(cls, catalog_base_dir: str | Path | UPath):
        super()._check_files_exist(catalog_base_dir)
        partition_join_info_file = paths.get_partition_join_info_pointer(catalog_base_dir)
        metadata_file = paths.get_parquet_metadata_pointer(catalog_base_dir)
        if not (
            file_io.does_file_or_directory_exist(partition_join_info_file)
            or file_io.does_file_or_directory_exist(metadata_file)
        ):
            raise FileNotFoundError(
                f"_metadata or partition join info file is required in catalog directory {catalog_base_dir}"
            )
