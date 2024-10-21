"""Container class to hold per-partition metadata"""

from __future__ import annotations

import warnings
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd
import pyarrow as pa
from upath import UPath

import hats.pixel_math.healpix_shim as hp
from hats.io import file_io, paths
from hats.io.parquet_metadata import (
    read_row_group_fragments,
    row_group_stat_single_value,
    write_parquet_metadata_for_batches,
)
from hats.pixel_math import HealpixPixel


class PartitionInfo:
    """Container class for per-partition info."""

    METADATA_ORDER_COLUMN_NAME = "Norder"
    METADATA_PIXEL_COLUMN_NAME = "Npix"

    def __init__(self, pixel_list: List[HealpixPixel], catalog_base_dir: str = None) -> None:
        self.pixel_list = pixel_list
        self.catalog_base_dir = catalog_base_dir

    def get_healpix_pixels(self) -> List[HealpixPixel]:
        """Get healpix pixel objects for all pixels represented as partitions.

        Returns:
            List of HealpixPixel
        """
        return self.pixel_list

    def get_highest_order(self) -> int:
        """Get the highest healpix order for the dataset.

        Returns:
            int representing highest order.
        """
        max_pixel = np.max(self.pixel_list)
        return max_pixel.order

    def write_to_file(
        self,
        partition_info_file: str | Path | UPath | None = None,
        catalog_path: str | Path | UPath | None = None,
    ):
        """Write all partition data to CSV file.

        If no paths are provided, the catalog base directory from the `read_from_dir` call is used.

        Args:
            partition_info_file: path to where the `partition_info.csv`
                file will be written.
            catalog_path: base directory for a catalog where the `partition_info.csv`
                file will be written.

        Raises:
            ValueError: if no path is provided, and could not be inferred.
        """
        if partition_info_file is None:
            if catalog_path is not None:
                partition_info_file = paths.get_partition_info_pointer(catalog_path)
            elif self.catalog_base_dir is not None:
                partition_info_file = paths.get_partition_info_pointer(self.catalog_base_dir)
            else:
                raise ValueError("partition_info_file is required if info was not loaded from a directory")

        file_io.write_dataframe_to_csv(self.as_dataframe(), partition_info_file, index=False)

    def write_to_metadata_files(self, catalog_path: str | Path | UPath | None = None):
        """Generate parquet metadata, using the known partitions.

        If no catalog_path is provided, the catalog base directory from the `read_from_dir` call is used.

        Args:
            catalog_path (UPath): base path for the catalog

        Returns:
            sum of the number of rows in the dataset.

        Raises:
            ValueError: if no path is provided, and could not be inferred.
        """
        if catalog_path is None:
            if self.catalog_base_dir is None:
                raise ValueError("catalog_path is required if partition info was not loaded from a directory")
            catalog_path = self.catalog_base_dir

        batches = [
            [
                pa.RecordBatch.from_arrays(
                    [[pixel.order], [pixel.pixel]],
                    names=[
                        self.METADATA_ORDER_COLUMN_NAME,
                        self.METADATA_PIXEL_COLUMN_NAME,
                    ],
                )
            ]
            for pixel in self.get_healpix_pixels()
        ]

        return write_parquet_metadata_for_batches(batches, catalog_path)

    @classmethod
    def read_from_dir(cls, catalog_base_dir: str | Path | UPath | None) -> PartitionInfo:
        """Read partition info from a file within a hats directory.

        This will look for a `partition_info.csv` file, and if not found, will look for
        a `_metadata` file. The second approach is typically slower for large catalogs
        therefore a warning is issued to the user. In internal testing with large catalogs,
        the first approach takes less than a second, while the second can take 10-20 seconds.

        Args:
            catalog_base_dir: path to the root directory of the catalog

        Returns:
            A `PartitionInfo` object with the data from the file

        Raises:
            FileNotFoundError: if neither desired file is found in the catalog_base_dir
        """
        metadata_file = paths.get_parquet_metadata_pointer(catalog_base_dir)
        partition_info_file = paths.get_partition_info_pointer(catalog_base_dir)
        if file_io.does_file_or_directory_exist(partition_info_file):
            pixel_list = PartitionInfo._read_from_csv(partition_info_file)
        elif file_io.does_file_or_directory_exist(metadata_file):
            warnings.warn("Reading partitions from parquet metadata. This is typically slow.")
            pixel_list = PartitionInfo._read_from_metadata_file(metadata_file)
        else:
            raise FileNotFoundError(
                f"_metadata or partition info file is required in catalog directory {catalog_base_dir}"
            )
        return cls(pixel_list, catalog_base_dir)

    @classmethod
    def read_from_file(cls, metadata_file: str | Path | UPath, strict: bool = False) -> PartitionInfo:
        """Read partition info from a `_metadata` file to create an object

        Args:
            metadata_file (UPath): path to the `_metadata` file
            strict (bool): use strict parsing of _metadata file. this is slower, but
                gives more helpful error messages in the case of invalid data.

        Returns:
            A `PartitionInfo` object with the data from the file
        """
        return cls(cls._read_from_metadata_file(metadata_file, strict))

    @classmethod
    def _read_from_metadata_file(
        cls, metadata_file: str | Path | UPath, strict: bool = False
    ) -> List[HealpixPixel]:
        """Read partition info list from a `_metadata` file.

        Args:
            metadata_file (UPath): path to the `_metadata` file
            strict (bool): use strict parsing of _metadata file. this is slower, but
                gives more helpful error messages in the case of invalid data.

        Returns:
            A `PartitionInfo` object with the data from the file
        """
        if strict:
            pixel_list = [
                HealpixPixel(
                    row_group_stat_single_value(row_group, cls.METADATA_ORDER_COLUMN_NAME),
                    row_group_stat_single_value(row_group, cls.METADATA_PIXEL_COLUMN_NAME),
                )
                for row_group in read_row_group_fragments(metadata_file)
            ]
        else:
            total_metadata = file_io.read_parquet_metadata(metadata_file)
            num_row_groups = total_metadata.num_row_groups

            first_row_group = total_metadata.row_group(0)
            norder_column = -1
            npix_column = -1

            for i in range(0, first_row_group.num_columns):
                column = first_row_group.column(i)
                if column.path_in_schema == cls.METADATA_ORDER_COLUMN_NAME:
                    norder_column = i
                elif column.path_in_schema == cls.METADATA_PIXEL_COLUMN_NAME:
                    npix_column = i

            if norder_column == -1 or npix_column == -1:
                raise ValueError("Metadata missing Norder or Npix column")

            row_group_index = np.arange(0, num_row_groups)

            pixel_list = [
                HealpixPixel(
                    total_metadata.row_group(index).column(norder_column).statistics.min,
                    total_metadata.row_group(index).column(npix_column).statistics.min,
                )
                for index in row_group_index
            ]
        ## Remove duplicates, preserving order.
        ## In the case of association partition join info, we may have multiple entries
        ## for the primary order/pixels.
        return list(dict.fromkeys(pixel_list))

    @classmethod
    def read_from_csv(cls, partition_info_file: str | Path | UPath) -> PartitionInfo:
        """Read partition info from a `partition_info.csv` file to create an object

        Args:
            partition_info_file (UPath): path to the `partition_info.csv` file

        Returns:
            A `PartitionInfo` object with the data from the file
        """
        return cls(cls._read_from_csv(partition_info_file))

    @classmethod
    def _read_from_csv(cls, partition_info_file: str | Path | UPath) -> PartitionInfo:
        """Read partition info from a `partition_info.csv` file to create an object

        Args:
            partition_info_file (UPath): path to the `partition_info.csv` file

        Returns:
            A `PartitionInfo` object with the data from the file
        """
        if not file_io.does_file_or_directory_exist(partition_info_file):
            raise FileNotFoundError(f"No partition info found where expected: {str(partition_info_file)}")

        data_frame = file_io.load_csv_to_pandas(partition_info_file)

        return [
            HealpixPixel(order, pixel)
            for order, pixel in zip(
                data_frame[cls.METADATA_ORDER_COLUMN_NAME],
                data_frame[cls.METADATA_PIXEL_COLUMN_NAME],
            )
        ]

    def as_dataframe(self):
        """Construct a pandas dataframe for the partition info pixels.

        Returns:
            Dataframe with order, directory, and pixel info.
        """
        partition_info_dict = {
            PartitionInfo.METADATA_ORDER_COLUMN_NAME: [],
            PartitionInfo.METADATA_PIXEL_COLUMN_NAME: [],
        }
        for pixel in self.pixel_list:
            partition_info_dict[PartitionInfo.METADATA_ORDER_COLUMN_NAME].append(pixel.order)
            partition_info_dict[PartitionInfo.METADATA_PIXEL_COLUMN_NAME].append(pixel.pixel)
        return pd.DataFrame.from_dict(partition_info_dict)

    @classmethod
    def from_healpix(cls, healpix_pixels: List[HealpixPixel]) -> PartitionInfo:
        """Create a partition info object from a list of constituent healpix pixels.

        Args:
            healpix_pixels: list of healpix pixels
        Returns:
            A `PartitionInfo` object with the same healpix pixels
        """
        return cls(healpix_pixels)

    def calculate_fractional_coverage(self):
        """Calculate what fraction of the sky is covered by partition tiles."""
        pixel_orders = [p.order for p in self.pixel_list]
        cov_order, cov_count = np.unique(pixel_orders, return_counts=True)
        area_by_order = [hp.nside2pixarea(hp.order2nside(order), degrees=True) for order in cov_order]
        # 41253 is the number of square degrees in a sphere
        # https://en.wikipedia.org/wiki/Square_degree
        return (area_by_order * cov_count).sum() / 41253
