"""Container class to hold per-partition metadata"""

from __future__ import annotations

import warnings
from pathlib import Path

import numpy as np
import pandas as pd
from upath import UPath

import hats.pixel_math.healpix_shim as hp
from hats.io import file_io, paths
from hats.pixel_math.healpix_pixel import INVALID_PIXEL, HealpixPixel


class PartitionInfo:
    """Container class for per-partition info."""

    METADATA_ORDER_COLUMN_NAME = "Norder"
    METADATA_PIXEL_COLUMN_NAME = "Npix"

    def __init__(self, pixel_list: list[HealpixPixel], catalog_base_dir: str = None) -> None:
        self.pixel_list = pixel_list
        self.catalog_base_dir = catalog_base_dir

    def get_healpix_pixels(self) -> list[HealpixPixel]:
        """Get healpix pixel objects for all pixels represented as partitions.

        Returns
        -------
        list[HealpixPixel]
            List of HealpixPixel
        """
        return self.pixel_list

    def get_highest_order(self) -> int:
        """Get the highest healpix order for the dataset.

        Returns
        -------
        int
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

        Parameters
        ----------
        partition_info_file : str | Path | UPath | None
            path to where the `partition_info.csv` file will be written.
        catalog_path : str | Path | UPath | None
            base directory for a catalog where the `partition_info.csv` file will be written.

        Raises
        ------
        ValueError
            if no path is provided, and could not be inferred.
        """
        if partition_info_file is None:
            if catalog_path is not None:
                partition_info_file = paths.get_partition_info_pointer(catalog_path)
            elif self.catalog_base_dir is not None:
                partition_info_file = paths.get_partition_info_pointer(self.catalog_base_dir)
            else:
                raise ValueError("partition_info_file is required if info was not loaded from a directory")

        file_io.write_dataframe_to_csv(self.as_dataframe(), partition_info_file, index=False)

    @classmethod
    def read_from_dir(cls, catalog_base_dir: str | Path | UPath | None) -> PartitionInfo:
        """Read partition info from a file within a hats directory.

        This will look for a `partition_info.csv` file, and if not found, will look for
        a `_metadata` file. The second approach is typically slower for large catalogs
        therefore a warning is issued to the user. In internal testing with large catalogs,
        the first approach takes less than a second, while the second can take 10-20 seconds.

        Parameters
        ----------
        catalog_base_dir : str | Path | UPath | None
            path to the root directory of the catalog

        Returns
        -------
        PartitionInfo
            A `PartitionInfo` object with the data from the file

        Raises
        ------
        FileNotFoundError
            if neither desired file is found in the catalog_base_dir
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
    def read_from_file(cls, metadata_file: str | Path | UPath) -> PartitionInfo:
        """Read partition info from a `_metadata` file to create an object

        Parameters
        ----------
        metadata_file : str | Path | UPath
            path to the `_metadata` file

        Returns
        -------
        PartitionInfo
            A `PartitionInfo` object with the data from the file
        """
        return cls(cls._read_from_metadata_file(metadata_file))

    @classmethod
    def _read_from_metadata_file(cls, metadata_file: str | Path | UPath) -> list[HealpixPixel]:
        """Read partition info list from a `_metadata` file.

        Parameters
        ----------
        metadata_file : str | Path | UPath
            path to the `_metadata` file

        Returns
        -------
        list[HealpixPixel]
            The list of `HealpixPixel` extracted from the data in the metadata file
        """
        total_metadata = file_io.read_parquet_metadata(metadata_file)
        if total_metadata.num_row_groups == 0 or total_metadata.row_group(0).num_columns == 0:
            raise ValueError(f"Insufficient metadata in file {metadata_file}")

        pixel_list = [
            paths.get_healpix_from_path(total_metadata.row_group(index).column(0).file_path)
            for index in range(0, total_metadata.num_row_groups)
        ]
        pixel_list = [p for p in pixel_list if p != INVALID_PIXEL]
        if len(pixel_list) == 0:
            raise ValueError(f"Insufficient metadata in file {metadata_file}")
        ## Remove duplicates, preserving order.
        return list(dict.fromkeys(pixel_list))

    @classmethod
    def read_from_csv(cls, partition_info_file: str | Path | UPath) -> PartitionInfo:
        """Read partition info from a `partition_info.csv` file to create an object

        Parameters
        ----------
        partition_info_file : str | Path | UPath
            path to the `partition_info.csv` file

        Returns
        -------
        PartitionInfo
            A `PartitionInfo` object with the data from the file
        """
        return cls(cls._read_from_csv(partition_info_file))

    @classmethod
    def _read_from_csv(cls, partition_info_file: str | Path | UPath) -> PartitionInfo:
        """Read partition info from a `partition_info.csv` file to create an object

        Parameters
        ----------
        partition_info_file : str | Path | UPath
            path to the `partition_info.csv` file

        Returns
        -------
        PartitionInfo
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

        Returns
        -------
        pd.DataFrame
            Pandas Dataframe with order, directory, and pixel info.
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
    def from_healpix(cls, healpix_pixels: list[HealpixPixel]) -> PartitionInfo:
        """Create a partition info object from a list of constituent healpix pixels.

        Parameters
        ----------
        healpix_pixels: list[HealpixPixel]
            a list of constituent healpix pixels

        Returns
        -------
        PartitionInfo
            A `PartitionInfo` object with the same healpix pixels
        """
        return cls(healpix_pixels)

    def calculate_fractional_coverage(self):
        """Calculate what fraction of the sky is covered by partition tiles."""
        pixel_orders = [p.order for p in self.pixel_list]
        cov_order, cov_count = np.unique(pixel_orders, return_counts=True)
        area_by_order = [hp.order2pixarea(order, degrees=True) for order in cov_order]
        # 41253 is the number of square degrees in a sphere
        # https://en.wikipedia.org/wiki/Square_degree
        return (area_by_order * cov_count).sum() / (360**2 / np.pi)
