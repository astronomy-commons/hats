"""Container class to hold primary-to-join partition metadata"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from upath import UPath

from hats.catalog.partition_info import PartitionInfo
from hats.io import file_io, paths
from hats.pixel_math.healpix_pixel import HealpixPixel


class PartitionJoinInfo:
    """Association catalog metadata with which partitions matches occur in the join"""

    PRIMARY_ORDER_COLUMN_NAME = "Norder"
    PRIMARY_PIXEL_COLUMN_NAME = "Npix"
    JOIN_ORDER_COLUMN_NAME = "join_Norder"
    JOIN_PIXEL_COLUMN_NAME = "join_Npix"

    COLUMN_NAMES = [
        PRIMARY_ORDER_COLUMN_NAME,
        PRIMARY_PIXEL_COLUMN_NAME,
        JOIN_ORDER_COLUMN_NAME,
        JOIN_PIXEL_COLUMN_NAME,
    ]

    def __init__(self, join_info_df: pd.DataFrame, catalog_base_dir: str = None) -> None:
        self.data_frame = join_info_df
        self.catalog_base_dir = catalog_base_dir
        self._check_column_names()

    def _check_column_names(self):
        for column in self.COLUMN_NAMES:
            if column not in self.data_frame.columns:
                raise ValueError(f"join_info_df does not contain column {column}")

    def primary_to_join_map(self) -> dict[HealpixPixel, list[HealpixPixel]]:
        """Generate a map from a single primary pixel to one or more pixels in the join catalog.

        Lots of cute comprehension is happening here, so watch out!
        We create tuple of (primary order/pixel) and [array of tuples of (join order/pixel)]

        Returns:
            dictionary mapping (primary order/pixel) to [array of (join order/pixel)]
        """
        primary_map = self.data_frame.groupby(
            [self.PRIMARY_ORDER_COLUMN_NAME, self.PRIMARY_PIXEL_COLUMN_NAME], group_keys=True
        )
        primary_to_join = [
            (
                HealpixPixel(int(primary_pixel[0]), int(primary_pixel[1])),
                [
                    HealpixPixel(int(object_elem[0]), int(object_elem[1]))
                    for object_elem in join_group.dropna().to_numpy().T[2:4].T
                ],
            )
            for primary_pixel, join_group in primary_map
        ]
        ## Treat the array of tuples as a dictionary.
        primary_to_join = dict(primary_to_join)
        return primary_to_join

    def write_to_csv(self, catalog_path: str | Path | UPath | None = None):
        """Write all partition data to CSV files.

        Two files will be written:

            - partition_info.csv - covers all primary catalog pixels, and should match the file structure
            - partition_join_info.csv - covers all pairwise relationships between primary and
              join catalogs.

        Args:
            catalog_path: path to the directory where the
                `partition_join_info.csv` file will be written

        Raises:
            ValueError: if no path is provided, and could not be inferred.
        """
        if catalog_path is None:
            if self.catalog_base_dir is None:
                raise ValueError("catalog_path is required if info was not loaded from a directory")
            catalog_path = self.catalog_base_dir

        partition_join_info_file = paths.get_partition_join_info_pointer(catalog_path)
        file_io.write_dataframe_to_csv(self.data_frame, partition_join_info_file, index=False)

        primary_pixels = self.primary_to_join_map().keys()
        partition_info_pointer = paths.get_partition_info_pointer(catalog_path)
        partition_info = PartitionInfo.from_healpix(primary_pixels)
        partition_info.write_to_file(partition_info_file=partition_info_pointer)

    @classmethod
    def read_from_dir(cls, catalog_base_dir: str | Path | UPath | None = None) -> PartitionJoinInfo:
        """Read partition join info from a partition_join_info file within a hats directory.

        Args:
            catalog_base_dir: path to the root directory of the catalog

        Returns:
            A `PartitionJoinInfo` object with the data from the file

        Raises:
            FileNotFoundError: if the desired file is found in the catalog_base_dir
        """
        partition_join_info_file = paths.get_partition_join_info_pointer(catalog_base_dir)
        if file_io.does_file_or_directory_exist(partition_join_info_file):
            pixel_frame = PartitionJoinInfo._read_from_csv(partition_join_info_file)
        else:
            raise FileNotFoundError(
                f"partition join info file is required in catalog directory {catalog_base_dir}"
            )
        return cls(pixel_frame, catalog_base_dir)

    @classmethod
    def read_from_csv(cls, partition_join_info_file: str | Path | UPath) -> PartitionJoinInfo:
        """Read partition join info from a `partition_join_info.csv` file to create an object

        Args:
            partition_join_info_file (UPath): path to the `partition_join_info.csv` file

        Returns:
            A `PartitionJoinInfo` object with the data from the file
        """
        return cls(cls._read_from_csv(partition_join_info_file))

    @classmethod
    def _read_from_csv(cls, partition_join_info_file: str | Path | UPath) -> pd.DataFrame:
        """Read partition join info from a `partition_join_info.csv` file to create an object

        Args:
            partition_join_info_file (UPath): path to the `partition_join_info.csv` file

        Returns:
            A `PartitionJoinInfo` object with the data from the file
        """
        if not file_io.does_file_or_directory_exist(partition_join_info_file):
            raise FileNotFoundError(
                f"No partition join info found where expected: {str(partition_join_info_file)}"
            )

        return file_io.load_csv_to_pandas(partition_join_info_file)
