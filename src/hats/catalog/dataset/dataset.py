from __future__ import annotations

from pathlib import Path
from typing import List

import pyarrow as pa
from upath import UPath

from hats.catalog.dataset.table_properties import TableProperties
from hats.io import file_io
from hats.io.parquet_metadata import aggregate_column_statistics


# pylint: disable=too-few-public-methods
class Dataset:
    """A base HATS dataset that contains a properties file
    and the data contained in parquet files"""

    def __init__(
        self,
        catalog_info: TableProperties,
        catalog_path: str | Path | UPath | None = None,
        schema: pa.Schema | None = None,
    ) -> None:
        """Initializes a Dataset

        Args:
            catalog_info: A TableProperties object with the catalog metadata
            catalog_path: If the catalog is stored on disk, specify the location of the catalog
                Does not load the catalog from this path, only store as metadata
            schema (pa.Schema): The pyarrow schema for the catalog
        """
        self.catalog_info = catalog_info
        self.catalog_name = self.catalog_info.catalog_name

        self.catalog_path = catalog_path
        self.on_disk = catalog_path is not None
        self.catalog_base_dir = file_io.get_upath(self.catalog_path)
        self.schema = schema

    def aggregate_column_statistics(
        self,
        exclude_hats_columns: bool = True,
        exclude_columns: List[str] = None,
        include_columns: List[str] = None,
    ):
        """Read footer statistics in parquet metadata, and report on global min/max values.

        Args:
            exclude_hats_columns (bool): exclude HATS spatial and partitioning fields
                from the statistics. Defaults to True.
            exclude_columns (List[str]): additional columns to exclude from the statistics.
            include_columns (List[str]): if specified, only return statistics for the column
                names provided. Defaults to None, and returns all non-hats columns.
        """
        return aggregate_column_statistics(
            self.catalog_base_dir / "dataset" / "_metadata",
            exclude_hats_columns=exclude_hats_columns,
            exclude_columns=exclude_columns,
            include_columns=include_columns,
        )
