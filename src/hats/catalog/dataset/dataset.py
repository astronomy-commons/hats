from __future__ import annotations

import warnings
from pathlib import Path

import pandas as pd
import pyarrow as pa
from upath import UPath

from hats.catalog.dataset.table_properties import TableProperties
from hats.io import file_io
from hats.io.parquet_metadata import aggregate_column_statistics, per_pixel_statistics


# pylint: disable=too-few-public-methods
class Dataset:
    """A base HATS dataset that contains a properties file and the data contained in parquet files"""

    def __init__(
        self,
        catalog_info: TableProperties,
        catalog_path: str | Path | UPath | None = None,
        schema: pa.Schema | None = None,
        original_schema: pa.Schema | None = None,
    ) -> None:
        """Initializes a Dataset

        Parameters
        ----------
        catalog_info: TableProperties
            A TableProperties object with the catalog metadata
        catalog_path: str | Path | UPath | None
            If the catalog is stored on disk, specify the location of the catalog
            Does not load the catalog from this path, only store as metadata
        schema : pa.Schema
            The pyarrow schema for the catalog. May be modified e.g. based on loaded columns
        original_schema : pa.Schema
            The original pyarrow schema for the catalog. May NOT be modified e.g. based on loaded columns
        """
        self.catalog_info = catalog_info
        self.catalog_name = self.catalog_info.catalog_name

        self.catalog_path = catalog_path
        self.on_disk = catalog_path is not None
        self.catalog_base_dir = file_io.get_upath(self.catalog_path)
        self.schema = schema
        self.original_schema = original_schema

    def aggregate_column_statistics(
        self,
        exclude_hats_columns: bool = True,
        exclude_columns: list[str] = None,
        include_columns: list[str] = None,
    ):
        """Read footer statistics in parquet metadata, and report on global min/max values.

        Parameters
        ----------
        exclude_hats_columns : bool
            exclude HATS spatial and partitioning fields
            from the statistics. Defaults to True.
        exclude_columns : list[str]
            additional columns to exclude from the statistics.
        include_columns : list[str]
            if specified, only return statistics for the column
            names provided. Defaults to None, and returns all non-hats columns.

        Returns
        -------
        Dataframe
            aggregated statistics.
        """
        if not self.on_disk:
            warnings.warn("Calling aggregate_column_statistics on an in-memory catalog. No results.")
            return pd.DataFrame()
        return aggregate_column_statistics(
            self.catalog_base_dir / "dataset" / "_metadata",
            exclude_hats_columns=exclude_hats_columns,
            exclude_columns=exclude_columns,
            include_columns=include_columns,
        )

    def per_pixel_statistics(
        self,
        exclude_hats_columns: bool = True,
        exclude_columns: list[str] = None,
        include_columns: list[str] = None,
        include_stats: list[str] = None,
        multi_index=False,
    ):
        """Read footer statistics in parquet metadata, and report on statistics about
        each pixel partition.

        Parameters
        ----------
        exclude_hats_columns : bool
            exclude HATS spatial and partitioning fields from the statistics. Defaults to True.
        exclude_columns : list[str]
            additional columns to exclude from the statistics.
        include_columns : list[str]
            if specified, only return statistics for the column
            names provided. Defaults to None, and returns all non-hats columns.
        include_stats : list[str]
            if specified, only return the kinds of values from list
            (min_value, max_value, null_count, row_count). Defaults to None, and returns all values.
        multi_index : bool
            should the returned frame be created with a multi-index, first on
            pixel, then on column name? Default is False, and instead indexes on pixel, with
            separate columns per-data-column and stat value combination.
            (Default value = False)

        Returns
        -------
        Dataframe
            all statistics.
        """
        if not self.on_disk:
            warnings.warn("Calling per_pixel_statistics on an in-memory catalog. No results.")
            return pd.DataFrame()
        return per_pixel_statistics(
            self.catalog_base_dir / "dataset" / "_metadata",
            exclude_hats_columns=exclude_hats_columns,
            exclude_columns=exclude_columns,
            include_columns=include_columns,
            include_stats=include_stats,
            multi_index=multi_index,
        )
