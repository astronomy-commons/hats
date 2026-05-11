from __future__ import annotations

import warnings
from pathlib import Path

import pandas as pd
import pyarrow as pa
from deprecated import deprecated  # type: ignore
from upath import UPath

from hats.catalog.catalog_snapshot import CatalogSnapshot
from hats.catalog.dataset.table_properties import TableProperties
from hats.io import file_io
from hats.io.parquet_metadata import aggregate_column_statistics, per_partition_statistics


# pylint: disable=too-few-public-methods
class Dataset:
    """A base HATS dataset that contains a properties file and the data contained in parquet files"""

    def __init__(
        self,
        catalog_info: TableProperties,
        catalog_path: str | Path | UPath | None = None,
        schema: pa.Schema | None = None,
        snapshot: CatalogSnapshot | None = None,
        generate_snapshot: bool = False,
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
        snapshot : CatalogSnapshot
            Immutable snapshot of the catalog's original on-disk schema and partition info.
            May NOT be modified e.g. based on loaded columns or filtered pixels.
        generate_snapshot : bool
            If True and no snapshot is provided, automatically generate one from the current
            schema and catalog_info. Default False.
        """
        self.catalog_info = catalog_info
        self.catalog_name = self.catalog_info.catalog_name

        self.catalog_path = catalog_path
        self.catalog_base_dir = file_io.get_upath(self.catalog_path)
        self.schema = schema
        if snapshot is None and generate_snapshot:
            snapshot = CatalogSnapshot(schema=schema, catalog_info=catalog_info)
        self.snapshot = snapshot

    @property
    def original_schema(self) -> pa.Schema | None:
        """The original on-disk schema, before any column selection."""
        return self.snapshot.schema if self.snapshot is not None else None

    @property
    def on_disk(self) -> bool:
        """Is the catalog stored on disk?"""
        return self.catalog_path is not None

    @property
    def unmodified(self) -> bool:
        """Has the catalog been modified from its original on disk state?"""
        return self.catalog_info.total_rows is not None

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
        if not self.unmodified:
            warnings.warn(
                "Calling aggregate_column_statistics on a modified catalog. Results may be inaccurate."
            )
        return aggregate_column_statistics(
            self.catalog_base_dir / "dataset" / "_metadata",
            exclude_hats_columns=exclude_hats_columns,
            exclude_columns=exclude_columns,
            include_columns=include_columns,
        )

    @deprecated(
        version="0.9.1",
        reason="`per_pixel_statistics` will be removed in the future, "
        "use `per_partition_statistics` instead.",
    )
    def per_pixel_statistics(
        self,
        *,
        exclude_hats_columns: bool = True,
        exclude_columns: list[str] | None = None,
        include_columns: list[str] | None = None,
        only_numeric_columns: bool = False,
        include_stats: list[str] | None = None,
        multi_index=False,
        per_row_group: bool = False,
    ):
        """Read footer statistics in parquet metadata, and report on statistics about
        each pixel partition."""
        return self.per_partition_statistics(
            exclude_hats_columns=exclude_hats_columns,
            exclude_columns=exclude_columns,
            include_columns=include_columns,
            only_numeric_columns=only_numeric_columns,
            include_stats=include_stats,
            multi_index=multi_index,
            per_row_group=per_row_group,
        )

    def per_partition_statistics(
        self,
        *,
        exclude_hats_columns: bool = True,
        exclude_columns: list[str] = None,
        include_columns: list[str] = None,
        only_numeric_columns: bool = False,
        include_stats: list[str] = None,
        multi_index=False,
        per_row_group: bool = False,
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
            warnings.warn("Calling per_partition_statistics on an in-memory catalog. No results.")
            return pd.DataFrame()
        if not self.unmodified:
            warnings.warn(
                "Calling per_partition_statistics on a modified catalog. Results may be inaccurate."
            )
        return per_partition_statistics(
            self.catalog_base_dir / "dataset" / "_metadata",
            exclude_hats_columns=exclude_hats_columns,
            exclude_columns=exclude_columns,
            include_columns=include_columns,
            only_numeric_columns=only_numeric_columns,
            include_stats=include_stats,
            multi_index=multi_index,
            per_row_group=per_row_group,
        )
