from __future__ import annotations

import warnings
from pathlib import Path

import pandas as pd
import pyarrow as pa
from mocpy import MOC
from typing_extensions import Self
from upath import UPath

from hats.catalog.dataset import Dataset
from hats.catalog.dataset.table_properties import TableProperties
from hats.catalog.partition_info import PartitionInfo
from hats.inspection import plot_pixels
from hats.inspection.visualize_catalog import plot_moc
from hats.io.parquet_metadata import aggregate_column_statistics, per_pixel_statistics
from hats.pixel_math import HealpixPixel
from hats.pixel_math.region_to_moc import box_to_moc, cone_to_moc, pixel_list_to_moc, polygon_to_moc
from hats.pixel_math.spatial_index import SPATIAL_INDEX_COLUMN, SPATIAL_INDEX_ORDER
from hats.pixel_tree import PixelAlignment, PixelAlignmentType
from hats.pixel_tree.moc_filter import filter_by_moc
from hats.pixel_tree.pixel_alignment import align_with_mocs
from hats.pixel_tree.pixel_tree import PixelTree


class HealpixDataset(Dataset):
    """A HATS dataset partitioned with a HEALPix partitioning structure.

    Catalogs of this type are partitioned based on the ra and dec of the points with each partition
    containing points within a given HEALPix pixel. The files are in the form::

        Norder=/Dir=/Npix=.parquet
    """

    def __init__(
        self,
        catalog_info: TableProperties,
        pixels: PartitionInfo | PixelTree | list[HealpixPixel],
        catalog_path: str | Path | UPath | None = None,
        moc: MOC | None = None,
        schema: pa.Schema | None = None,
        original_schema: pa.Schema | None = None,
    ) -> None:
        """Initializes a Catalog

        Parameters
        ----------
        catalog_info: TableProperties
            A TableProperties object with the catalog metadata
        pixels: PartitionInfo | PixelTree | list[HealpixPixel]
            Specifies the pixels contained in the catalog. Can be either a
            list of HealpixPixel, `PartitionInfo object`, or a `PixelTree` object
        catalog_path: str | Path | UPath | None
            If the catalog is stored on disk, specify the location of the catalog
            Does not load the catalog from this path, only store as metadata
        moc : mocpy.MOC
            MOC object representing the coverage of the catalog
        schema : pa.Schema
            The pyarrow schema for the catalog. May be modified e.g. based on loaded columns
        original_schema : pa.Schema
            The original pyarrow schema for the catalog. May NOT be modified e.g. based on loaded columns
        """
        super().__init__(
            catalog_info, catalog_path=catalog_path, schema=schema, original_schema=original_schema
        )
        self.partition_info = self._get_partition_info_from_pixels(pixels)
        self.pixel_tree = self._get_pixel_tree_from_pixels(pixels)
        self.moc = moc

    def get_healpix_pixels(self) -> list[HealpixPixel]:
        """Get healpix pixel objects for all pixels contained in the catalog.

        Returns
        -------
        list[HealpixPixel]
            List of HealpixPixel
        """
        return self.partition_info.get_healpix_pixels()

    @staticmethod
    def _get_partition_info_from_pixels(
        pixels: PartitionInfo | PixelTree | list[HealpixPixel],
    ) -> PartitionInfo:
        if isinstance(pixels, PartitionInfo):
            return pixels
        if isinstance(pixels, PixelTree):
            return PartitionInfo.from_healpix(pixels.get_healpix_pixels())
        if pd.api.types.is_list_like(pixels):
            return PartitionInfo.from_healpix(pixels)
        raise TypeError("Pixels must be of type PartitionInfo, PixelTree, or List[HealpixPixel]")

    @staticmethod
    def _get_pixel_tree_from_pixels(pixels: PartitionInfo | PixelTree | list[HealpixPixel]) -> PixelTree:
        if isinstance(pixels, PartitionInfo):
            return PixelTree.from_healpix(pixels.get_healpix_pixels())
        if isinstance(pixels, PixelTree):
            return pixels
        if pd.api.types.is_list_like(pixels):
            return PixelTree.from_healpix(pixels)
        raise TypeError("Pixels must be of type PartitionInfo, PixelTree, or List[HealpixPixel]")

    def __len__(self):
        """The number of rows in the catalog.

        Returns
        -------
        int
            The number of rows in the catalog, as specified in its metadata.
            This value is undetermined when the catalog is modified, and
            therefore an error is raised.
        """
        if self.catalog_info.total_rows == 0:
            raise ValueError("The number of rows is undetermined because the catalog was modified.")
        return self.catalog_info.total_rows

    def get_max_coverage_order(self, default_order: int = 3) -> int:
        """Gets the maximum HEALPix order for which the coverage of the catalog is known from the pixel
        tree and moc if it exists

        Parameters
        ----------
        default_order : int
            The order to return if the dataset has no pixels.
            (Default value = 3)

        Returns
        -------
        int
            maximum HEALPix order
        """
        if len(self.pixel_tree) == 0:
            return default_order
        max_order = (
            max(self.moc.max_order, self.pixel_tree.get_max_depth())
            if self.moc is not None
            else self.pixel_tree.get_max_depth()
        )
        return max_order

    def filter_from_pixel_list(self, pixels: list[HealpixPixel]) -> Self:
        """Filter the pixels in the catalog to only include any that overlap with the requested pixels.

        Parameters
        ----------
        pixels : list[HealpixPixel]
            the pixels to include

        Returns
        -------
        HealpixDataset
            A new catalog with only the pixels that overlap with the given pixels. Note that we reset the
            total_rows to None, as updating would require a scan over the new pixel sizes.
        """
        return self.filter_by_moc(pixel_list_to_moc(pixels))

    def filter_by_cone(self, ra: float, dec: float, radius_arcsec: float) -> Self:
        """Filter the pixels in the catalog to only include the pixels that overlap with a cone

        Parameters
        ----------
        ra : float
            Right ascension of the center of the cone, in degrees
        dec : float
            Declination of the center of the cone, in degrees
        radius_arcsec : float
            Radius of the cone, in arcseconds

        Returns
        -------
        HealpixDataset
            A new catalog with only the pixels that overlap with the specified cone
        """
        return self.filter_by_moc(cone_to_moc(ra, dec, radius_arcsec, self.get_max_coverage_order()))

    def filter_by_box(self, ra: tuple[float, float], dec: tuple[float, float]) -> Self:
        """Filter the pixels in the catalog to only include the pixels that overlap with a
        zone, defined by right ascension and declination ranges. The right ascension edges follow
        great arc circles and the declination edges follow small arc circles.

        Parameters
        ----------
        ra : tuple[float, float]
            Right ascension range, in degrees
        dec : tuple[float, float]
            Declination range, in degrees

        Returns
        -------
        HealpixDataset
            A new catalog with only the pixels that overlap with the specified region
        """
        return self.filter_by_moc(box_to_moc(ra, dec, self.get_max_coverage_order()))

    def filter_by_polygon(self, vertices: list[tuple[float, float]]) -> Self:
        """Filter the pixels in the catalog to only include the pixels that overlap
        with a polygonal sky region.

        Parameters
        ----------
        vertices : list[tuple[float, float]]
            The list of vertice coordinates for the polygon, (ra, dec), in degrees.

        Returns
        -------
        HealpixDataset
            A new catalog with only the pixels that overlap with the specified polygon.
        """
        return self.filter_by_moc(polygon_to_moc(vertices, self.get_max_coverage_order()))

    def filter_by_moc(self, moc: MOC) -> Self:
        """Filter the pixels in the catalog to only include the pixels that overlap with the moc provided.

        Parameters
        ----------
        moc : mocpy.MOC
            the moc to filter by

        Returns
        -------
        HealpixDataset
            A new catalog with only the pixels that overlap with the moc. Note that we reset the total_rows
            to 0, as updating would require a scan over the new pixel sizes.
        """
        filtered_tree = filter_by_moc(self.pixel_tree, moc)
        filtered_moc = self.moc.intersection(moc) if self.moc is not None else None
        filtered_catalog_info = self.catalog_info.copy_and_update(total_rows=0)
        return self.__class__(
            filtered_catalog_info,
            pixels=filtered_tree,
            catalog_path=self.catalog_path,
            moc=filtered_moc,
            schema=self.schema,
            original_schema=self.original_schema,
        )

    def align(
        self, other_cat: Self, alignment_type: PixelAlignmentType = PixelAlignmentType.INNER
    ) -> PixelAlignment:
        """Performs an alignment to another catalog, using the pixel tree and mocs if available

        An alignment compares the pixel structures of the two catalogs, checking which pixels overlap.
        The alignment includes the mapping of all pairs of pixels in each tree that overlap with each other,
        and the aligned tree which consists of the overlapping pixels in the two input catalogs, using the
        higher order pixels where there is overlap with differing orders.

        For more information, see this document:
        https://docs.google.com/document/d/1gqb8qb3HiEhLGNav55LKKFlNjuusBIsDW7FdTkc5mJU/edit?usp=sharing

        Parameters
        ----------
        other_cat : Catalog
            The catalog to align to
        alignment_type : PixelAlignmentType
            The type of alignment describing how to handle nodes which exist in one tree but not the other.
            Mirrors the 'how' argument of a pandas/sql join. Options are:

            - "inner" - only use pixels that appear in both catalogs
            - "left" - use all pixels that appear in the left catalog and any overlapping from the right
            - "right" - use all pixels that appear in the right catalog and any overlapping from the left
            - "outer" - use all pixels from both catalogs

        Returns
        -------
        PixelAlignment
            A `PixelAlignment` object with the alignment from the two catalogs
        """
        return align_with_mocs(
            self.pixel_tree, other_cat.pixel_tree, self.moc, other_cat.moc, alignment_type=alignment_type
        )

    def plot_pixels(self, **kwargs):
        """Create a visual map of the pixel density of the catalog.

        Parameters
        ----------
        **kwargs
            Additional args to pass to `hats.inspection.visualize_catalog.plot_healpix_map`
        """
        return plot_pixels(self, **kwargs)

    def plot_moc(self, **kwargs):
        """Create a visual map of the coverage of the catalog.

        Parameters
        ----------
        **kwargs
            Additional args to pass to `hats.inspection.visualize_catalog.plot_moc`
        """
        default_title = f"Coverage MOC of {self.catalog_name}"
        plot_args = {"title": default_title}
        plot_args.update(kwargs)
        return plot_moc(self.moc, **plot_args)

    def aggregate_column_statistics(
        self,
        exclude_hats_columns: bool = True,
        exclude_columns: list[str] | None = None,
        include_columns: list[str] | None = None,
        include_pixels: list[HealpixPixel] | None = None,
    ):
        """Read footer statistics in parquet metadata, and report on global min/max values.

        Parameters
        ----------
        exclude_hats_columns : bool
            exclude HATS spatial and partitioning fields
            from the statistics. Defaults to True.
        exclude_columns : list[str] | None
            additional columns to exclude from the statistics.
        include_columns : list[str] | None
            if specified, only return statistics for the column
            names provided. Defaults to None, and returns all non-hats columns.
        include_pixels: list[HealpixPixel] | None
            (Default value = None)

        Returns
        -------
        Dataframe
            aggregated statistics
        """
        if not self.on_disk:
            warnings.warn("Calling aggregate_column_statistics on an in-memory catalog. No results.")
            return pd.DataFrame()

        if include_pixels is None:
            include_pixels = self.get_healpix_pixels()
        return aggregate_column_statistics(
            self.catalog_base_dir / "dataset" / "_metadata",
            exclude_hats_columns=exclude_hats_columns,
            exclude_columns=exclude_columns,
            include_columns=include_columns,
            include_pixels=include_pixels,
        )

    def per_pixel_statistics(
        self,
        exclude_hats_columns: bool = True,
        exclude_columns: list[str] | None = None,
        include_columns: list[str] | None = None,
        include_stats: list[str] | None = None,
        multi_index=False,
        include_pixels: list[HealpixPixel] | None = None,
    ):
        """Read footer statistics in parquet metadata, and report on statistics about
        each pixel partition.

        Parameters
        ----------
        exclude_hats_columns : bool
            exclude HATS spatial and partitioning fields
            from the statistics. Defaults to True.
        exclude_columns : list[str] | None
            additional columns to exclude from the statistics.
        include_columns : list[str] | None
            if specified, only return statistics for the column
            names provided. Defaults to None, and returns all non-hats columns.
        include_stats : list[str] | None
            if specified, only return the kinds of values from list
            (min_value, max_value, null_count, row_count). Defaults to None, and returns all values.
        multi_index : bool
            should the returned frame be created with a multi-index, first on
            pixel, then on column name? (Default value = False)
        include_pixels : list[HealpixPixel] | None
            if specified, only return statistics
            for the pixels indicated. Defaults to none, and returns all pixels.

        Returns
        -------
        Dataframe
            granular statistics
        """
        if not self.on_disk:
            warnings.warn("Calling per_pixel_statistics on an in-memory catalog. No results.")
            return pd.DataFrame()

        if include_pixels is None:
            include_pixels = self.get_healpix_pixels()
        return per_pixel_statistics(
            self.catalog_base_dir / "dataset" / "_metadata",
            exclude_hats_columns=exclude_hats_columns,
            exclude_columns=exclude_columns,
            include_columns=include_columns,
            include_stats=include_stats,
            multi_index=multi_index,
            include_pixels=include_pixels,
        )

    def has_healpix_column(self):
        """Does this catalog's schema contain a healpix spatial index column?

        This is True if either:

        - there is a value for the ``hats_col_healpix`` property, and that string
          exists as a column name in the pyarrow schema
        - there is a ``_healpix_29`` column in the pyarrow schema

        Returns
        -------
        bool
            if the dataset has a healpix column in the properties
        """
        property_column = self.catalog_info.healpix_column
        if property_column:
            return not self.schema or property_column in self.schema.names
        if self.schema:
            if SPATIAL_INDEX_COLUMN in self.schema.names:
                self.catalog_info.healpix_column = SPATIAL_INDEX_COLUMN
                self.catalog_info.healpix_order = SPATIAL_INDEX_ORDER
                return True
        return False
