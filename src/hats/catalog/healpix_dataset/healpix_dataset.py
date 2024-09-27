from __future__ import annotations

import warnings
from pathlib import Path
from typing import List, Tuple, Union

import numpy as np
import pandas as pd
import pyarrow as pa
from mocpy import MOC
from typing_extensions import Self
from upath import UPath

import hats.pixel_math.healpix_shim as hp
from hats.catalog.dataset import Dataset
from hats.catalog.dataset.table_properties import TableProperties
from hats.catalog.partition_info import PartitionInfo
from hats.inspection import plot_pixel_list
from hats.io import file_io, paths
from hats.io.file_io import read_parquet_metadata
from hats.pixel_math import HealpixPixel
from hats.pixel_tree import PixelAlignment, PixelAlignmentType
from hats.pixel_tree.moc_filter import filter_by_moc
from hats.pixel_tree.pixel_alignment import align_with_mocs
from hats.pixel_tree.pixel_tree import PixelTree

PixelInputTypes = Union[PartitionInfo, PixelTree, List[HealpixPixel]]


class HealpixDataset(Dataset):
    """A HATS dataset partitioned with a HEALPix partitioning structure.

    Catalogs of this type are partitioned based on the ra and dec of the points with each partition
    containing points within a given HEALPix pixel. The files are in the form::

        Norder=/Dir=/Npix=.parquet
    """

    def __init__(
        self,
        catalog_info: TableProperties,
        pixels: PixelInputTypes,
        catalog_path: str | Path | UPath | None = None,
        moc: MOC | None = None,
        schema: pa.Schema | None = None,
    ) -> None:
        """Initializes a Catalog

        Args:
            catalog_info: TableProperties object with catalog metadata
            pixels: Specifies the pixels contained in the catalog. Can be either a
                list of HealpixPixel, `PartitionInfo object`, or a `PixelTree` object
            catalog_path: If the catalog is stored on disk, specify the location of the catalog
                Does not load the catalog from this path, only store as metadata
            moc (mocpy.MOC): MOC object representing the coverage of the catalog
            schema (pa.Schema): The pyarrow schema for the catalog
        """
        super().__init__(catalog_info, catalog_path=catalog_path)
        self.partition_info = self._get_partition_info_from_pixels(pixels)
        self.pixel_tree = self._get_pixel_tree_from_pixels(pixels)
        self.moc = moc
        self.schema = schema

    def get_healpix_pixels(self) -> List[HealpixPixel]:
        """Get healpix pixel objects for all pixels contained in the catalog.

        Returns:
            List of HealpixPixel
        """
        return self.partition_info.get_healpix_pixels()

    @staticmethod
    def _get_partition_info_from_pixels(pixels: PixelInputTypes) -> PartitionInfo:
        if isinstance(pixels, PartitionInfo):
            return pixels
        if isinstance(pixels, PixelTree):
            return PartitionInfo.from_healpix(pixels.get_healpix_pixels())
        if pd.api.types.is_list_like(pixels):
            return PartitionInfo.from_healpix(pixels)
        raise TypeError("Pixels must be of type PartitionInfo, PixelTree, or List[HealpixPixel]")

    @staticmethod
    def _get_pixel_tree_from_pixels(pixels: PixelInputTypes) -> PixelTree:
        if isinstance(pixels, PartitionInfo):
            return PixelTree.from_healpix(pixels.get_healpix_pixels())
        if isinstance(pixels, PixelTree):
            return pixels
        if pd.api.types.is_list_like(pixels):
            return PixelTree.from_healpix(pixels)
        raise TypeError("Pixels must be of type PartitionInfo, PixelTree, or List[HealpixPixel]")

    @classmethod
    def _read_args(cls, catalog_base_dir: str | Path | UPath) -> Tuple[TableProperties, PartitionInfo]:
        args = super()._read_args(catalog_base_dir)
        partition_info = PartitionInfo.read_from_dir(catalog_base_dir)
        return args + (partition_info,)

    @classmethod
    def _read_kwargs(cls, catalog_base_dir: str | Path | UPath) -> dict:
        kwargs = super()._read_kwargs(catalog_base_dir)
        kwargs["moc"] = cls._read_moc_from_point_map(catalog_base_dir)
        kwargs["schema"] = cls._read_schema_from_metadata(catalog_base_dir)
        return kwargs

    @classmethod
    def _read_moc_from_point_map(cls, catalog_base_dir: str | Path | UPath) -> MOC | None:
        """Reads a MOC object from the `point_map.fits` file if it exists in the catalog directory"""
        point_map_path = paths.get_point_map_file_pointer(catalog_base_dir)
        if not file_io.does_file_or_directory_exist(point_map_path):
            return None
        fits_image = file_io.read_fits_image(point_map_path)
        order = hp.nside2order(hp.npix2nside(len(fits_image)))
        boolean_skymap = fits_image.astype(bool)
        ipix = np.where(boolean_skymap)[0]
        orders = np.full(ipix.shape, order)
        return MOC.from_healpix_cells(ipix, orders, order)

    @classmethod
    def _read_schema_from_metadata(cls, catalog_base_dir: str | Path | UPath) -> pa.Schema | None:
        """Reads the schema information stored in the _common_metadata or _metadata files."""
        common_metadata_file = paths.get_common_metadata_pointer(catalog_base_dir)
        common_metadata_exists = file_io.does_file_or_directory_exist(common_metadata_file)
        metadata_file = paths.get_parquet_metadata_pointer(catalog_base_dir)
        metadata_exists = file_io.does_file_or_directory_exist(metadata_file)
        if not (common_metadata_exists or metadata_exists):
            warnings.warn(
                "_common_metadata or _metadata files not found for this catalog."
                "The arrow schema will not be set."
            )
            return None
        schema_file = common_metadata_file if common_metadata_exists else metadata_file
        metadata = read_parquet_metadata(schema_file)
        return metadata.schema.to_arrow_schema()

    @classmethod
    def _check_files_exist(cls, catalog_base_dir: str | Path | UPath):
        super()._check_files_exist(catalog_base_dir)
        partition_info_file = paths.get_partition_info_pointer(catalog_base_dir)
        metadata_file = paths.get_parquet_metadata_pointer(catalog_base_dir)
        if not (
            file_io.does_file_or_directory_exist(partition_info_file)
            or file_io.does_file_or_directory_exist(metadata_file)
        ):
            raise FileNotFoundError(
                f"_metadata or partition info file is required in catalog directory {catalog_base_dir}"
            )

    def __len__(self):
        """The number of rows in the catalog.

        Returns:
            The number of rows in the catalog, as specified in its metadata.
            This value is undetermined when the catalog is modified, and
            therefore an error is raised.
        """
        if self.catalog_info.total_rows == 0:
            raise ValueError("The number of rows is undetermined because the catalog was modified.")
        return self.catalog_info.total_rows

    def get_max_coverage_order(self) -> int:
        """Gets the maximum HEALPix order for which the coverage of the catalog is known from the pixel
        tree and moc if it exists"""
        max_order = (
            max(self.moc.max_order, self.pixel_tree.get_max_depth())
            if self.moc is not None
            else self.pixel_tree.get_max_depth()
        )
        return max_order

    def filter_from_pixel_list(self, pixels: List[HealpixPixel]) -> Self:
        """Filter the pixels in the catalog to only include any that overlap with the requested pixels.

        Args:
            pixels (List[HealpixPixels]): the pixels to include

        Returns:
            A new catalog with only the pixels that overlap with the given pixels. Note that we reset the
            total_rows to None, as updating would require a scan over the new pixel sizes.
        """
        orders = np.array([p.order for p in pixels])
        pixel_inds = np.array([p.pixel for p in pixels])
        max_order = np.max(orders) if len(orders) > 0 else 0
        moc = MOC.from_healpix_cells(ipix=pixel_inds, depth=orders, max_depth=max_order)
        return self.filter_by_moc(moc)

    def filter_by_moc(self, moc: MOC) -> Self:
        """Filter the pixels in the catalog to only include the pixels that overlap with the moc provided.

        Args:
            moc (mocpy.MOC): the moc to filter by

        Returns:
            A new catalog with only the pixels that overlap with the moc. Note that we reset the total_rows
        to 0, as updating would require a scan over the new pixel sizes."""
        filtered_tree = filter_by_moc(self.pixel_tree, moc)
        filtered_moc = self.moc.intersection(moc) if self.moc is not None else None
        filtered_catalog_info = self.catalog_info.copy_and_update(total_rows=0)
        return self.__class__(filtered_catalog_info, filtered_tree, moc=filtered_moc, schema=self.schema)

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

        Args:
            other_cat (Catalog): The catalog to align to
            alignment_type (PixelAlignmentType): The type of alignment describing how to handle nodes which
            exist in one tree but not the other. Mirrors the 'how' argument of a pandas/sql join. Options are:

                - "inner" - only use pixels that appear in both catalogs
                - "left" - use all pixels that appear in the left catalog and any overlapping from the right
                - "right" - use all pixels that appear in the right catalog and any overlapping from the left
                - "outer" - use all pixels from both catalogs

        Returns (PixelAlignment):
            A `PixelAlignment` object with the alignment from the two catalogs
        """
        return align_with_mocs(
            self.pixel_tree, other_cat.pixel_tree, self.moc, other_cat.moc, alignment_type=alignment_type
        )

    def plot_pixels(self, projection: str = "MOL", **kwargs):
        return plot_pixel_list(
            self.get_healpix_pixels(),
            f"HATS HEALPix structure of {self.catalog_name}",
            projection,
            **kwargs,
        )
