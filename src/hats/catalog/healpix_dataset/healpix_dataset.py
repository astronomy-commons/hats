from __future__ import annotations

from pathlib import Path
from typing import List, Tuple, Union

import astropy.units as u
import numpy as np
import pandas as pd
import pyarrow as pa
from astropy.coordinates import SkyCoord
from mocpy import MOC
from typing_extensions import Self
from upath import UPath

from hats.catalog.dataset import Dataset
from hats.catalog.dataset.table_properties import TableProperties
from hats.catalog.partition_info import PartitionInfo
from hats.inspection import plot_pixels
from hats.inspection.visualize_catalog import plot_moc
from hats.pixel_math import HealpixPixel
from hats.pixel_math.box_filter import generate_box_moc, wrap_ra_angles
from hats.pixel_math.validators import (
    validate_box,
    validate_declination_values,
    validate_polygon,
    validate_radius,
)
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
        super().__init__(catalog_info, catalog_path=catalog_path, schema=schema)
        self.partition_info = self._get_partition_info_from_pixels(pixels)
        self.pixel_tree = self._get_pixel_tree_from_pixels(pixels)
        self.moc = moc

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
        if len(self.pixel_tree) == 0:
            raise ValueError("Cannot get max_order of empty catalog")
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

    def filter_by_cone(self, ra: float, dec: float, radius_arcsec: float) -> Self:
        """Filter the pixels in the catalog to only include the pixels that overlap with a cone

        Args:
            ra (float): Right ascension of the center of the cone, in degrees
            dec (float): Declination of the center of the cone, in degrees
            radius_arcsec (float): Radius of the cone, in arcseconds

        Returns:
            A new catalog with only the pixels that overlap with the specified cone
        """
        validate_radius(radius_arcsec)
        validate_declination_values(dec)
        cone_moc = MOC.from_cone(
            lon=ra * u.deg,
            lat=dec * u.deg,
            radius=radius_arcsec * u.arcsec,
            max_depth=self.get_max_coverage_order(),
        )
        return self.filter_by_moc(cone_moc)

    def filter_by_box(self, ra: Tuple[float, float], dec: Tuple[float, float]) -> Self:
        """Filter the pixels in the catalog to only include the pixels that overlap with a
        zone, defined by right ascension and declination ranges. The right ascension edges follow
        great arc circles and the declination edges follow small arc circles.

        Args:
            ra (Tuple[float, float]): Right ascension range, in degrees
            dec (Tuple[float, float]): Declination range, in degrees

        Returns:
            A new catalog with only the pixels that overlap with the specified region
        """
        ra = tuple(wrap_ra_angles(ra)) if ra else None
        validate_box(ra, dec)
        box_moc = generate_box_moc(ra, dec, self.get_max_coverage_order())
        return self.filter_by_moc(box_moc)

    def filter_by_polygon(self, vertices: list[tuple[float, float]]) -> Self:
        """Filter the pixels in the catalog to only include the pixels that overlap
        with a polygonal sky region.

        Args:
            vertices (list[tuple[float,float]]): The list of vertice coordinates for
                the polygon, (ra, dec), in degrees.

        Returns:
            A new catalog with only the pixels that overlap with the specified polygon.
        """
        validate_polygon(vertices)
        polygon_moc = MOC.from_polygon_skycoord(
            SkyCoord(vertices, unit="deg"), max_depth=self.get_max_coverage_order()
        )
        return self.filter_by_moc(polygon_moc)

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

    def plot_pixels(self, **kwargs):
        """Create a visual map of the pixel density of the catalog.

        Args:
            kwargs: Additional args to pass to `hats.inspection.visualize_catalog.plot_healpix_map`
        """
        return plot_pixels(self, **kwargs)

    def plot_moc(self, **kwargs):
        """Create a visual map of the coverage of the catalog.

        Args:
            kwargs: Additional args to pass to `hats.inspection.visualize_catalog.plot_moc`
        """
        default_title = f"Coverage MOC of {self.catalog_name}"
        plot_args = {"title": default_title}
        plot_args.update(kwargs)
        return plot_moc(self.moc, **plot_args)
