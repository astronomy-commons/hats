from __future__ import annotations

import warnings
from pathlib import Path

import numpy as np
import pyarrow as pa
from mocpy import MOC
from upath import UPath

import hats.pixel_math.healpix_shim as hp
from hats.catalog import AssociationCatalog, Catalog, CatalogType, Dataset, MapCatalog, MarginCatalog
from hats.catalog.association_catalog.partition_join_info import PartitionJoinInfo
from hats.catalog.dataset.table_properties import TableProperties
from hats.catalog.index.index_catalog import IndexCatalog
from hats.catalog.partition_info import PartitionInfo
from hats.io import file_io, paths
from hats.io.file_io import read_parquet_metadata

DATASET_TYPE_TO_CLASS = {
    CatalogType.OBJECT: Catalog,
    CatalogType.SOURCE: Catalog,
    CatalogType.ASSOCIATION: AssociationCatalog,
    CatalogType.INDEX: IndexCatalog,
    CatalogType.MARGIN: MarginCatalog,
    CatalogType.MAP: MapCatalog,
}


def read_hats(catalog_path: str | Path | UPath) -> Dataset:
    """Reads a HATS Catalog from a HATS directory

    Args:
        catalog_path (str): path to the root directory of the catalog
    Returns:
        The initialized catalog object
    """
    catalog_path = file_io.get_upath(catalog_path)
    try:
        properties = TableProperties.read_from_dir(catalog_path)
        dataset_type = properties.catalog_type
        if dataset_type not in DATASET_TYPE_TO_CLASS:
            raise NotImplementedError(f"Cannot load catalog of type {dataset_type}")

        loader = DATASET_TYPE_TO_CLASS[dataset_type]
        kwargs = {
            "catalog_path": catalog_path,
            "catalog_info": properties,
            "schema": _read_schema_from_metadata(catalog_path),
        }
        if _is_healpix_dataset(dataset_type):
            kwargs["pixels"] = PartitionInfo.read_from_dir(catalog_path)
            kwargs["moc"] = _read_moc_from_point_map(catalog_path)
        if dataset_type == CatalogType.ASSOCIATION:
            kwargs["join_pixels"] = PartitionJoinInfo.read_from_dir(catalog_path)
        return loader(**kwargs)
    except Exception as exception:  # pylint: disable=broad-except
        raise FileNotFoundError(f"Failed to read HATS at location {catalog_path}") from exception


def _is_healpix_dataset(dataset_type):
    return dataset_type in (
        CatalogType.OBJECT,
        CatalogType.SOURCE,
        CatalogType.ASSOCIATION,
        CatalogType.MARGIN,
        CatalogType.MAP,
    )


def _read_moc_from_point_map(catalog_base_dir: str | Path | UPath) -> MOC | None:
    """Reads a MOC object from the `point_map.fits` file if it exists in the catalog directory"""
    point_map_path = paths.get_point_map_file_pointer(catalog_base_dir)
    if not file_io.does_file_or_directory_exist(point_map_path):
        return None
    fits_image = file_io.read_fits_image(point_map_path)
    order = hp.npix2order(len(fits_image))
    boolean_skymap = fits_image.astype(bool)
    ipix = np.where(boolean_skymap)[0]
    orders = np.full(ipix.shape, order)
    return MOC.from_healpix_cells(ipix, orders, order)


def _read_schema_from_metadata(catalog_base_dir: str | Path | UPath) -> pa.Schema | None:
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
