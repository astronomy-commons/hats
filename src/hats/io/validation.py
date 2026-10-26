from __future__ import annotations

import logging
import warnings
from pathlib import Path

import numpy as np
import pyarrow.dataset as pds
from nested_pandas.nestedframe.io import from_pyarrow
from upath import UPath

import hats.pixel_math.healpix_shim as hp
from hats.catalog.catalog import Catalog
from hats.catalog.catalog_collection import CatalogCollection
from hats.catalog.dataset.collection_properties import CollectionProperties
from hats.catalog.dataset.table_properties import TableProperties
from hats.catalog.healpix_dataset.healpix_dataset import HealpixDataset
from hats.catalog.index.index_catalog import IndexCatalog
from hats.catalog.margin_cache.margin_catalog import MarginCatalog
from hats.catalog.partition_info import PartitionInfo
from hats.io import file_io, paths
from hats.loaders import read_hats
from hats.pixel_math.healpix_pixel import INVALID_PIXEL
from hats.pixel_math.healpix_pixel_function import sort_pixels

# Allow f-strings here - this is all very logging-forward functionality.
# pylint: disable=logging-fstring-interpolation,too-many-branches


def is_valid_catalog(pointer: str | Path | UPath, strict: bool = False) -> bool:
    """Checks if a catalog is valid for a given base catalog pointer

    NB: This method uses logging to issue INFO, WARNING, and ERROR
    messages. Configure your logging environment to appropriately
    channel messages.

    Parameters
    ----------
    pointer : str | Path | UPath
        pointer to base catalog directory
    strict : bool
        should we perform additional checking that every optional
        file exists, and contains valid, consistent information, AS WELL AS
        all expected data partitions for the catalog's partitions.
        (Default value = False)

    Returns
    -------
    bool
        True if both the properties and partition_info files are valid, False otherwise
    """
    pointer = paths.get_upath(pointer)

    if is_collection_info_valid(pointer):
        logging.warning("Looking for catalog - found collection.")
        return is_valid_collection(pointer, strict=strict)

    if not strict:
        return _is_catalog_info_valid(pointer) and (
            _is_partition_info_valid(pointer) or _is_metadata_valid(pointer)
        )

    is_valid, _ = _is_valid_catalog_strict(pointer)
    return is_valid


def is_valid_collection(pointer: str | Path | UPath, strict: bool = False) -> bool:
    """Checks if a COLLECTION is valid for a given base catalog pointer

    NB: This method uses logging to issue INFO, WARNING, and ERROR
    messages. Configure your logging environment to appropriately
    channel messages.

    Parameters
    ----------
    pointer : str | Path | UPath
        pointer to base catalog collection directory
    strict : bool
        should we perform additional checking that every optional
        file exists, and contains valid, consistent information, AS WELL
        AS strict checking on all sub-catalogs (primary, margin, and index).
        (Default value = False)

    Returns
    -------
    bool
        True if the collection properties are valid, and all sub-catalogs pass
        validation.
    """
    pointer = paths.get_upath(pointer)
    if not is_collection_info_valid(pointer):
        return False
    if not strict:
        collection_properties = CollectionProperties.read_from_dir(pointer)
        return is_valid_catalog(
            CatalogCollection.resolve_inner_path(pointer, collection_properties.hats_primary_table_url)
        )

    # For catalog collections, we will confirm that all the member catalogs listed in the
    # collection properties exist and are valid, according to their expected types.

    logging.info(f"Validating collection at path {pointer} ... ")

    is_valid = True

    collection_properties = CollectionProperties.read_from_dir(pointer)
    subcatalog_valid, sub_catalog = _is_valid_catalog_strict(
        CatalogCollection.resolve_inner_path(pointer, collection_properties.hats_primary_table_url)
    )
    is_valid &= subcatalog_valid

    if sub_catalog and not isinstance(sub_catalog, Catalog):
        logging.warning(
            "Primary catalog is the wrong type (expected Catalog, "
            f"found {sub_catalog.catalog_info.catalog_type})."
        )
        is_valid = False

    if collection_properties.all_margins:
        for margin in collection_properties.all_margins:
            sub_catalog_path = CatalogCollection.resolve_inner_path(pointer, margin)
            subcatalog_valid, sub_catalog = _is_valid_catalog_strict(sub_catalog_path)
            is_valid &= subcatalog_valid

            if sub_catalog and not isinstance(sub_catalog, MarginCatalog):
                logging.warning(
                    "Margin catalog is the wrong type (expected margin, "
                    f"found {sub_catalog.catalog_info.catalog_type})."
                )
                is_valid = False

    if collection_properties.all_indexes:
        for index_field, index_dir in collection_properties.all_indexes.items():
            sub_catalog_path = CatalogCollection.resolve_inner_path(pointer, index_dir)
            subcatalog_valid, sub_catalog = _is_valid_catalog_strict(sub_catalog_path)
            is_valid &= subcatalog_valid

            if sub_catalog and not isinstance(sub_catalog, IndexCatalog):
                logging.warning(
                    f"Index catalog is the wrong type (expected index, "
                    f"found {sub_catalog.catalog_info.catalog_type})."
                )
                is_valid = False
            if sub_catalog and sub_catalog.catalog_info.indexing_column != index_field:
                logging.warning(
                    f"Index catalog index columns don't match (expected {index_field}, "
                    f"found {sub_catalog.catalog_info.indexing_column})."
                )
                is_valid = False
    return is_valid


# pylint: disable=too-many-statements
def _is_valid_catalog_strict(pointer):
    """Determine if this is a valid catalog, using strict criteria.

    If a catalog object can be loaded (even if it's not strictly valid),
    return it as well, for type-specific checks."""
    logging.info(f"Validating catalog at path {pointer} ... ")

    is_valid = True
    if not _is_catalog_info_valid(pointer):
        logging.warning("properties file does not exist or is invalid.")
        is_valid = False

    if not _is_common_metadata_valid(pointer):
        logging.warning("_common_metadata file does not exist.")
        is_valid = False

    if not is_valid:
        # Even if we're not failing fast, we need to stop here if the metadata
        # files don't exist.
        return (False, None)

    # Load as a catalog object. Confirms that the catalog info matches type.
    catalog = read_hats(pointer, single_catalog=True, read_moc=False)
    metadata_file = paths.get_parquet_metadata_pointer(pointer)

    if not isinstance(catalog, HealpixDataset):
        if not _is_metadata_valid(pointer):
            logging.warning("_metadata file does not exist.")
            return (False, catalog)

        ## Load as parquet dataset. Allow errors, and check pixel set against _metadata
        # As a side effect, this confirms that we can load the directory as a valid dataset.
        dataset = pds.parquet_dataset(metadata_file.path, filesystem=metadata_file.fs)

        return (is_valid, catalog)

    if not _is_partition_info_valid(pointer):
        logging.warning("partition_info.csv file does not exist.")
        return (False, catalog)

    expected_pixels = sort_pixels(catalog.get_healpix_pixels())

    logging.info(f"Found {len(expected_pixels)} partitions.")

    ## Compare the pixels in _metadata with partition_info.csv
    # We typically warn when reading from _metadata, but it's expected right now.
    if _is_metadata_valid(pointer):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            metadata_pixels = sort_pixels(PartitionInfo.read_from_file(metadata_file).get_healpix_pixels())
        if not np.array_equal(expected_pixels, metadata_pixels):
            logging.warning("Partition pixels differ between catalog and _metadata file")
            is_valid = False

        ## Load as parquet dataset. Allow errors, and check pixel set against _metadata
        # As a side effect, this confirms that we can load the directory as a valid dataset.
        dataset = pds.parquet_dataset(metadata_file.path, filesystem=metadata_file.fs)

        parquet_path_pixels = []
        for hats_file in dataset.files:
            hats_fp = UPath(hats_file, protocol=metadata_file.protocol, **metadata_file.storage_options)
            healpix_pixel = paths.get_healpix_from_path(hats_file)
            if healpix_pixel == INVALID_PIXEL:
                logging.warning(f"Could not derive partition pixel from parquet path: {str(hats_fp)}")
                is_valid = False
            parquet_path_pixels.append(healpix_pixel)

        parquet_path_pixels = sort_pixels(parquet_path_pixels)

        if not np.array_equal(expected_pixels, parquet_path_pixels):
            logging.warning(
                "Partition pixels differ between _metadata and partition_info\n"
                f"Extra: {set(parquet_path_pixels) - set(expected_pixels)} \n"
                f"Missing: {set(expected_pixels) - set(parquet_path_pixels)}"
            )
            is_valid = False
    else:
        logging.warning("_metadata file does not exist.")

    if len(catalog.get_healpix_pixels()) > 10_000:
        logging.info(
            f"Checking file existence for {len(catalog.get_healpix_pixels())} data partitions."
            " This might take a while."
        )
    parquet_path_pixels = []
    for pixel_path in pointer.rglob("Norder*/Dir*/Npix*"):
        healpix_pixel = paths.get_healpix_from_path(pixel_path)
        if healpix_pixel == INVALID_PIXEL:
            logging.warning(f"Could not derive partition pixel from parquet path: {str(pixel_path)}")
            is_valid = False
        parquet_path_pixels.append(healpix_pixel)

    parquet_path_pixels = sort_pixels(parquet_path_pixels)

    if not np.array_equal(expected_pixels, parquet_path_pixels):
        logging.warning(
            "Partition pixels differ between partition_info and parquet paths\n"
            f"Extra: {sorted(set(parquet_path_pixels) - set(expected_pixels))} \n"
            f"Missing: {sorted(set(expected_pixels) - set(parquet_path_pixels))}"
        )
        is_valid = False

    if catalog.schema:
        nested_schema = from_pyarrow(catalog.schema.empty_table())
        column_names = set(list(nested_schema.columns) + nested_schema.get_subcolumns())

        if catalog.catalog_info.default_columns:
            missing_columns = set(catalog.catalog_info.default_columns) - column_names
            if len(missing_columns):
                logging.warning(f"Some default columns not found in catalog: {missing_columns}")
                is_valid = False

        if catalog.catalog_info.ra_column and catalog.catalog_info.ra_column not in column_names:
            logging.warning(
                f"Right ascension coordinate column not found in catalog: {catalog.catalog_info.ra_column}"
            )
            is_valid = False

        if catalog.catalog_info.dec_column and catalog.catalog_info.dec_column not in column_names:
            logging.warning(
                f"Declination coordinate column not found in catalog: {catalog.catalog_info.dec_column}"
            )
            is_valid = False

        if catalog.catalog_info.healpix_column and catalog.catalog_info.healpix_column not in column_names:
            logging.warning(
                f"HEALPix spatial index column not found in catalog: {catalog.catalog_info.healpix_column}"
            )
            is_valid = False
    else:
        logging.warning("Could not load schema for catalog.")

    if catalog.catalog_info.skymap_order:
        intended_order = catalog.catalog_info.skymap_order
        if intended_order < 0:
            logging.warning(f"skymap_order cannot be negative ({intended_order}).")
            is_valid = False
        else:
            point_map_order = _get_point_map_order(pointer, None)
            if point_map_order is None:
                logging.warning("skymap.fits file not found")
            elif point_map_order != intended_order:
                logging.warning(
                    f"skymap_order in hat.properties ({intended_order}) "
                    f"does not match order inside skymap.fits file ({point_map_order})."
                )
                is_valid = False

    if catalog.catalog_info.skymap_alt_orders:
        available_orders = catalog.catalog_info.skymap_alt_orders
        for intended_order in available_orders:
            if intended_order < 0:
                logging.warning(f"skymap alternate order cannot be negative ({intended_order}).")
                is_valid = False
            else:
                point_map_order = _get_point_map_order(pointer, intended_order)
                if point_map_order is None:
                    logging.warning(f"skymap.{intended_order}.fits file not found")
                elif point_map_order != intended_order:
                    logging.warning(
                        f"skymap alternate order in hat.properties ({intended_order}) "
                        f"does not match order inside skymap.{intended_order}.fits file ({point_map_order})."
                    )
                    is_valid = False

    # Print a few more stats
    logging.info(
        "Approximate coverage is "
        f"{catalog.partition_info.calculate_fractional_coverage()*100:0.2f} % of the sky."
    )

    return (is_valid, catalog)


def _get_point_map_order(catalog_dir, intended_order):
    try:
        point_map = file_io.read_fits_image(
            paths.get_skymap_file_pointer(catalog_base_dir=catalog_dir, order=intended_order)
        )
    except (FileNotFoundError, ValueError, NotImplementedError):
        return None
    return hp.npix2order(len(point_map))


def _is_catalog_info_valid(pointer: str | Path | UPath) -> bool:
    """Checks if properties file is valid for a given base catalog pointer"""
    try:
        TableProperties.read_from_dir(pointer)
    except (FileNotFoundError, ValueError, NotImplementedError):
        return False
    return True


def is_collection_info_valid(pointer: str | Path | UPath) -> bool:
    """Checks if collection.properties file is valid for a given base catalog pointer"""
    try:
        CollectionProperties.read_from_dir(pointer)
    except (FileNotFoundError, ValueError, NotImplementedError):
        return False
    return True


def _is_partition_info_valid(pointer: UPath) -> bool:
    """Checks if partition_info is valid for a given base catalog pointer"""
    return paths.get_partition_info_pointer(pointer).exists()


def _is_metadata_valid(pointer: UPath) -> bool:
    """Checks if _metadata is valid for a given base catalog pointer"""
    return paths.get_parquet_metadata_pointer(pointer).exists()


def _is_common_metadata_valid(pointer: UPath) -> bool:
    """Checks if _common_metadata is valid for a given base catalog pointer"""
    return paths.get_common_metadata_pointer(pointer).exists()
