from __future__ import annotations

import warnings
from pathlib import Path

import numpy as np
import pyarrow.dataset as pds
from upath import UPath

from hats.catalog.catalog import Catalog
from hats.catalog.dataset.collection_properties import CollectionProperties
from hats.catalog.dataset.table_properties import TableProperties
from hats.catalog.healpix_dataset.healpix_dataset import HealpixDataset
from hats.catalog.index.index_catalog import IndexCatalog
from hats.catalog.margin_cache.margin_catalog import MarginCatalog
from hats.catalog.partition_info import PartitionInfo
from hats.io import get_common_metadata_pointer, get_parquet_metadata_pointer, get_partition_info_pointer
from hats.io.file_io import does_file_or_directory_exist, get_upath
from hats.io.file_io.file_pointer import is_regular_file
from hats.io.paths import get_healpix_from_path
from hats.loaders import read_hats
from hats.pixel_math.healpix_pixel import INVALID_PIXEL
from hats.pixel_math.healpix_pixel_function import sort_pixels


def is_valid_catalog(
    pointer: str | Path | UPath,
    strict: bool = False,
    fail_fast: bool = False,
    verbose: bool = True,
) -> bool:
    """Checks if a catalog is valid for a given base catalog pointer

    Parameters
    ----------
    pointer : str | Path | UPath
        pointer to base catalog directory
    strict : bool
        should we perform additional checking that every optional
        file exists, and contains valid, consistent information.
        (Default value = False)
    fail_fast : bool
        if performing strict checks, should we return at the first
        failure, or continue and find all problems?
        (Default value = False)
    verbose : bool
        if performing strict checks, should we print out counts,
        progress, and approximate sky coverage?
        (Default value = True)

    Returns
    -------
    bool
        True if both the properties and partition_info files are valid, False otherwise
    """
    pointer = get_upath(pointer)
    if not strict:
        return _is_catalog_info_valid(pointer) and (
            _is_partition_info_valid(pointer) or _is_metadata_valid(pointer)
        )

    def handle_error(msg):
        """inline-method to handle repeated logic of raising error or warning and
        continuing."""
        nonlocal fail_fast
        nonlocal verbose
        if fail_fast:
            raise ValueError(msg)
        if verbose:
            print(msg)
        else:
            warnings.warn(msg)

    (is_valid, _) = _is_valid_catalog_strict(pointer, handle_error, verbose)
    return is_valid


def is_valid_collection(
    pointer: str | Path | UPath,
    strict: bool = False,
    fail_fast: bool = False,
    verbose: bool = True,
) -> bool:
    """Checks if a COLLECTION is valid for a given base catalog pointer

    Parameters
    ----------
    pointer : str | Path | UPath
        pointer to base catalog collection directory
    strict : bool
        should we perform additional checking that every optional
        file exists, and contains valid, consistent information.
        (Default value = False)
    fail_fast : bool
        if performing strict checks, should we return at the first
        failure, or continue and find all problems?
        (Default value = False)
    verbose : bool
        if performing strict checks, should we print out counts,
        progress, and approximate sky coverage?
        (Default value = True)

    Returns
    -------
    bool
        True if the collection properties are valid, and all sub-catalogs pass
        validation.
    """
    pointer = get_upath(pointer)
    if not is_collection_info_valid(pointer):
        return False
    if not strict:
        collection_properties = CollectionProperties.read_from_dir(pointer)
        return is_valid_catalog(pointer / collection_properties.hats_primary_table_url)

    def handle_error(msg):
        """inline-method to handle repeated logic of raising error or warning and
        continuing."""
        nonlocal fail_fast
        nonlocal verbose
        if fail_fast:
            raise ValueError(msg)
        if verbose:
            print(msg)
        else:
            warnings.warn(msg)

    # For catalog collections, we will confirm that all the member catalogs listed in the
    # collection properties exist and are valid, according to their expected types.

    if verbose:
        print(f"Validating collection at path {pointer} ... ")

    is_valid = True

    collection_properties = CollectionProperties.read_from_dir(pointer)
    (subcatalog_valid, sub_catalog) = _is_valid_catalog_strict(
        pointer / collection_properties.hats_primary_table_url,
        handle_error,
        verbose,
    )
    is_valid &= subcatalog_valid

    if sub_catalog and not isinstance(sub_catalog, Catalog):
        handle_error(
            "Primary catalog is the wrong type (expected Catalog, "
            f"found {sub_catalog.catalog_info.catalog_type})."
        )
        is_valid = False

    if collection_properties.all_margins:
        for margin in collection_properties.all_margins:
            (subcatalog_valid, sub_catalog) = _is_valid_catalog_strict(
                pointer / margin,
                handle_error,
                verbose,
            )
            is_valid &= subcatalog_valid

            if sub_catalog and not isinstance(sub_catalog, MarginCatalog):
                handle_error(
                    "Margin catalog is the wrong type (expected margin, "
                    f"found {sub_catalog.catalog_info.catalog_type})."
                )
                is_valid = False

    if collection_properties.all_indexes:
        for index_field, index_dir in collection_properties.all_indexes.items():
            (subcatalog_valid, sub_catalog) = _is_valid_catalog_strict(
                pointer / index_dir, handle_error, verbose
            )
            is_valid &= subcatalog_valid

            if sub_catalog and not isinstance(sub_catalog, IndexCatalog):
                handle_error(
                    f"Index catalog is the wrong type (expected index, "
                    f"found {sub_catalog.catalog_info.catalog_type})."
                )
                is_valid = False
            if sub_catalog and sub_catalog.catalog_info.indexing_column != index_field:
                handle_error(
                    f"Index catalog index columns don't match (expected {index_field}, "
                    f"found {sub_catalog.catalog_info.indexing_column})."
                )
                is_valid = False
    return is_valid


def _is_valid_catalog_strict(pointer, handle_error, verbose):
    """Determine if this is a valid catalog, using strict criteria.

    If a catalog object can be loaded (even if it's not strictly valid),
    return it as well, for type-specific checks."""
    if verbose:
        print(f"Validating catalog at path {pointer} ... ")

    is_valid = True
    if not _is_catalog_info_valid(pointer):
        handle_error("properties file does not exist or is invalid.")
        is_valid = False

    if not _is_metadata_valid(pointer):
        handle_error("_metadata file does not exist.")
        is_valid = False

    if not _is_common_metadata_valid(pointer):
        handle_error("_common_metadata file does not exist.")
        is_valid = False

    if not is_valid:
        # Even if we're not failing fast, we need to stop here if the metadata
        # files don't exist.
        return (False, None)

    # Load as a catalog object. Confirms that the catalog info matches type.
    catalog = read_hats(pointer)
    metadata_file = get_parquet_metadata_pointer(pointer)

    if isinstance(catalog, HealpixDataset):
        if not _is_partition_info_valid(pointer):
            handle_error("partition_info.csv file does not exist.")
            return (False, catalog)

        expected_pixels = sort_pixels(catalog.get_healpix_pixels())

        if verbose:
            print(f"Found {len(expected_pixels)} partitions.")

        ## Compare the pixels in _metadata with partition_info.csv
        # We typically warn when reading from _metadata, but it's expected right now.
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            metadata_pixels = sort_pixels(PartitionInfo.read_from_file(metadata_file).get_healpix_pixels())
        if not np.array_equal(expected_pixels, metadata_pixels):
            handle_error("Partition pixels differ between catalog and _metadata file")
            is_valid = False

        ## Load as parquet dataset. Allow errors, and check pixel set against _metadata
        # As a side effect, this confirms that we can load the directory as a valid dataset.
        dataset = pds.parquet_dataset(metadata_file.path, filesystem=metadata_file.fs)

        parquet_path_pixels = []
        for hats_file in dataset.files:
            hats_fp = UPath(hats_file, protocol=metadata_file.protocol, **metadata_file.storage_options)
            if not does_file_or_directory_exist(hats_fp):
                handle_error(f"Pixel partition is missing: {hats_fp}")
                is_valid = False
            healpix_pixel = get_healpix_from_path(hats_file)
            if healpix_pixel == INVALID_PIXEL:
                handle_error(f"Could not derive partition pixel from parquet path: {str(hats_fp)}")
                is_valid = False
            parquet_path_pixels.append(healpix_pixel)

        parquet_path_pixels = sort_pixels(parquet_path_pixels)

        if not np.array_equal(expected_pixels, parquet_path_pixels):
            handle_error("Partition pixels differ between catalog and parquet paths")
            is_valid = False

        if verbose:
            # Print a few more stats
            print(
                "Approximate coverage is "
                f"{catalog.partition_info.calculate_fractional_coverage()*100:0.2f} % of the sky."
            )
    else:
        ## Load as parquet dataset. Allow errors, and check pixel set against _metadata
        # As a side effect, this confirms that we can load the directory as a valid dataset.
        dataset = pds.parquet_dataset(metadata_file.path, filesystem=metadata_file.fs)

    return (is_valid, catalog)


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
    partition_info_pointer = get_partition_info_pointer(pointer)
    partition_info_exists = is_regular_file(partition_info_pointer)
    return partition_info_exists


def _is_metadata_valid(pointer: UPath) -> bool:
    """Checks if _metadata is valid for a given base catalog pointer"""
    metadata_file = get_parquet_metadata_pointer(pointer)
    metadata_file_exists = is_regular_file(metadata_file)
    return metadata_file_exists


def _is_common_metadata_valid(pointer: UPath) -> bool:
    """Checks if _common_metadata is valid for a given base catalog pointer"""
    metadata_file = get_common_metadata_pointer(pointer)
    metadata_file_exists = is_regular_file(metadata_file)
    return metadata_file_exists
