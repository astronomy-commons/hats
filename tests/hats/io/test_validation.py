"""Test catalog information is valid"""

import logging
import os
import shutil

import pytest

from hats.catalog import PartitionInfo
from hats.io.validation import is_valid_catalog, is_valid_collection


def test_is_valid_catalog(tmp_path, small_sky_catalog):
    """Tests if the catalog_info and partition_info files are valid"""
    # An empty directory means an invalid catalog
    assert not is_valid_catalog(tmp_path)

    # Having the catalog_info file is not enough
    small_sky_catalog.catalog_info.to_properties_file(tmp_path)
    assert not is_valid_catalog(tmp_path)

    # The catalog is valid if both the catalog_info and _metadata files exist,
    # and the catalog_info is in a valid format
    (tmp_path / "dataset").mkdir()
    shutil.copy(small_sky_catalog.catalog_path / "dataset" / "_metadata", tmp_path / "dataset" / "_metadata")
    assert is_valid_catalog(tmp_path)

    # A partition_info file alone is also not enough
    os.remove(tmp_path / "properties")
    os.remove(tmp_path / "hats.properties")
    assert not is_valid_catalog(tmp_path)

    # The catalog_info file needs to be in the correct format
    small_sky_catalog.catalog_info.catalog_type = "invalid"
    small_sky_catalog.catalog_info.to_properties_file(tmp_path)
    assert not is_valid_catalog(tmp_path)


def test_is_valid_catalog_strict(tmp_path, small_sky_catalog, small_sky_pixels, small_sky_order1_pixels):
    """Tests if the catalog_info and partition_info files are valid"""
    # An empty directory means an invalid catalog
    assert not is_valid_catalog(tmp_path, strict=True)

    # Having the catalog_info file is not enough
    small_sky_catalog.catalog_info.to_properties_file(tmp_path)
    assert not is_valid_catalog(tmp_path, strict=True)

    # Adds the _metadata and _common_metadata, but that's not enough.
    (tmp_path / "dataset").mkdir()
    shutil.copy(small_sky_catalog.catalog_path / "dataset" / "_metadata", tmp_path / "dataset" / "_metadata")
    assert not is_valid_catalog(tmp_path, strict=True)

    # Adds the partition_info.csv, but that's STILL not enough
    PartitionInfo.from_healpix(small_sky_pixels).write_to_file(catalog_path=tmp_path)
    assert not is_valid_catalog(tmp_path, strict=True)

    # This outta do it! Add parquet files that match the _metadata pixels.
    shutil.copytree(small_sky_catalog.catalog_path / "dataset", tmp_path / "dataset", dirs_exist_ok=True)

    assert is_valid_catalog(tmp_path, strict=True)

    # Uh oh! Now the sets of pixels don't match between _metadata and partition_info.csv!
    PartitionInfo.from_healpix(small_sky_order1_pixels).write_to_file(catalog_path=tmp_path)
    assert not is_valid_catalog(tmp_path, strict=True)


def test_valid_catalog_strict_all(
    small_sky_source_dir,
    small_sky_order1_dir,
    small_sky_dir,
    small_sky_source_object_index_dir,
    margin_catalog_path,
):
    """Check that all of our object catalogs in test data are valid, using strict mechanism"""
    assert is_valid_catalog(small_sky_source_dir, strict=True)
    assert is_valid_catalog(small_sky_order1_dir, strict=True)
    assert is_valid_catalog(small_sky_dir, strict=True)
    assert is_valid_catalog(small_sky_source_object_index_dir, strict=True)
    assert is_valid_catalog(margin_catalog_path, strict=True)

    assert not is_valid_collection(small_sky_dir, strict=True)


def test_valid_catalog_all_basic(
    small_sky_source_dir,
    small_sky_order1_dir,
    small_sky_dir,
    small_sky_source_object_index_dir,
    margin_catalog_path,
):
    """Check that all of our object catalogs in test data are valid"""
    assert is_valid_catalog(small_sky_source_dir)
    assert is_valid_catalog(small_sky_order1_dir)
    assert is_valid_catalog(small_sky_dir)
    assert is_valid_catalog(small_sky_source_object_index_dir)
    assert is_valid_catalog(margin_catalog_path)

    assert not is_valid_collection(small_sky_dir)


def test_is_valid_catalog_fail_with_missing_partitions(small_sky_source_dir, tmp_path, caplog):
    """Test that if some files are missing an error is raised"""
    # copy all partitions but two
    shutil.copytree(
        small_sky_source_dir, tmp_path / "copy", ignore=lambda _, f: ["Npix=4.parquet", "Npix=176.parquet"]
    )
    assert not is_valid_catalog(tmp_path / "copy", strict=True)

    captured = caplog.text
    assert "Partition pixels differ" in captured


def test_is_valid_catalog_fail_index_missing_metadata(small_sky_source_object_index_dir, tmp_path):
    shutil.copytree(small_sky_source_object_index_dir, tmp_path / "copy", ignore=lambda _, f: ["_metadata"])
    assert not is_valid_catalog(tmp_path / "copy", strict=True)


def test_is_valid_catalog_warn_missing_metadata(small_sky_source_dir, tmp_path, caplog):
    """Test that if some files are missing an error is raised"""
    # copy all files, but the `_metadata`
    shutil.copytree(small_sky_source_dir, tmp_path / "copy", ignore=lambda _, f: ["_metadata"])
    assert is_valid_catalog(tmp_path / "copy", strict=True)

    captured = caplog.text
    assert "_metadata file does not exist" in captured


def test_is_valid_catalog_fail_with_missing_partition_info(small_sky_source_dir, tmp_path, caplog):
    """Test that if some files are missing an error is raised"""
    # copy everything but the partition info file.
    shutil.copytree(small_sky_source_dir, tmp_path / "copy", ignore=lambda _, f: ["partition_info.csv"])
    with pytest.warns():
        # UserWarning from reading _metadata for pixels
        assert not is_valid_catalog(tmp_path / "copy", strict=True)

    captured = caplog.text
    assert "partition_info.csv file does not exist" in captured


def test_valid_collection(small_sky_collection_dir):
    """Check that all of our object catalogs in test data are valid"""
    assert is_valid_collection(small_sky_collection_dir)
    assert is_valid_collection(small_sky_collection_dir, strict=True)
    assert is_valid_catalog(small_sky_collection_dir)
    assert is_valid_catalog(small_sky_collection_dir, strict=True)


def test_is_valid_collection_fail_with_missing_primary_lax(tmp_path):
    properties = """#HATS Collection
obs_collection=small_sky_o1_collection
hats_primary_table_url=small_sky_order1
"""
    with (tmp_path / "collection.properties").open("w", encoding="utf-8") as file:
        file.write(properties)

    assert not is_valid_collection(tmp_path)

    properties = """#HATS Collection
obs_collection=small_sky_o1_collection
"""
    with (tmp_path / "collection.properties").open("w", encoding="utf-8") as file:
        file.write(properties)

    assert not is_valid_collection(tmp_path)


def test_is_valid_collection_fail_with_missing_strict(tmp_path):
    properties = """#HATS Collection
obs_collection=small_sky_o1_collection
hats_primary_table_url=small_sky_order1
all_margins=small_sky_order1
default_margin=small_sky_order1
all_indexes=foo small_sky_order1
"""
    with (tmp_path / "collection.properties").open("w", encoding="utf-8") as file:
        file.write(properties)

    assert not is_valid_collection(tmp_path, strict=True)

    properties = """#HATS Collection
obs_collection=small_sky_o1_collection
"""
    with (tmp_path / "collection.properties").open("w", encoding="utf-8") as file:
        file.write(properties)

    assert not is_valid_collection(tmp_path, strict=True)


def test_is_valid_collection_fail_with_wrong_types(tmp_path, small_sky_collection_dir, caplog):
    """Using a totally valid copy of the small sky collection, modify the
    `collection.properties` file to point to the wrong types of catalogs."""
    caplog.at_level(logging.INFO)

    shutil.copytree(small_sky_collection_dir, tmp_path / "copy")
    assert is_valid_collection(tmp_path / "copy", strict=True)

    catalog_relative = "small_sky_order1"
    margin_relative = "small_sky_order1_margin"
    index_relative = "small_sky_order1_id_index"
    properties = f"""#HATS Collection
obs_collection=small_sky_o1_collection
hats_primary_table_url={catalog_relative}
all_margins={margin_relative}
default_margin={margin_relative}
all_indexes=id {index_relative}
"""
    with (tmp_path / "copy" / "collection.properties").open("w", encoding="utf-8") as file:
        file.write(properties)
    assert is_valid_collection(tmp_path / "copy", strict=True)

    properties = f"""#HATS Collection
obs_collection=small_sky_o1_collection
hats_primary_table_url={margin_relative}
all_margins={index_relative}
default_margin={index_relative}
all_indexes=foo {catalog_relative}
"""
    with (tmp_path / "copy" / "collection.properties").open("w", encoding="utf-8") as file:
        file.write(properties)
    caplog.clear()
    caplog.at_level(logging.INFO)
    assert not is_valid_collection(tmp_path / "copy", strict=True)
    captured = caplog.text

    assert "Validating collection at path" in captured
    assert "Primary catalog is the wrong type (expected Catalog, found margin)" in captured
    assert "Margin catalog is the wrong type (expected margin, found index)" in captured
    assert "Index catalog is the wrong type (expected index, found object)" in captured
    assert "Index catalog index columns don't match (expected foo, found None)" in captured
