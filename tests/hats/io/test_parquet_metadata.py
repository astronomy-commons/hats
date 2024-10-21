"""Tests of file IO (reads and writes)"""

import shutil

import pandas as pd
import pyarrow as pa
import pytest

from hats.io import file_io, paths
from hats.io.parquet_metadata import (
    aggregate_column_statistics,
    get_healpix_pixel_from_metadata,
    read_row_group_fragments,
    row_group_stat_single_value,
    write_parquet_metadata,
)
from hats.pixel_math.healpix_pixel import HealpixPixel


def test_write_parquet_metadata(tmp_path, small_sky_dir, small_sky_schema, check_parquet_schema):
    """Copy existing catalog and create new metadata files for it"""
    catalog_base_dir = tmp_path / "catalog"
    shutil.copytree(
        small_sky_dir,
        catalog_base_dir,
    )
    total_rows = write_parquet_metadata(catalog_base_dir)
    assert total_rows == 131
    check_parquet_schema(catalog_base_dir / "dataset" / "_metadata", small_sky_schema)
    ## _common_metadata has 0 row groups
    check_parquet_schema(
        catalog_base_dir / "dataset" / "_common_metadata",
        small_sky_schema,
        0,
    )
    ## Re-write - should still have the same properties.
    total_rows = write_parquet_metadata(catalog_base_dir)
    assert total_rows == 131
    check_parquet_schema(catalog_base_dir / "dataset" / "_metadata", small_sky_schema)
    ## _common_metadata has 0 row groups
    check_parquet_schema(
        catalog_base_dir / "dataset" / "_common_metadata",
        small_sky_schema,
        0,
    )


def test_write_parquet_metadata_order1(
    tmp_path, small_sky_order1_dir, small_sky_schema, check_parquet_schema
):
    """Copy existing catalog and create new metadata files for it,
    using a catalog with multiple files."""
    temp_path = tmp_path / "catalog"
    shutil.copytree(
        small_sky_order1_dir,
        temp_path,
    )

    total_rows = write_parquet_metadata(temp_path)
    assert total_rows == 131
    ## 4 row groups for 4 partitioned parquet files
    check_parquet_schema(
        temp_path / "dataset" / "_metadata",
        small_sky_schema,
        4,
    )
    ## _common_metadata has 0 row groups
    check_parquet_schema(
        temp_path / "dataset" / "_common_metadata",
        small_sky_schema,
        0,
    )


def test_write_parquet_metadata_sorted(
    tmp_path, small_sky_order1_dir, small_sky_schema, check_parquet_schema
):
    """Copy existing catalog and create new metadata files for it,
    using a catalog with multiple files."""
    temp_path = tmp_path / "catalog"
    shutil.copytree(
        small_sky_order1_dir,
        temp_path,
    )

    total_rows = write_parquet_metadata(temp_path)
    assert total_rows == 131
    ## 4 row groups for 4 partitioned parquet files
    check_parquet_schema(
        temp_path / "dataset" / "_metadata",
        small_sky_schema,
        4,
    )
    ## _common_metadata has 0 row groups
    check_parquet_schema(
        temp_path / "dataset" / "_common_metadata",
        small_sky_schema,
        0,
    )


def test_write_index_parquet_metadata(tmp_path, check_parquet_schema):
    """Create an index-like catalog, and test metadata creation."""
    temp_path = tmp_path / "index"

    index_parquet_path = temp_path / "dataset" / "Parts=0" / "part_000_of_001.parquet"
    file_io.make_directory(temp_path / "dataset" / "Parts=0")
    basic_index = pd.DataFrame({"_healpix_29": [4000, 4001], "ps1_objid": [700, 800]})
    file_io.write_dataframe_to_parquet(basic_index, index_parquet_path)

    index_catalog_parquet_metadata = pa.schema(
        [
            pa.field("_healpix_29", pa.int64()),
            pa.field("ps1_objid", pa.int64()),
        ]
    )

    total_rows = write_parquet_metadata(temp_path, order_by_healpix=False)
    assert total_rows == 2

    check_parquet_schema(tmp_path / "index" / "dataset" / "_metadata", index_catalog_parquet_metadata)
    ## _common_metadata has 0 row groups
    check_parquet_schema(
        tmp_path / "index" / "dataset" / "_common_metadata",
        index_catalog_parquet_metadata,
        0,
    )


def test_row_group_fragments(small_sky_order1_dir):
    partition_info_file = paths.get_parquet_metadata_pointer(small_sky_order1_dir)

    num_row_groups = 0
    for _ in read_row_group_fragments(partition_info_file):
        num_row_groups += 1

    assert num_row_groups == 4


def test_row_group_fragments_with_dir(small_sky_order1_dir):
    num_row_groups = 0
    for _ in read_row_group_fragments(small_sky_order1_dir):
        num_row_groups += 1

    assert num_row_groups == 4


def test_aggregate_column_statistics(small_sky_order1_dir):
    partition_info_file = paths.get_parquet_metadata_pointer(small_sky_order1_dir)

    result_frame = aggregate_column_statistics(partition_info_file)
    assert len(result_frame) == 5

    result_frame = aggregate_column_statistics(partition_info_file, exclude_hats_columns=False)
    assert len(result_frame) == 9

    result_frame = aggregate_column_statistics(partition_info_file, include_columns=["ra", "dec"])
    assert len(result_frame) == 2


def test_row_group_stats(small_sky_dir):
    partition_info_file = paths.get_parquet_metadata_pointer(small_sky_dir)
    first_row_group = next(read_row_group_fragments(partition_info_file))

    assert row_group_stat_single_value(first_row_group, "Norder") == 0
    assert row_group_stat_single_value(first_row_group, "Npix") == 11

    with pytest.raises(ValueError, match="doesn't have expected key"):
        row_group_stat_single_value(first_row_group, "NOT HERE")

    with pytest.raises(ValueError, match="stat min != max"):
        row_group_stat_single_value(first_row_group, "ra")


def test_get_healpix_pixel_from_metadata(small_sky_dir):
    partition_info_file = paths.get_parquet_metadata_pointer(small_sky_dir)
    single_metadata = file_io.read_parquet_metadata(partition_info_file)
    pixel = get_healpix_pixel_from_metadata(single_metadata)
    assert pixel == HealpixPixel(0, 11)


def test_get_healpix_pixel_from_metadata_min_max(tmp_path):
    good_healpix_dataframe = pd.DataFrame({"data": [0, 1], "Norder": [1, 1], "Npix": [44, 44]})
    metadata_filename = tmp_path / "non_healpix_metadata.parquet"
    good_healpix_dataframe.to_parquet(metadata_filename)
    single_metadata = file_io.read_parquet_metadata(metadata_filename)
    pixel = get_healpix_pixel_from_metadata(single_metadata)
    assert pixel == HealpixPixel(1, 44)

    non_healpix_dataframe = pd.DataFrame({"data": [0, 1], "Npix": [45, 44]})
    non_healpix_dataframe.to_parquet(metadata_filename)
    single_metadata = file_io.read_parquet_metadata(metadata_filename)
    with pytest.raises(ValueError, match="Npix stat min != max"):
        get_healpix_pixel_from_metadata(single_metadata)

    non_healpix_dataframe = pd.DataFrame({"data": [0, 1], "Norder": [5, 6]})
    non_healpix_dataframe.to_parquet(metadata_filename)
    single_metadata = file_io.read_parquet_metadata(metadata_filename)
    with pytest.raises(ValueError, match="Norder stat min != max"):
        get_healpix_pixel_from_metadata(single_metadata)


def test_get_healpix_pixel_from_metadata_fail(tmp_path):
    empty_dataframe = pd.DataFrame()
    metadata_filename = tmp_path / "empty_metadata.parquet"
    empty_dataframe.to_parquet(metadata_filename)
    single_metadata = file_io.read_parquet_metadata(metadata_filename)
    with pytest.raises(ValueError, match="empty table"):
        get_healpix_pixel_from_metadata(single_metadata)

    non_healpix_dataframe = pd.DataFrame({"data": [0], "Npix": [45]})
    metadata_filename = tmp_path / "non_healpix_metadata.parquet"
    non_healpix_dataframe.to_parquet(metadata_filename)
    single_metadata = file_io.read_parquet_metadata(metadata_filename)
    with pytest.raises(ValueError, match="missing Norder"):
        get_healpix_pixel_from_metadata(single_metadata)


def test_get_healpix_pixel_from_metadata_columns(tmp_path):
    """Test fetching the healpix pixel from columns with non-default names."""
    non_healpix_dataframe = pd.DataFrame({"data": [1], "Npix": [45], "join_Norder": [2], "join_Npix": [3]})
    metadata_filename = tmp_path / "non_healpix_metadata.parquet"
    non_healpix_dataframe.to_parquet(metadata_filename)
    single_metadata = file_io.read_parquet_metadata(metadata_filename)
    with pytest.raises(ValueError, match="missing Norder"):
        get_healpix_pixel_from_metadata(single_metadata)

    pixel = get_healpix_pixel_from_metadata(single_metadata, norder_column="data")
    assert pixel == HealpixPixel(1, 45)

    pixel = get_healpix_pixel_from_metadata(
        single_metadata, norder_column="join_Norder", npix_column="join_Npix"
    )
    assert pixel == HealpixPixel(2, 3)

    ## People can do silly things!
    pixel = get_healpix_pixel_from_metadata(single_metadata, norder_column="data", npix_column="join_Npix")
    assert pixel == HealpixPixel(1, 3)
