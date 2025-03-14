"""Tests of file IO (reads and writes)"""

import shutil

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from hats.io import file_io, paths
from hats.io.parquet_metadata import aggregate_column_statistics, write_parquet_metadata


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


def test_aggregate_column_statistics(small_sky_order1_dir):
    partition_info_file = paths.get_parquet_metadata_pointer(small_sky_order1_dir)

    result_frame = aggregate_column_statistics(partition_info_file)
    assert len(result_frame) == 5

    result_frame = aggregate_column_statistics(partition_info_file, exclude_hats_columns=False)
    assert len(result_frame) == 9

    result_frame = aggregate_column_statistics(partition_info_file, include_columns=["ra", "dec"])
    assert len(result_frame) == 2

    result_frame = aggregate_column_statistics(partition_info_file, include_columns=["does", "not", "exist"])
    assert len(result_frame) == 0


def test_aggregate_column_statistics_with_nulls(tmp_path):
    file_io.make_directory(tmp_path / "dataset")

    metadata_filename = tmp_path / "dataset" / "dataframe_01.parquet"
    table_with_schema = pa.Table.from_arrays([[-1.0], [1.0]], names=["data", "Npix"])
    pq.write_table(table_with_schema, metadata_filename)

    icky_table = pa.Table.from_arrays([[2.0, None], [None, 6.0]], schema=table_with_schema.schema)
    metadata_filename = tmp_path / "dataset" / "dataframe_02.parquet"
    pq.write_table(icky_table, metadata_filename)

    icky_table = pa.Table.from_arrays([[None], [None]], schema=table_with_schema.schema)
    metadata_filename = tmp_path / "dataset" / "dataframe_00.parquet"
    pq.write_table(icky_table, metadata_filename)

    icky_table = pa.Table.from_arrays([[None, None], [None, None]], schema=table_with_schema.schema)
    metadata_filename = tmp_path / "dataset" / "dataframe_03.parquet"
    pq.write_table(icky_table, metadata_filename)

    assert write_parquet_metadata(tmp_path, order_by_healpix=False) == 6

    result_frame = aggregate_column_statistics(tmp_path / "dataset" / "_metadata", exclude_hats_columns=False)
    assert len(result_frame) == 2

    data_stats = result_frame.loc["data"]
    assert data_stats["min_value"] == -1
    assert data_stats["max_value"] == 2
    assert data_stats["null_count"] == 4

    data_stats = result_frame.loc["Npix"]
    assert data_stats["min_value"] == 1
    assert data_stats["max_value"] == 6
    assert data_stats["null_count"] == 4
