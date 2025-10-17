"""Tests of file IO (reads and writes)"""

import shutil

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pandas.api.types import is_numeric_dtype
from pyarrow.parquet import ParquetFile

from hats.io import file_io, paths
from hats.io.parquet_metadata import aggregate_column_statistics, per_pixel_statistics, write_parquet_metadata
from hats.pixel_math.healpix_pixel import HealpixPixel
from hats.pixel_math.spatial_index import SPATIAL_INDEX_COLUMN


def test_write_parquet_metadata(tmp_path, small_sky_dir, small_sky_schema, check_parquet_schema):
    """Copy existing catalog and create new metadata files for it"""
    catalog_base_dir = tmp_path / "catalog"
    shutil.copytree(
        small_sky_dir,
        catalog_base_dir,
    )

    total_rows = write_parquet_metadata(catalog_base_dir, create_thumbnail=True)
    assert total_rows == 131
    check_parquet_schema(catalog_base_dir / "dataset" / "_metadata", small_sky_schema)
    ## _common_metadata has 0 row groups
    check_parquet_schema(
        catalog_base_dir / "dataset" / "_common_metadata",
        small_sky_schema,
        0,
    )
    assert (catalog_base_dir / "dataset" / "data_thumbnail.parquet").exists()

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
    total_rows = write_parquet_metadata(temp_path, create_thumbnail=True)
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
    ## the data thumbnail has 1 row group and a total of 4 rows
    ## corresponding to the number of partitions
    data_thumbnail_path = temp_path / "dataset" / "data_thumbnail.parquet"
    assert data_thumbnail_path.exists()
    thumbnail = ParquetFile(data_thumbnail_path)
    data_thumbnail = thumbnail.read()
    assert len(data_thumbnail) == 4
    assert thumbnail.metadata.num_row_groups == 1
    assert data_thumbnail.schema.equals(small_sky_schema)
    assert data_thumbnail.equals(data_thumbnail.sort_by(SPATIAL_INDEX_COLUMN))


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
    ## Sneak in a test for the data thumbnail generation, specifying a
    ## thumbnail threshold that is smaller than the number of partitions
    total_rows = write_parquet_metadata(temp_path, create_thumbnail=True, thumbnail_threshold=2)
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
    ## the data thumbnail has 1 row group and a total of 2 rows
    ## because that is what the pixel threshold specified
    data_thumbnail_path = temp_path / "dataset" / "data_thumbnail.parquet"
    assert data_thumbnail_path.exists()
    thumbnail = ParquetFile(data_thumbnail_path)
    data_thumbnail = thumbnail.read()
    assert len(data_thumbnail) == 2
    assert thumbnail.metadata.num_row_groups == 1
    assert data_thumbnail.schema.equals(small_sky_schema)
    assert data_thumbnail.equals(data_thumbnail.sort_by(SPATIAL_INDEX_COLUMN))


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
    assert len(result_frame) == 6

    result_frame = aggregate_column_statistics(partition_info_file, include_columns=["ra", "dec"])
    assert len(result_frame) == 2

    result_frame = aggregate_column_statistics(partition_info_file, include_columns=["does", "not", "exist"])
    assert len(result_frame) == 0


def assert_column_stat_as_floats(
    result_frame, column_name, min_value=None, max_value=None, null_count=0, row_count=None
):
    assert column_name in result_frame.index
    data_stats = result_frame.loc[column_name]
    assert float(data_stats["min_value"]) >= min_value
    assert float(data_stats["max_value"]) <= max_value
    assert data_stats["null_count"] == null_count
    assert data_stats["row_count"] == row_count


def test_aggregate_column_statistics_with_pixel(small_sky_order1_dir):
    partition_info_file = paths.get_parquet_metadata_pointer(small_sky_order1_dir)

    result_frame = aggregate_column_statistics(partition_info_file)
    assert len(result_frame) == 5
    assert_column_stat_as_floats(result_frame, "dec", min_value=-69.5, max_value=-25.5, row_count=131)

    result_frame = aggregate_column_statistics(partition_info_file, include_pixels=[HealpixPixel(1, 45)])
    assert len(result_frame) == 5
    assert_column_stat_as_floats(result_frame, "dec", min_value=-60.5, max_value=-25.5, row_count=29)

    result_frame = aggregate_column_statistics(partition_info_file, include_pixels=[HealpixPixel(1, 47)])
    assert len(result_frame) == 5
    assert_column_stat_as_floats(result_frame, "dec", min_value=-36.5, max_value=-25.5, row_count=18)

    result_frame = aggregate_column_statistics(
        partition_info_file, include_pixels=[HealpixPixel(1, 45), HealpixPixel(1, 47)]
    )
    assert len(result_frame) == 5
    assert_column_stat_as_floats(result_frame, "dec", min_value=-60.5, max_value=-25.5, row_count=47)

    result_frame = aggregate_column_statistics(partition_info_file, include_pixels=[HealpixPixel(1, 4)])
    assert len(result_frame) == 0


def test_aggregate_column_statistics_with_rowgroups(small_sky_source_dir):
    partition_info_file = paths.get_parquet_metadata_pointer(small_sky_source_dir)

    result_frame = aggregate_column_statistics(partition_info_file)
    assert len(result_frame) == 9
    assert_column_stat_as_floats(
        result_frame, "object_dec", min_value=-69.5, max_value=-25.5, row_count=17161
    )

    result_frame = aggregate_column_statistics(partition_info_file, include_pixels=[HealpixPixel(1, 47)])
    assert len(result_frame) == 9
    assert_column_stat_as_floats(result_frame, "object_dec", min_value=-36.5, max_value=-25.5, row_count=2395)

    result_frame = aggregate_column_statistics(
        partition_info_file, include_pixels=[HealpixPixel(1, 45), HealpixPixel(1, 47)]
    )
    assert len(result_frame) == 9
    assert_column_stat_as_floats(result_frame, "object_dec", min_value=-60.5, max_value=-25.5, row_count=2395)

    result_frame = aggregate_column_statistics(partition_info_file, include_pixels=[HealpixPixel(1, 4)])
    assert len(result_frame) == 0


def test_aggregate_column_statistics_with_nested(small_sky_nested_dir):
    partition_info_file = paths.get_parquet_metadata_pointer(small_sky_nested_dir)

    ## Will have 13 returned columns (5 object and 8 light curve)
    ## Since object_dec is copied from object.dec, the min/max are the same,
    ## but there are MANY more rows of light curve dec values.
    result_frame = aggregate_column_statistics(partition_info_file)
    assert len(result_frame) == 13
    assert_column_stat_as_floats(result_frame, "dec", min_value=-69.5, max_value=-25.5, row_count=131)
    assert_column_stat_as_floats(
        result_frame, "lc.object_dec", min_value=-69.5, max_value=-25.5, row_count=16135
    )

    ## Only peeking at a single pixel, we should see the same dec min/max as
    ## we see above for the flat object table.
    result_frame = aggregate_column_statistics(partition_info_file, include_pixels=[HealpixPixel(1, 47)])
    assert len(result_frame) == 13
    assert_column_stat_as_floats(result_frame, "dec", min_value=-36.5, max_value=-25.5, row_count=18)
    assert_column_stat_as_floats(
        result_frame, "lc.source_id", min_value=70008, max_value=87148, row_count=2358
    )
    assert_column_stat_as_floats(result_frame, "lc.mag", min_value=15, max_value=21, row_count=2358)

    ## Test that we can request light curve columns, using the shorter name
    ## e.g. full path in the file is "lc.source_id.list.element"
    result_frame = aggregate_column_statistics(
        partition_info_file, include_columns=["ra", "dec", "lc.source_ra", "lc.source_dec", "lc.mag"]
    )
    assert len(result_frame) == 5
    assert_column_stat_as_floats(result_frame, "dec", min_value=-69.5, max_value=-25.5, row_count=131)
    assert_column_stat_as_floats(result_frame, "lc.mag", min_value=15, max_value=21, row_count=16135)


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

    assert_column_stat_as_floats(result_frame, "data", min_value=-1, max_value=2, null_count=4, row_count=6)
    assert_column_stat_as_floats(result_frame, "Npix", min_value=1, max_value=6, null_count=4, row_count=6)


def test_per_pixel_statistics(small_sky_order1_dir):
    partition_info_file = paths.get_parquet_metadata_pointer(small_sky_order1_dir)

    result_frame = per_pixel_statistics(partition_info_file)
    # 30 = 5 columns * 6 stats per-column
    assert result_frame.shape == (4, 30)

    result_frame = per_pixel_statistics(partition_info_file, exclude_hats_columns=False)
    # 36 = 6 columns * 6 stats per-column
    assert result_frame.shape == (4, 36)

    result_frame = per_pixel_statistics(partition_info_file, include_columns=["ra", "dec"])
    # 12 = 2 columns * 6 stats per-column
    assert result_frame.shape == (4, 12)

    result_frame = per_pixel_statistics(partition_info_file, include_columns=["does", "not", "exist"])
    assert len(result_frame) == 0


def test_per_pixel_statistics_multi_index(small_sky_order1_dir):
    partition_info_file = paths.get_parquet_metadata_pointer(small_sky_order1_dir)

    result_frame = per_pixel_statistics(partition_info_file, multi_index=True)
    # 20 = 5 columns * 4 pixels
    # 6 = 6 stats per-column
    assert result_frame.shape == (20, 6)

    result_frame = per_pixel_statistics(partition_info_file, exclude_hats_columns=False, multi_index=True)
    # 24 = 6 columns * 4 pixels
    # 6 = 6 stats per-column
    assert result_frame.shape == (24, 6)


def test_per_pixel_statistics_include_stats(small_sky_order1_dir):
    partition_info_file = paths.get_parquet_metadata_pointer(small_sky_order1_dir)

    result_frame = per_pixel_statistics(partition_info_file, include_stats=["row_count"])
    # 5 = 5 columns * 1 stat per column
    assert result_frame.shape == (4, 5)

    result_frame = per_pixel_statistics(
        partition_info_file, include_stats=["row_count"], include_columns=["id"]
    )
    # 1 = 1 columns * 1 stat per column
    assert result_frame.shape == (4, 1)

    result_frame = per_pixel_statistics(
        partition_info_file, include_stats=["row_count"], include_columns=["id"], multi_index=True
    )
    # 1 = 1 columns * 1 stat per column
    assert result_frame.shape == (4, 1)

    with pytest.raises(ValueError, match="include_stats"):
        per_pixel_statistics(partition_info_file, include_stats=["bad", "min"])


def test_per_pixel_statistics_with_nested(small_sky_nested_dir):
    partition_info_file = paths.get_parquet_metadata_pointer(small_sky_nested_dir)

    ## Will have 13 returned columns (5 object and 8 light curve)
    ## Since object_dec is copied from object.dec, the min/max are the same,
    ## but there are MANY more rows of light curve dec values.
    result_frame = per_pixel_statistics(partition_info_file)
    assert len(result_frame) == 13
    assert result_frame["dec: row_count"].sum() == 131

    ## Only peeking at a single pixel, we should see the same dec min/max as
    ## we see for the flat object table.
    single_pixel = HealpixPixel(1, 47)
    result_frame = per_pixel_statistics(partition_info_file, include_pixels=[single_pixel], multi_index=True)
    assert len(result_frame) == 13
    assert_column_stat_as_floats(
        result_frame, (single_pixel, "dec"), min_value=-36.5, max_value=-25.5, row_count=18
    )
    assert_column_stat_as_floats(
        result_frame, (single_pixel, "lc.source_id"), min_value=70008, max_value=87148, row_count=2358
    )
    assert_column_stat_as_floats(
        result_frame, (single_pixel, "lc.mag"), min_value=15, max_value=21, row_count=2358
    )


def test_per_pixel_statistics_with_rowgroups_aggregated(small_sky_source_dir):
    partition_info_file = paths.get_parquet_metadata_pointer(small_sky_source_dir)

    result_frame = per_pixel_statistics(partition_info_file)
    ## 14 = number of partitions in this catalog
    assert len(result_frame) == 14
    assert result_frame["object_dec: row_count"].sum() == 17161

    single_pixel = HealpixPixel(1, 47)
    result_frame = per_pixel_statistics(partition_info_file, include_pixels=[single_pixel], multi_index=True)
    ## 9 = number of columns
    assert len(result_frame) == 9
    assert_column_stat_as_floats(
        result_frame, (single_pixel, "object_dec"), min_value=-36.5, max_value=-25.5, row_count=2395
    )


def test_statistics_numeric_fields(small_sky_source_dir):
    """Test behavior of the `only_numeric_columns` flag on both statistics-gathering methods."""
    partition_info_file = paths.get_parquet_metadata_pointer(small_sky_source_dir)

    result_frame = per_pixel_statistics(partition_info_file, only_numeric_columns=True)
    ## 14 = number of partitions in this catalog
    assert len(result_frame) == 14
    assert result_frame["object_dec: row_count"].sum() == 17161
    for col in result_frame.columns:
        assert is_numeric_dtype(result_frame[col]), f"Expected {col} to be numeric"

    single_pixel = HealpixPixel(1, 47)
    result_frame = per_pixel_statistics(
        partition_info_file, include_pixels=[single_pixel], multi_index=True, only_numeric_columns=True
    )
    ## 8 = number of NUMERIC columns (band is a string)
    assert len(result_frame) == 8
    for col in result_frame.columns:
        assert is_numeric_dtype(result_frame[col]), f"Expected {col} to be numeric"

    assert_column_stat_as_floats(
        result_frame, (single_pixel, "object_dec"), min_value=-36.5, max_value=-25.5, row_count=2395
    )

    result_frame = aggregate_column_statistics(partition_info_file, only_numeric_columns=True)
    assert len(result_frame) == 8

    for col in result_frame.columns:
        assert is_numeric_dtype(result_frame[col]), f"Expected {col} to be numeric"


def test_per_pixel_statistics_per_row_group(small_sky_source_dir):
    partition_info_file = paths.get_parquet_metadata_pointer(small_sky_source_dir)

    result_frame = per_pixel_statistics(partition_info_file, per_row_group=True)
    ## 24 = number of ROW GROUPS in ALL partitions in this catalog
    assert len(result_frame) == 24
    assert result_frame["object_dec: row_count"].sum() == 17161


def test_per_pixel_statistics_with_rowgroups_empty_result(small_sky_source_dir):
    partition_info_file = paths.get_parquet_metadata_pointer(small_sky_source_dir)
    result_frame = per_pixel_statistics(partition_info_file, include_pixels=[HealpixPixel(1, 4)])
    assert len(result_frame) == 0

    result_frame = per_pixel_statistics(
        partition_info_file, include_pixels=[HealpixPixel(1, 4)], multi_index=True
    )
    assert len(result_frame) == 0
