"""Tests of file IO (reads and writes)"""

import shutil

import astropy.units as u
import nested_pandas as npd
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from astropy.io.misc.parquet import read_parquet_votable
from pandas.api.types import is_numeric_dtype
from pyarrow.parquet import ParquetFile

from hats.io import file_io, paths
from hats.io.parquet_metadata import (
    aggregate_column_statistics,
    nested_frame_to_vo_schema,
    per_pixel_statistics,
    write_parquet_metadata,
    write_voparquet_in_common_metadata,
)
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


def test_skip_parquet_metadata_creation(tmp_path, small_sky_dir):
    """Copy existing catalog and skip metadata file creation"""
    catalog_base_dir = tmp_path / "catalog"
    shutil.copytree(
        small_sky_dir,
        catalog_base_dir,
    )

    # Remove existing files to ensure they are (or are not) re-created.
    metadata_file = catalog_base_dir / "dataset" / "_metadata"
    thumbnail_file = catalog_base_dir / "dataset" / "data_thumbnail.parquet"

    metadata_file.unlink(missing_ok=True)
    thumbnail_file.unlink(missing_ok=True)

    # Do NOT create _metadata OR thumbnail (but expect _common_metadata).
    total_rows = write_parquet_metadata(catalog_base_dir, create_metadata=False, create_thumbnail=False)
    assert total_rows == 131
    assert not (catalog_base_dir / "dataset" / "_metadata").exists()
    assert (catalog_base_dir / "dataset" / "_common_metadata").exists()
    assert not (catalog_base_dir / "dataset" / "data_thumbnail.parquet").exists()

    # Now create only the thumbnail, but make sure _metadata still is not created.
    total_rows = write_parquet_metadata(catalog_base_dir, create_metadata=False, create_thumbnail=True)
    assert total_rows == 131
    assert not (catalog_base_dir / "dataset" / "_metadata").exists()
    assert (catalog_base_dir / "dataset" / "_common_metadata").exists()
    assert (catalog_base_dir / "dataset" / "data_thumbnail.parquet").exists()


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


def test_aggregate_column_statistics_nested(small_sky_nested_dir):
    partition_info_file = paths.get_parquet_metadata_pointer(small_sky_nested_dir)

    result_frame = aggregate_column_statistics(partition_info_file)
    assert len(result_frame) == 13

    result_frame = aggregate_column_statistics(partition_info_file, include_columns=["ra", "lc"])
    # 9 = 1 base column + 8 nested sub-columns
    assert len(result_frame) == 9

    result_frame = aggregate_column_statistics(partition_info_file, include_columns=["ra", "lc.source_ra"])
    # 2 = 1 base column + 1 nested sub-column
    assert len(result_frame) == 2

    result_frame = aggregate_column_statistics(partition_info_file, exclude_columns=["lc"])
    # 5 base columns
    assert len(result_frame) == 5

    result_frame = aggregate_column_statistics(partition_info_file, exclude_columns=["lc.source_dec"])
    # 12 = 5 base columns + 7 nested sub-columns
    assert len(result_frame) == 12


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


def test_aggregate_column_statistics_empty_catalog(small_sky_order1_empty_margin_dir):
    partition_info_file = paths.get_parquet_metadata_pointer(small_sky_order1_empty_margin_dir)

    result_frame = aggregate_column_statistics(partition_info_file)
    assert len(result_frame) == 0


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


def test_per_pixel_statistics_nested(small_sky_nested_dir):
    # 13 = 13 pixels
    partition_info_file = paths.get_parquet_metadata_pointer(small_sky_nested_dir)

    result_frame = per_pixel_statistics(partition_info_file)
    # 78 = (5 base columns + 8 nested sub-columns) * 6 stats
    assert result_frame.shape == (13, 78)

    result_frame = per_pixel_statistics(partition_info_file, include_columns=["lc", "ra"])
    # 54 = (8 nested sub-columns + "ra") * 6 stats
    assert result_frame.shape == (13, 54)

    result_frame = per_pixel_statistics(partition_info_file, include_columns=["lc.source_ra", "ra"])
    # 12 = ("lc.source_ra" + "ra") * 6 stats
    assert result_frame.shape == (13, 12)

    result_frame = per_pixel_statistics(partition_info_file, exclude_columns=["lc"])
    # 30 = 5 base columns * 6 stats
    assert result_frame.shape == (13, 30)

    result_frame = per_pixel_statistics(partition_info_file, exclude_columns=["lc.source_ra"])
    # 72 = (5 base columns + 7 nested sub-columns) * 6 stats
    assert result_frame.shape == (13, 72)


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

    result_frame = per_pixel_statistics(partition_info_file, include_stats=["disk_bytes", "memory_bytes"])
    # 10 = 5 columns * 2 stats per column
    assert result_frame.shape == (4, 10)

    # The order of the stats should not matter
    result_frame_2 = per_pixel_statistics(partition_info_file, include_stats=["memory_bytes", "disk_bytes"])
    pd.testing.assert_frame_equal(result_frame, result_frame_2)

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


def test_per_pixel_statistics_empty_catalog(small_sky_order1_empty_margin_dir):
    partition_info_file = paths.get_parquet_metadata_pointer(small_sky_order1_empty_margin_dir)

    result_frame = per_pixel_statistics(partition_info_file)
    assert len(result_frame) == 0


def test_nested_frame_to_vo_schema_small(small_sky_nested_dir):
    dec_utype = "stc:AstroCoords.Position2D.Value2.C2"
    return_value = nested_frame_to_vo_schema(
        npd.read_parquet(small_sky_nested_dir / "dataset" / "_common_metadata"),
        field_units={
            "ra": "deg",
            "dec": u.deg,
            "lc.source_ra": "deg",
            "lc.source_dec": u.deg,
            "lc.object_ra": "deg",
            "lc.object_dec": u.deg,
            "does_not_exist": "deg**2",
        },
        field_ucds={
            "ra": "pos.eq.ra",
            "dec": "pos.eq.dec",
            "lc.source_ra": "pos.eq.ra",
            "lc.source_dec": "pos.eq.dec",
            "lc.object_ra": "pos.eq.ra",
            "lc.object_dec": "pos.eq.dec",
            "does_not_exist": "pos.eq.dec",
        },
        field_descriptions={
            "ra": "Object ICRS Right Ascension",
            "dec": "Object ICRS Declination",
            "lc.source_ra": "Object ICRS Right Ascension",
            "lc.source_dec": "Object ICRS Declination",
            "lc.object_ra": "Object ICRS Right Ascension",
            "lc.object_dec": "Object ICRS Declination",
            "lc": "Properties of transient-object detections on the single-epoch difference images",
            "lc.band": "Band used to take this observation",
            "does_not_exist": "Band used to take this observation",
        },
        field_utypes={
            "ra": "stc:AstroCoords.Position2D.Value2.C1",
            "dec": dec_utype,
            "does_not_exist": dec_utype,
        },
    )
    assert return_value
    dec_value = next(return_value.get_first_table().get_fields_by_utype(dec_utype))
    assert dec_value.name == "dec"
    assert dec_value.datatype == "double"
    assert dec_value.unit == "deg"
    assert dec_value.utype == dec_utype


def test_write_voparquet_in_common_metadata_verbosity(small_sky_nested_dir, tmp_path, capsys):
    catalog_base_dir = tmp_path / "catalog"
    shutil.copytree(
        small_sky_nested_dir,
        catalog_base_dir,
    )

    dec_utype = "stc:AstroCoords.Position2D.Value2.C2"
    field_kwargs = {
        "field_units": {
            "ra": "deg",
            "dec": u.deg,
            "does_not_exist": "deg**2",
        },
        "field_ucds": {
            "ra": "pos.eq.ra",
            "dec": "pos.eq.dec",
            "does_not_exist": "pos.eq.dec",
        },
        "field_descriptions": {
            "ra": "Object ICRS Right Ascension",
            "dec": "Object ICRS Declination",
            "does_not_exist": "Band used to take this observation",
        },
        "field_utypes": {
            "ra": "stc:AstroCoords.Position2D.Value2.C1",
            "dec": dec_utype,
            "does_not_exist": dec_utype,
        },
    }

    ## No verbosity - nothing printed
    write_voparquet_in_common_metadata(catalog_base_dir, **field_kwargs)

    captured = capsys.readouterr().out
    assert captured == ""

    ## Yes verbosity - print a few warnings and full XML
    write_voparquet_in_common_metadata(catalog_base_dir, verbose=True, **field_kwargs)

    captured = capsys.readouterr().out
    assert "Extra Fields" in captured
    assert "dropping some units" in captured
    assert "dropping some descriptions" in captured
    assert dec_utype in captured
    # Default description for a nested field.
    assert "multi-column nested format" in captured


def test_write_voparquet_in_common_metadata_small(small_sky_order1_dir, tmp_path):
    catalog_base_dir = tmp_path / "catalog"
    shutil.copytree(
        small_sky_order1_dir,
        catalog_base_dir,
    )

    dec_utype = "stc:AstroCoords.Position2D.Value2.C2"
    write_voparquet_in_common_metadata(
        catalog_base_dir,
        field_units={
            "ra": "deg",
            "dec": u.deg,
            "does_not_exist": "deg**2",
        },
        field_ucds={
            "ra": "pos.eq.ra",
            "dec": "pos.eq.dec",
            "does_not_exist": "pos.eq.dec",
        },
        field_descriptions={
            "ra": "Object ICRS Right Ascension",
            "dec": "Object ICRS Declination",
            "does_not_exist": "Band used to take this observation",
        },
        field_utypes={
            "ra": "stc:AstroCoords.Position2D.Value2.C1",
            "dec": dec_utype,
            "does_not_exist": dec_utype,
        },
    )

    table = read_parquet_votable(catalog_base_dir / "dataset" / "_common_metadata")
    assert table.colnames == ["_healpix_29", "id", "ra", "dec", "ra_error", "dec_error"]
