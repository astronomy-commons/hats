import pandas as pd
import pytest

from hats.catalog.association_catalog.partition_join_info import PartitionJoinInfo
from hats.pixel_math.healpix_pixel import HealpixPixel


def test_init(association_catalog_join_pixels):
    partition_join_info = PartitionJoinInfo(association_catalog_join_pixels)
    pd.testing.assert_frame_equal(partition_join_info.data_frame, association_catalog_join_pixels)


def test_wrong_columns(association_catalog_join_pixels):
    for column in PartitionJoinInfo.COLUMN_NAMES:
        join_pixels = association_catalog_join_pixels.copy()
        join_pixels = join_pixels.rename(columns={column: "wrong_name"})
        with pytest.raises(ValueError, match=column):
            PartitionJoinInfo(join_pixels)


def test_read_from_metadata(association_catalog_join_pixels, association_catalog_path):
    info = PartitionJoinInfo.read_from_file(association_catalog_path / "dataset" / "_metadata")
    pd.testing.assert_frame_equal(info.data_frame, association_catalog_join_pixels)


def test_read_from_metadata_fail(tmp_path):
    empty_dataframe = pd.DataFrame()
    metadata_filename = tmp_path / "empty_metadata.parquet"
    empty_dataframe.to_parquet(metadata_filename)
    with pytest.raises(ValueError, match="missing columns"):
        PartitionJoinInfo.read_from_file(metadata_filename)

    with pytest.raises(ValueError, match="at least one column"):
        PartitionJoinInfo.read_from_file(metadata_filename, strict=True)

    ## Starting with a valid join info, remove each column and make sure we error.
    valid_ish_dataframe = pd.DataFrame(
        {"data": [0], "Norder": [3], "Npix": [45], "join_Norder": [3], "join_Npix": [45]}
    )
    metadata_filename = tmp_path / "test_metadata.parquet"
    valid_ish_dataframe.to_parquet(metadata_filename)
    PartitionJoinInfo.read_from_file(metadata_filename)

    for drop_column in ["Norder", "Npix", "join_Norder", "join_Npix"]:
        missing_column_dataframe = valid_ish_dataframe.drop(labels=drop_column, axis=1)
        missing_column_dataframe.to_parquet(metadata_filename)
        with pytest.raises(ValueError, match=f"missing .*{drop_column}"):
            PartitionJoinInfo.read_from_file(metadata_filename)

    with pytest.raises(ValueError, match="empty file path"):
        PartitionJoinInfo.read_from_file(metadata_filename, strict=True)


def test_load_partition_join_info_from_dir_fail(tmp_path):
    empty_dataframe = pd.DataFrame()
    metadata_filename = tmp_path / "empty_metadata.parquet"
    empty_dataframe.to_parquet(metadata_filename)
    with pytest.raises(FileNotFoundError, match="_metadata or partition join info"):
        PartitionJoinInfo.read_from_dir(tmp_path)

    # The file is there, but doesn't have the required content.
    (tmp_path / "dataset").mkdir()
    metadata_filename = tmp_path / "dataset" / "_metadata"
    empty_dataframe.to_parquet(metadata_filename)
    with pytest.warns(UserWarning, match="slow"):
        with pytest.raises(ValueError, match="missing columns"):
            PartitionJoinInfo.read_from_dir(tmp_path)


def test_primary_to_join_map(association_catalog_join_pixels):
    info = PartitionJoinInfo(association_catalog_join_pixels)
    pd.testing.assert_frame_equal(info.data_frame, association_catalog_join_pixels)
    pixel_map = info.primary_to_join_map()

    expected = {
        HealpixPixel(0, 11): [
            HealpixPixel(1, 44),
            HealpixPixel(1, 45),
            HealpixPixel(1, 46),
            HealpixPixel(1, 47),
        ]
    }
    assert pixel_map == expected


def test_metadata_file_round_trip(association_catalog_join_pixels, tmp_path):
    info = PartitionJoinInfo(association_catalog_join_pixels)
    pd.testing.assert_frame_equal(info.data_frame, association_catalog_join_pixels)
    total_rows = info.write_to_metadata_files(tmp_path)
    assert total_rows == 4

    new_info = PartitionJoinInfo.read_from_file(tmp_path / "dataset" / "_metadata")
    pd.testing.assert_frame_equal(new_info.data_frame, association_catalog_join_pixels)


def test_csv_file_round_trip(association_catalog_join_pixels, tmp_path):
    info = PartitionJoinInfo(association_catalog_join_pixels)
    pd.testing.assert_frame_equal(info.data_frame, association_catalog_join_pixels)
    info.write_to_csv(tmp_path)

    new_info = PartitionJoinInfo.read_from_csv(tmp_path / "partition_join_info.csv")
    pd.testing.assert_frame_equal(new_info.data_frame, association_catalog_join_pixels)


def test_read_from_csv(association_catalog_partition_join_file, association_catalog_join_pixels):
    info = PartitionJoinInfo.read_from_csv(association_catalog_partition_join_file)
    pd.testing.assert_frame_equal(info.data_frame, association_catalog_join_pixels)


def test_read_from_missing_file(tmp_path):
    with pytest.raises(FileNotFoundError):
        PartitionJoinInfo.read_from_csv(tmp_path / "wrong")


def test_load_partition_info_from_dir_and_write(tmp_path, association_catalog_join_pixels):
    info = PartitionJoinInfo(association_catalog_join_pixels)

    ## Path arguments are required if the info was not created from a `read_from_dir` call
    with pytest.raises(ValueError):
        info.write_to_csv()
    with pytest.raises(ValueError):
        info.write_to_metadata_files()

    info.write_to_csv(catalog_path=tmp_path)
    info = PartitionJoinInfo.read_from_dir(tmp_path)

    ## Can write out the partition info CSV by providing:
    ##  - no arguments
    ##  - new catalog directory
    info.write_to_csv()
    info.write_to_csv(catalog_path=tmp_path)

    ## Can write out the _metadata file by providing:
    ##  - no arguments
    ##  - new catalog directory
    total_rows = info.write_to_metadata_files()
    assert total_rows == 4

    total_rows = info.write_to_metadata_files(catalog_path=tmp_path)
    assert total_rows == 4
