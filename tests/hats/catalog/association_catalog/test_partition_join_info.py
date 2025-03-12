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


def test_load_partition_join_info_from_dir_fail(tmp_path):
    empty_dataframe = pd.DataFrame()
    metadata_filename = tmp_path / "empty_metadata.parquet"
    empty_dataframe.to_parquet(metadata_filename)
    with pytest.raises(FileNotFoundError, match="partition join info"):
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

    info.write_to_csv(catalog_path=tmp_path)
    info = PartitionJoinInfo.read_from_dir(tmp_path)

    ## Can write out the partition info CSV by providing:
    ##  - no arguments
    ##  - new catalog directory
    info.write_to_csv()
    info.write_to_csv(catalog_path=tmp_path)
