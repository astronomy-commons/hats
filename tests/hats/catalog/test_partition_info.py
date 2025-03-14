"""Tests of partition info functionality"""

import pandas as pd
import pytest

from hats.catalog import PartitionInfo
from hats.io import paths
from hats.pixel_math import HealpixPixel


def test_load_partition_info_small_sky(small_sky_dir):
    """Instantiate the partition info for catalog with 1 pixel"""
    partition_info_file = paths.get_parquet_metadata_pointer(small_sky_dir)
    partitions = PartitionInfo.read_from_file(partition_info_file)

    order_pixel_pairs = partitions.get_healpix_pixels()
    assert len(order_pixel_pairs) == 1
    expected = [HealpixPixel(0, 11)]
    assert order_pixel_pairs == expected


def test_load_partition_info_from_metadata(small_sky_dir, small_sky_source_dir, small_sky_source_pixels):
    """Instantiate the partition info for catalogs via the `_metadata` file"""
    metadata_file = paths.get_parquet_metadata_pointer(small_sky_dir)
    partitions = PartitionInfo.read_from_file(metadata_file)
    assert partitions.get_healpix_pixels() == [HealpixPixel(0, 11)]

    metadata_file = paths.get_parquet_metadata_pointer(small_sky_source_dir)
    partitions = PartitionInfo.read_from_file(metadata_file)
    assert partitions.get_healpix_pixels() == small_sky_source_pixels


def test_load_partition_info_from_metadata_fail(tmp_path):
    empty_dataframe = pd.DataFrame()
    metadata_filename = tmp_path / "empty_metadata.parquet"
    empty_dataframe.to_parquet(metadata_filename)
    with pytest.raises(ValueError, match="Insufficient metadata"):
        PartitionInfo.read_from_file(metadata_filename)

    non_healpix_dataframe = pd.DataFrame({"data": [0], "Npix": [45]})
    metadata_filename = tmp_path / "non_healpix_metadata.parquet"
    non_healpix_dataframe.to_parquet(metadata_filename)
    with pytest.raises(ValueError, match="Insufficient metadata"):
        PartitionInfo.read_from_file(metadata_filename)


def test_load_partition_info_from_dir_fail(tmp_path):
    empty_dataframe = pd.DataFrame()
    metadata_filename = tmp_path / "empty_metadata.parquet"
    empty_dataframe.to_parquet(metadata_filename)
    with pytest.raises(FileNotFoundError, match="_metadata or partition info"):
        PartitionInfo.read_from_dir(tmp_path)

    # The file is there, but doesn't have the required content.
    (tmp_path / "dataset").mkdir()
    metadata_filename = tmp_path / "dataset" / "_metadata"
    empty_dataframe.to_parquet(metadata_filename)
    with pytest.warns(UserWarning, match="slow"):
        with pytest.raises(ValueError, match="Insufficient metadata"):
            PartitionInfo.read_from_dir(tmp_path)


def test_load_partition_info_small_sky_order1(small_sky_order1_dir):
    """Instantiate the partition info for catalog with 4 pixels"""
    partition_info_file = paths.get_parquet_metadata_pointer(small_sky_order1_dir)
    partitions = PartitionInfo.read_from_file(partition_info_file)

    order_pixel_pairs = partitions.get_healpix_pixels()
    assert len(order_pixel_pairs) == 4
    expected = [
        HealpixPixel(1, 44),
        HealpixPixel(1, 45),
        HealpixPixel(1, 46),
        HealpixPixel(1, 47),
    ]
    assert order_pixel_pairs == expected


def test_load_partition_no_file(tmp_path):
    wrong_path = tmp_path / "_metadata"
    with pytest.raises(FileNotFoundError):
        PartitionInfo.read_from_file(wrong_path)

    wrong_path = tmp_path / "partition_info.csv"
    with pytest.raises(FileNotFoundError):
        PartitionInfo.read_from_csv(wrong_path)


def test_get_highest_order(small_sky_order1_dir):
    """test the `get_highest_order` method"""
    partition_info_file = paths.get_parquet_metadata_pointer(small_sky_order1_dir)
    partitions = PartitionInfo.read_from_file(partition_info_file)

    highest_order = partitions.get_highest_order()

    assert highest_order == 1


def test_calculate_fractional_coverage(small_sky_order1_dir):
    """test the `calculate_fractional_coverage` method"""
    partition_info_file = paths.get_parquet_metadata_pointer(small_sky_order1_dir)
    partitions = PartitionInfo.read_from_file(partition_info_file)

    fractional_coverage = partitions.calculate_fractional_coverage()

    assert fractional_coverage == pytest.approx(0.083, abs=0.001)


def test_write_to_file(tmp_path, small_sky_pixels):
    """Write out the partition info to file and make sure we can read it again."""
    partition_info_pointer = paths.get_partition_info_pointer(tmp_path)
    partition_info = PartitionInfo.from_healpix(small_sky_pixels)
    partition_info.write_to_file(partition_info_pointer)

    new_partition_info = PartitionInfo.read_from_csv(partition_info_pointer)

    # We're not using parquet metadata, so we don't force a re-sorting.
    assert partition_info.get_healpix_pixels() == new_partition_info.get_healpix_pixels()


def test_load_partition_info_from_dir_and_write(tmp_path, pixel_list_depth_first):
    partition_info = PartitionInfo.from_healpix(pixel_list_depth_first)

    ## Path arguments are required if the info was not created from a `read_from_dir` call
    with pytest.raises(ValueError):
        partition_info.write_to_file()

    partition_info.write_to_file(catalog_path=tmp_path)
    info = PartitionInfo.read_from_dir(tmp_path)

    ## Can write out the partition info CSV by providing:
    ##  - no arguments
    ##  - new catalog directory
    ##  - full path to the csv file
    info.write_to_file()
    info.write_to_file(catalog_path=tmp_path)
    info.write_to_file(partition_info_file=tmp_path / "new_csv.csv")
