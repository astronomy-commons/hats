import numpy as np
import pandas as pd
import pytest

from hats.io.file_io import (
    delete_file,
    load_csv_to_pandas,
    load_csv_to_pandas_generator,
    make_directory,
    read_parquet_dataset,
    read_parquet_file_to_pandas,
    remove_directory,
    write_dataframe_to_csv,
    write_string_to_file,
)
from hats.io.file_io.file_pointer import does_file_or_directory_exist


def test_make_directory(tmp_path):
    test_dir_path = tmp_path / "test_path"
    assert not does_file_or_directory_exist(test_dir_path)
    make_directory(test_dir_path)
    assert does_file_or_directory_exist(test_dir_path)


def test_make_existing_directory_raises(tmp_path):
    test_dir_path = tmp_path / "test_path"
    make_directory(test_dir_path)
    assert does_file_or_directory_exist(test_dir_path)
    with pytest.raises(OSError):
        make_directory(test_dir_path)


def test_make_existing_directory_existok(tmp_path):
    test_dir_path = tmp_path / "test_path"
    make_directory(test_dir_path)
    test_inner_dir_path = test_dir_path / "test_inner"
    make_directory(test_inner_dir_path)
    assert does_file_or_directory_exist(test_dir_path)
    assert does_file_or_directory_exist(test_inner_dir_path)
    make_directory(test_dir_path, exist_ok=True)
    assert does_file_or_directory_exist(test_dir_path)
    assert does_file_or_directory_exist(test_inner_dir_path)


def test_make_and_remove_directory(tmp_path):
    test_dir_path = tmp_path / "test_path"
    assert not does_file_or_directory_exist(test_dir_path)
    make_directory(test_dir_path)
    make_directory(test_dir_path / "subdirectory")
    (test_dir_path / "subdirectory" / "file").touch()
    assert does_file_or_directory_exist(test_dir_path)
    remove_directory(test_dir_path)
    assert not does_file_or_directory_exist(test_dir_path)

    ## Directory no longer exists to be deleted.
    with pytest.raises(FileNotFoundError):
        remove_directory(test_dir_path)

    ## Directory doesn't exist, but shouldn't throw an error.
    remove_directory(test_dir_path, ignore_errors=True)
    assert not does_file_or_directory_exist(test_dir_path)


def test_write_string_to_file(tmp_path):
    test_file_path = tmp_path / "text_file.txt"
    test_string = "this is a test"
    write_string_to_file(test_file_path, test_string, encoding="utf-8")
    with open(test_file_path, "r", encoding="utf-8") as file:
        data = file.read()
        assert data == test_string
    delete_file(test_file_path)
    assert not does_file_or_directory_exist(test_file_path)


def test_load_csv_to_pandas(small_sky_source_dir):
    partition_info_path = small_sky_source_dir / "partition_info.csv"
    frame = load_csv_to_pandas(partition_info_path)
    assert len(frame) == 14


def test_load_csv_to_pandas_generator(small_sky_source_dir):
    partition_info_path = small_sky_source_dir / "partition_info.csv"
    num_reads = 0
    for frame in load_csv_to_pandas_generator(partition_info_path, chunksize=7, compression=None):
        assert len(frame) == 7
        num_reads += 1
    assert num_reads == 2


def test_write_df_to_csv(tmp_path):
    random_df = pd.DataFrame(np.random.randint(0, 100, size=(100, 4)), columns=list("ABCD"))
    test_file_path = tmp_path / "test.csv"
    write_dataframe_to_csv(random_df, test_file_path, index=False)
    loaded_df = pd.read_csv(test_file_path)
    pd.testing.assert_frame_equal(loaded_df, random_df)


def test_read_parquet_data(tmp_path):
    random_df = pd.DataFrame(np.random.randint(0, 100, size=(100, 4)), columns=list("ABCD"))
    test_file_path = tmp_path / "test.parquet"
    random_df.to_parquet(test_file_path)
    dataframe = read_parquet_file_to_pandas(test_file_path)
    pd.testing.assert_frame_equal(dataframe, random_df)


def test_read_parquet_dataset(small_sky_dir, small_sky_order1_dir):
    (_, ds) = read_parquet_dataset(small_sky_dir / "dataset" / "Norder=0")

    assert ds.count_rows() == 131

    (_, ds) = read_parquet_dataset([small_sky_dir / "dataset" / "Norder=0" / "Dir=0" / "Npix=11.parquet"])

    assert ds.count_rows() == 131

    (_, ds) = read_parquet_dataset(
        [
            small_sky_order1_dir / "dataset" / "Norder=1" / "Dir=0" / "Npix=44.parquet",
            small_sky_order1_dir / "dataset" / "Norder=1" / "Dir=0" / "Npix=45.parquet",
            small_sky_order1_dir / "dataset" / "Norder=1" / "Dir=0" / "Npix=46.parquet",
            small_sky_order1_dir / "dataset" / "Norder=1" / "Dir=0" / "Npix=47.parquet",
        ]
    )

    assert ds.count_rows() == 131
