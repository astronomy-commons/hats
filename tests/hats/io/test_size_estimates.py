import nested_pandas as npd
import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from hats import read_hats
from hats.io.file_io import read_parquet_file
from hats.io.paths import pixel_catalog_file
from hats.io.size_estimates import estimate_dir_size, get_mem_size_per_row


def test_estimate_dir_size(small_sky_dir):
    estimate = estimate_dir_size(small_sky_dir)
    assert estimate > 0
    assert isinstance(estimate, int)

    estimate_kb = estimate_dir_size(small_sky_dir, divisor=1024)
    assert estimate_kb > 0
    assert isinstance(estimate_kb, int)
    # That's just how division works, bro.
    assert estimate_kb < estimate


def test_estimate_dir_size_edge(tmp_path):
    estimate = estimate_dir_size(tmp_path)
    assert estimate == 0
    assert isinstance(estimate, int)

    estimate = estimate_dir_size("")
    assert estimate == 0
    assert isinstance(estimate, int)


def test_get_mem_size_per_row_pandas(small_sky_dir):
    small_sky_catalog = read_hats(small_sky_dir)

    single_pixel_df = small_sky_catalog.read_pixel_to_pandas(small_sky_catalog.get_healpix_pixels()[0])
    mem_sizes = get_mem_size_per_row(single_pixel_df)
    assert len(mem_sizes) == len(single_pixel_df)

    # All rows should be the same, and positive!
    assert np.all(np.array(mem_sizes) > 0)
    assert np.all(np.array(mem_sizes) == mem_sizes[0])


def test_get_mem_size_per_row_pyarrow(small_sky_dir):
    small_sky_catalog = read_hats(small_sky_dir)
    single_pixel_path = pixel_catalog_file(
        small_sky_dir,
        small_sky_catalog.get_healpix_pixels()[0],
    )

    single_pixel_table = read_parquet_file(single_pixel_path).read()
    mem_sizes = get_mem_size_per_row(single_pixel_table)
    assert len(mem_sizes) == len(single_pixel_table)

    # All rows should be the same, and positive!
    assert np.all(np.array(mem_sizes) > 0)
    assert np.all(np.array(mem_sizes) == mem_sizes[0])


def test_get_mem_size_per_row_pandas_nested(small_sky_nested_dir):
    small_sky_catalog = read_hats(small_sky_nested_dir)

    single_pixel_df = small_sky_catalog.read_pixel_to_pandas(small_sky_catalog.get_healpix_pixels()[0])
    mem_sizes = get_mem_size_per_row(single_pixel_df)
    assert len(mem_sizes) == len(single_pixel_df)

    # Not all rows are the same here.
    assert np.all(np.array(mem_sizes) > 0)


def test_get_mem_size_per_row_errors(small_sky_dir):
    small_sky_catalog = read_hats(small_sky_dir)
    single_pixel_path = pixel_catalog_file(
        small_sky_dir,
        small_sky_catalog.get_healpix_pixels()[0],
    )

    single_pixel_table = read_parquet_file(single_pixel_path)
    with pytest.raises(NotImplementedError, match="Unsupported data type"):
        get_mem_size_per_row(single_pixel_table)


def test_get_mem_size_of_chunk():
    """Test the _get_mem_size_of_chunk function for reasonable outputs."""
    # Test with an empty DataFrame
    empty_df = pd.DataFrame(columns=["id", "ra", "dec", "value"])
    mem_sizes_empty = get_mem_size_per_row(empty_df)
    assert len(mem_sizes_empty) == 0

    # Test with a small DataFrame
    df = pd.DataFrame(
        {
            "id": [0, 0, 0, 1, 1, 1, 2, 2, 2, 2],
            "ra": [10.0, 10.0, 10.0, 15.0, 15.0, 15.0, 12.1, 12.1, 12.1, 12.1],
            "dec": [0.0, 0.0, 0.0, -1.0, -1.0, -1.0, 0.5, 0.5, 0.5, 0.5],
            "time": [
                60676.0,
                60677.0,
                60678.0,
                60675.0,
                60676.5,
                60677.0,
                60676.6,
                60676.7,
                60676.8,
                60676.9,
            ],
            "brightness": [100.0, 101.0, 99.8, 5.0, 5.01, 4.98, 20.1, 20.5, 20.3, 20.2],
            "band": ["g", "r", "g", "r", "g", "r", "g", "g", "r", "r"],
        }
    )
    mem_sizes = get_mem_size_per_row(df)
    # Since we have 10 rows, mem_sizes should have length 10
    assert len(mem_sizes) == 10
    # Each entry should be a positive integer (size in bytes)
    assert all(isinstance(size, int) and size > 0 for size in mem_sizes)

    # Compare to a smaller DataFrame with fewer columns
    df_small = df[["id", "ra", "dec"]]
    mem_sizes_small = get_mem_size_per_row(df_small)
    assert len(mem_sizes_small) == 10
    assert all(isinstance(size, int) and size > 0 for size in mem_sizes_small)
    # Each entry in mem_sizes should be > corresponding entry in mem_sizes_small
    assert all(m > s for m, s in zip(mem_sizes, mem_sizes_small, strict=True))

    # Test with a pyarrow Table
    table = pa.Table.from_pandas(df)
    mem_sizes_table = get_mem_size_per_row(table)
    assert len(mem_sizes_table) == 10
    assert all(isinstance(size, int) and size > 0 for size in mem_sizes_table)

    # Test with a smaller pyarrow Table
    table_small = pa.Table.from_pandas(df_small)
    mem_sizes_table_small = get_mem_size_per_row(table_small)
    assert len(mem_sizes_table_small) == 10
    assert all(isinstance(size, int) and size > 0 for size in mem_sizes_table_small)
    # Each entry in mem_sizes_table should be > corresponding entry in mem_sizes_table_small
    assert all(m > s for m, s in zip(mem_sizes_table, mem_sizes_table_small, strict=True))


def test_get_mem_size_of_chunk_nested():
    """Test the _get_mem_size_of_chunk function with nested data."""
    # Create a small DataFrame and nest it
    df = pd.DataFrame(
        {
            "id": [0, 0, 0, 1, 1, 1, 2, 2, 2, 2],
            "ra": [10.0, 10.0, 10.0, 15.0, 15.0, 15.0, 12.1, 12.1, 12.1, 12.1],
            "dec": [0.0, 0.0, 0.0, -1.0, -1.0, -1.0, 0.5, 0.5, 0.5, 0.5],
            "time": [
                60676.0,
                60677.0,
                60678.0,
                60675.0,
                60676.5,
                60677.0,
                60676.6,
                60676.7,
                60676.8,
                60676.9,
            ],
            "brightness": [100.0, 101.0, 99.8, 5.0, 5.01, 4.98, 20.1, 20.5, 20.3, 20.2],
            "band": ["g", "r", "g", "r", "g", "r", "g", "g", "r", "r"],
        }
    )
    nf = npd.NestedFrame.from_flat(
        df,
        base_columns=["ra", "dec"],
        nested_columns=["time", "brightness", "band"],
        on="id",
        name="lightcurve",
    )

    # Calculate memory sizes
    mem_sizes = get_mem_size_per_row(nf)

    # Since we have only 3 rows once we nest, mem_sizes should have length 3
    assert len(mem_sizes) == 3
    # Each entry should be a positive integer (size in bytes)
    assert all(isinstance(size, int) and size > 0 for size in mem_sizes)
    # The first two entries should be the same, since they have 3 sub-rows each
    assert mem_sizes[0] == mem_sizes[1]
    # The last entry should be the larger than the other two, since it has 4 sub-rows
    assert mem_sizes[2] > mem_sizes[0]
