import numpy as np
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
