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
    assert all(isinstance(size, float) and size > 0 for size in mem_sizes)

    # Compare to a smaller DataFrame with fewer columns
    df_small = df[["id", "ra", "dec"]]
    mem_sizes_small = get_mem_size_per_row(df_small)
    assert len(mem_sizes_small) == 10
    assert all(isinstance(size, float) and size > 0 for size in mem_sizes_small)
    # Each entry in mem_sizes should be > corresponding entry in mem_sizes_small
    assert all(m > s for m, s in zip(mem_sizes, mem_sizes_small, strict=True))

    # Test with a pyarrow Table
    table = pa.Table.from_pandas(df)
    mem_sizes_table = get_mem_size_per_row(table)
    assert len(mem_sizes_table) == 10
    assert all(isinstance(size, float) and size > 0 for size in mem_sizes_table)

    # Test with a smaller pyarrow Table
    table_small = pa.Table.from_pandas(df_small)
    mem_sizes_table_small = get_mem_size_per_row(table_small)
    assert len(mem_sizes_table_small) == 10
    assert all(isinstance(size, float) and size > 0 for size in mem_sizes_table_small)
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
    assert all(isinstance(size, float) and size > 0 for size in mem_sizes)
    # The first two entries should be the same, since they have 3 sub-rows each
    assert mem_sizes[0] == mem_sizes[1]
    # The last entry should be the larger than the other two, since it has 4 sub-rows
    assert mem_sizes[2] > mem_sizes[0]


@pytest.mark.parametrize("list_type,offset_width", [(pa.list_, 4), (pa.large_list, 8)])
def test_get_mem_size_per_row_pyarrow_list_columns(list_type, offset_width):
    """List columns are sized from the arrow offsets: each row costs its slice of
    the shared values buffer (element count x element width) plus one offset entry.
    Null rows hold no data but still occupy an offset entry, and the null makes
    the column carry a validity bitmap (one bit = 1/8 byte per row)."""
    lightcurves = [[10.1, 10.3, 9.8], [], None, [4.2] * 100]
    table = pa.table({"lightcurve": pa.array(lightcurves, type=list_type(pa.float64()))})

    mem_sizes = get_mem_size_per_row(table)

    bitmap = 0.125
    expected = [
        3 * 8 + offset_width + bitmap,
        offset_width + bitmap,
        offset_width + bitmap,
        100 * 8 + offset_width + bitmap,
    ]
    assert mem_sizes == expected
    assert all(isinstance(size, float) for size in mem_sizes)


def test_get_mem_size_per_row_pyarrow_list_of_strings():
    """Lists of variable-width elements attribute the elements' total buffer size
    uniformly per element, so longer rows cost proportionally more and the group
    total tracks the loaded buffers."""
    names = pa.array([["a", "bb"], ["ccc"], []], type=pa.list_(pa.string()))
    table = pa.table({"names": names})

    mem_sizes = get_mem_size_per_row(table)

    assert all(isinstance(size, float) for size in mem_sizes)
    # The empty row costs only its offset entry; longer rows cost more.
    assert mem_sizes[0] > mem_sizes[1] > mem_sizes[2] == 4
    # The total approximates the column's actual loaded buffers: the model counts
    # n offset entries per offsets buffer where the real buffer holds n + 1, so
    # it undercounts one entry each for the outer and the flattened-string buffer.
    assert sum(mem_sizes) == pytest.approx(names.nbytes, abs=8)


def test_get_mem_size_per_row_pyarrow_strings_and_fixed_widths():
    """Fixed-width values cost their byte width (bit-packed bools count 1/8 byte);
    string values cost their UTF-8 data bytes plus one offset entry, plus a
    one-bit validity share since the null makes the column carry a bitmap."""
    table = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "flag": pa.array([True, False, True], type=pa.bool_()),
            "name": pa.array(["1001", None, "ééé"], type=pa.string()),
        }
    )

    mem_sizes = get_mem_size_per_row(table)

    # id: 8 each; flag: 1/8 each; name: data bytes + 4-byte offset entry + 1/8
    # bitmap, where "ééé" is 6 UTF-8 bytes and the null row holds no data.
    assert mem_sizes == [8 + 0.125 + (4 + 4 + 0.125), 8 + 0.125 + (0 + 4 + 0.125), 8 + 0.125 + (6 + 4 + 0.125)]


def test_get_mem_size_per_row_pyarrow_struct():
    """Struct rows (e.g. nested columns) cost the sum of their fields."""
    struct_rows = [
        {"time": [1.0, 2.0, 3.0], "band": "g"},
        {"time": [4.0], "band": "rr"},
    ]
    struct_type = pa.struct([("time", pa.list_(pa.float64())), ("band", pa.string())])
    table = pa.table({"lc": pa.array(struct_rows, type=struct_type)})

    mem_sizes = get_mem_size_per_row(table)

    # time: 8 x elements + 4-byte offset entry; band: UTF-8 bytes + 4-byte offset entry.
    assert mem_sizes == [(3 * 8 + 4) + (1 + 4), (1 * 8 + 4) + (2 + 4)]


def test_get_mem_size_per_row_pandas_matches_pyarrow():
    """A pandas chunk (numerics, strings, ndarray list cells) measures the same as
    the equivalent arrow table, since pandas columns are converted before measuring."""
    frame = pd.DataFrame(
        {
            "ra": [290.0, 300.0, 310.0],
            "id_str": ["1001", "1002", "1003"],
            "mags": [np.array([1.0, 2.0]), np.array([3.0]), np.array([], dtype=np.float64)],
        }
    )
    table = pa.table(
        {
            "ra": pa.array(frame["ra"], type=pa.float64()),
            "id_str": pa.array(frame["id_str"], type=pa.string()),
            "mags": pa.array([[1.0, 2.0], [3.0], []], type=pa.list_(pa.float64())),
        }
    )

    assert get_mem_size_per_row(frame) == get_mem_size_per_row(table)
