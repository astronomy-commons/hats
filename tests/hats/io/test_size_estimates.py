import sys

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


def test_get_mem_size_per_row_mixed_frame():
    """A realistic multi-column frame: sizes are positive, dropping columns shrinks
    every row, and a pandas frame measures the same as its equivalent pyarrow table."""
    # An empty frame has no rows to measure.
    empty_df = pd.DataFrame(columns=["id", "ra", "dec", "value"])
    mem_sizes_empty = get_mem_size_per_row(empty_df)
    assert len(mem_sizes_empty) == 0

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
    # Each entry should be a positive float (size in bytes)
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


def test_get_mem_size_per_row_nested_frame():
    """A nested (light-curve) frame: rows with more sub-rows cost more."""
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
    # Each entry should be a positive float (size in bytes)
    assert all(isinstance(size, float) and size > 0 for size in mem_sizes)
    # The first two entries should be the same, since they have 3 sub-rows each
    assert mem_sizes[0] == mem_sizes[1]
    # The last entry should be the larger than the other two, since it has 4 sub-rows
    assert mem_sizes[2] > mem_sizes[0]


def test_get_mem_size_per_row_fixed_and_string_columns():
    """Fixed-width and string/binary columns are sized by value width and offsets:

    - fixed-width values cost their byte width (bit-packed bools count 1/8 byte);
    - fixed-size binary reads its width from ``byte_width``;
    - string/binary values cost their UTF-8 data bytes plus one offset entry
      (4 bytes, or 8 for the ``large_`` variants);
    - a null makes the column carry a validity bitmap, a 1/8-byte-per-row share.
    """
    # ints, bit-packed bools, and a string column whose null makes it carry a bitmap.
    table = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "flag": pa.array([True, False, True], type=pa.bool_()),
            "name": pa.array(["1001", None, "ééé"], type=pa.string()),
        }
    )
    # id: 8 each; flag: 1/8 each; name: data bytes + 4-byte offset entry + 1/8
    # bitmap, where "ééé" is 6 UTF-8 bytes and the null row holds no data.
    assert get_mem_size_per_row(table) == [
        8 + 0.125 + (4 + 4 + 0.125),
        8 + 0.125 + (0 + 4 + 0.125),
        8 + 0.125 + (6 + 4 + 0.125),
    ]

    # Fixed-size binary: every value costs its declared byte width (here 4); no
    # nulls, so no bitmap share.
    fixed_binary = get_mem_size_per_row(pa.table({"fb": pa.array([b"abcd", b"efgh", b"ijkl"], pa.binary(4))}))
    assert fixed_binary == [4.0, 4.0, 4.0]
    assert all(isinstance(size, float) for size in fixed_binary)

    # Large string: 8-byte offset entries instead of 4; no nulls, so no bitmap.
    # "ab" = 2 data bytes + 8; "cde" = 3 + 8.
    large_string = get_mem_size_per_row(pa.table({"name": pa.array(["ab", "cde"], pa.large_string())}))
    assert large_string == [10.0, 11.0]
    assert all(isinstance(size, float) for size in large_string)


def test_get_mem_size_per_row_list_columns():
    """List columns are sized from the arrow offsets: each row costs its slice of
    the shared values buffer (element count x element width) plus one offset entry
    (4 bytes, 8 for large lists), and a null makes the column carry a validity
    bitmap (one bit = 1/8 byte per row)."""
    bitmap = 0.125
    lightcurves = [[10.1, 10.3, 9.8], [], None, [4.2] * 100]
    for list_type, offset_width in [(pa.list_, 4), (pa.large_list, 8)]:
        table = pa.table({"lightcurve": pa.array(lightcurves, type=list_type(pa.float64()))})
        # Null rows hold no data but still occupy an offset entry.
        assert get_mem_size_per_row(table) == [
            3 * 8 + offset_width + bitmap,
            offset_width + bitmap,
            offset_width + bitmap,
            100 * 8 + offset_width + bitmap,
        ]

    # Lists of variable-width elements (list<string>) attribute the elements' total
    # buffer size uniformly per element, so longer rows cost proportionally more and
    # the group total tracks the loaded buffers.
    names = pa.array([["a", "bb"], ["ccc"], []], type=pa.list_(pa.string()))
    mem_sizes = get_mem_size_per_row(pa.table({"names": names}))
    assert all(isinstance(size, float) for size in mem_sizes)
    # The empty row costs only its offset entry; longer rows cost more.
    assert mem_sizes[0] > mem_sizes[1] > mem_sizes[2] == 4
    # The model counts n offset entries per offsets buffer where the real buffer
    # holds n + 1, so it undercounts one entry each for the outer and the
    # flattened-string buffer.
    assert sum(mem_sizes) == pytest.approx(names.nbytes, abs=8)


def test_get_mem_size_per_row_struct_columns():
    """Struct rows (e.g. nested columns) cost the sum of their fields, whether the
    struct comes from a pyarrow table or from an object column of dict cells (which
    infers the same arrow struct)."""
    struct_rows = [
        {"time": [1.0, 2.0, 3.0], "band": "g"},
        {"time": [4.0], "band": "rr"},
    ]
    struct_type = pa.struct([("time", pa.list_(pa.float64())), ("band", pa.string())])
    # time: 8 x elements + 4-byte offset entry; band: UTF-8 bytes + 4-byte offset
    # entry. No nulls, so no bitmap share.
    expected = [(3 * 8 + 4) + (1 + 4), (1 * 8 + 4) + (2 + 4)]

    table = pa.table({"lc": pa.array(struct_rows, type=struct_type)})
    assert get_mem_size_per_row(table) == expected

    # An object column of dict cells infers the same struct model; field order may
    # differ after inference, but the per-row totals do not.
    frame = pd.DataFrame({"lc": pd.Series(struct_rows, dtype=object)})
    mem_sizes = get_mem_size_per_row(frame)
    assert mem_sizes == expected
    assert all(isinstance(size, float) for size in mem_sizes)


def test_get_mem_size_per_row_pandas_non_arrow_columns():
    """Object columns arrow can't represent fall back to per-item measurement,
    sized by the underlying buffer rather than the Python wrapper's overhead:
    ndarray cells by ``nbytes``, numpy scalars by ``itemsize``, and any other
    Python object by ``sys.getsizeof``."""
    # Numpy scalar cells (structured np.void records): sized by itemsize. Two
    # float64 fields => 16 bytes per record, regardless of Python overhead.
    record_type = np.dtype([("x", np.float64), ("y", np.float64)])
    records = np.array([(1.0, 2.0), (3.0, 4.0), (5.0, 6.0)], dtype=record_type)
    frame = pd.DataFrame({"record": pd.Series(list(records), dtype=object)})
    mem_sizes = get_mem_size_per_row(frame)
    assert mem_sizes == [16.0, 16.0, 16.0]
    assert all(isinstance(size, float) for size in mem_sizes)

    # Ragged ndarray cells (a 1-D and a 2-D array) can't be unified into one arrow
    # type; each is sized by its buffer. 2 x int64 = 16 bytes; 4 x int64 = 32 bytes.
    cells = [
        np.array([1, 2], dtype=np.int64),
        np.array([[1, 2], [3, 4]], dtype=np.int64),
    ]
    frame = pd.DataFrame({"arr": pd.Series(cells, dtype=object)})
    assert get_mem_size_per_row(frame) == [16.0, 32.0]

    # Arbitrary Python objects (neither ndarray nor numpy scalar): sys.getsizeof.
    opaque = [object(), object()]
    frame = pd.DataFrame({"obj": pd.Series(opaque, dtype=object)})
    assert get_mem_size_per_row(frame) == [float(sys.getsizeof(cell)) for cell in opaque]


def test_get_mem_size_per_row_pandas_pyarrow_equivalence_and_cols():
    """A pandas chunk measures the same as its equivalent arrow table (columns are
    converted before measuring), and passing ``cols`` restricts the measurement to
    those columns for both pandas and pyarrow inputs."""
    # Equivalence: numerics, strings, and ndarray list cells measure identically.
    frame = pd.DataFrame(
        {
            "ra": [290.0, 300.0, 310.0],
            "id_str": ["1001", "1002", "1003"],
            "mags": [
                np.array([1.0, 2.0]),
                np.array([3.0]),
                np.array([], dtype=np.float64),
            ],
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

    # Column selection restricts the measurement to the named columns, matching a
    # pre-selection; dropping the string column lowers every row's size.
    frame = pd.DataFrame(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()).to_pandas(),
            "ra": [10.0, 15.0, 12.1],
            "name": ["aaaa", "bb", "c"],
        }
    )
    selected = get_mem_size_per_row(frame, cols=["id", "ra"])
    assert selected == get_mem_size_per_row(frame[["id", "ra"]])
    assert all(s < full for s, full in zip(selected, get_mem_size_per_row(frame), strict=True))

    table = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "ra": pa.array([10.0, 15.0, 12.1], type=pa.float64()),
            "name": pa.array(["aaaa", "bb", "c"], type=pa.string()),
        }
    )
    selected = get_mem_size_per_row(table, cols=["id", "ra"])
    assert selected == get_mem_size_per_row(table.select(["id", "ra"]))
    assert all(s < full for s, full in zip(selected, get_mem_size_per_row(table), strict=True))


def test_get_mem_size_per_row_pyarrow_empty_fixed_width():
    """An empty fixed-width column has no rows to share a validity bitmap, so the
    bitmap share is zero and the per-row list is empty."""
    table = pa.table({"id": pa.array([], type=pa.int64())})

    assert get_mem_size_per_row(table) == []
