"""General utilities for estimating size of input and output."""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
from upath import UPath

from hats.io import file_io


def estimate_dir_size(path: str | Path | UPath | None = None, *, divisor=1):
    """Estimate the disk usage of a directory, and recursive contents.

    When divisor == 1, returns size in bytes."""
    path = file_io.get_upath(path)
    if path is None:
        return 0

    def _estimate_dir_size(target_dir):
        total_size = 0
        for item in target_dir.iterdir():
            if item.is_dir():
                total_size += _estimate_dir_size(item)
            else:
                total_size += item.stat().st_size
        return total_size

    est_size = _estimate_dir_size(path)
    if divisor > 1:
        return int(est_size / divisor)
    return est_size


def _item_mem_size(item):
    """Return the memory size of a single scalar value, in bytes.

    This is the per-element measurement shared by both the pandas and pyarrow
    branches of ``get_mem_size_per_row``. By the time a value reaches this
    function, it is already a native Python object: a pandas cell value (e.g.
    int, float, str, or, for nested/list-valued columns, a ``np.ndarray``), or
    the result of pyarrow's ``.as_py()``/``to_pylist()`` conversion (e.g. int,
    float, str, bytes, list, dict).

    Args:
        item: a single cell value, as described above.

    Returns:
        int: the estimated memory size of ``item``, in bytes.
    """
    if isinstance(item, np.ndarray):
        # np.ndarray is a special case: sys.getsizeof() on an ndarray only
        # reports the object's own overhead, not the underlying data buffer it
        # points to, so we add the buffer size (nbytes) explicitly.
        return item.nbytes + sys.getsizeof(item)  # object data + object overhead
    # For every other type (int, float, str, bytes, list, dict, etc.) sys.getsizeof
    # already accounts for the full memory footprint of the object.
    return sys.getsizeof(item)


def get_mem_size_per_row(data, cols=None):
    """Given a 2D array of data, return a list of memory sizes for each row in the chunk.

    Args:
        data (pd.DataFrame or pa.Table): the data chunk to measure
        cols (list[str] or None): if provided, only these columns are included in the
            per-row size calculation

    Returns:
        list[int]: list of memory sizes for each row in the chunk
    """
    if isinstance(data, pd.DataFrame):
        if cols is not None:
            data = data[cols]

        # Each row is conceptually a tuple of its column values, and sys.getsizeof()
        # of a tuple depends only on how many elements it holds, not on what those
        # elements are. So this overhead is the same for every row, and we only need
        # to compute it once here instead of re-measuring a row tuple for every row.
        row_overhead = sys.getsizeof(tuple([None] * len(data.columns)))

        # Start every row's running total at that shared per-row overhead.
        mem_sizes = pd.Series(row_overhead, index=data.index)

        # Add each column's contribution to every row's total, one column at a time.
        # Series.map() applies _item_mem_size to every value in the column in one
        # vectorized pass, which is much faster than rebuilding a Python tuple for
        # every row via itertuples() and looping over its items by hand.
        for column in data.columns:
            mem_sizes += data[column].map(_item_mem_size)

        # Series.tolist() converts numpy scalar types (e.g. np.int64) in the Series
        # back to plain Python ints, matching this function's documented return type.
        mem_sizes = mem_sizes.tolist()
    elif isinstance(data, pa.Table):
        if cols is not None:
            data = data.select(cols)

        # Mirror the pandas row overhead above: pyarrow's per-row overhead doesn't
        # depend on row content either, so it's a constant added to every row.
        row_overhead = sys.getsizeof(0)
        mem_sizes = [row_overhead] * data.num_rows

        # Walk one column at a time. column.to_pylist() converts the whole column to
        # native Python values in a single fast (C-level) pass, which avoids the
        # overhead of indexing into the column with column[row_index] (which boxes
        # each value as a pa.Scalar) once per cell.
        for column in data.itercolumns():
            for row_index, item in enumerate(column.to_pylist()):
                mem_sizes[row_index] += _item_mem_size(item)
    else:
        raise NotImplementedError(f"Unsupported data type {type(data)} for memory size calculation")
    return mem_sizes
