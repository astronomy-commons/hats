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


def _get_row_mem_size_data_frame(row):
    """Given a pandas dataframe row (as a tuple), return the memory size of that row.

    Args:
        row (tuple): the row from the dataframe

    Returns:
        int: the memory size of the row in bytes
    """
    total = 0

    # Add the memory overhead of the row object itself.
    total += sys.getsizeof(row)

    # Then add the size of each item in the row.
    for item in row:
        if isinstance(item, np.ndarray):
            total += item.nbytes + sys.getsizeof(item)  # object data + object overhead
        else:
            total += sys.getsizeof(item)
    return total


def _get_row_mem_size_pa_table(table, row_index):
    """Given a pyarrow table and a row index, return the memory size of that row.

    Args:
        table (pa.Table): the pyarrow table
        row_index (int): the index of the row to measure

    Returns:
        int: the memory size of the row in bytes
    """
    total = 0

    # Add the memory overhead of the row object itself.
    total += sys.getsizeof(row_index)

    # Then add the size of each item in the row.
    for column in table.itercolumns():
        item = column[row_index]
        if isinstance(item, np.ndarray):
            total += item.nbytes + sys.getsizeof(item)  # object data + object overhead
        else:
            total += sys.getsizeof(item.as_py())
    return total


def get_mem_size_per_row(data):
    """Given a 2D array of data, return a list of memory sizes for each row in the chunk.

    Args:
        data (pd.DataFrame or pa.Table): the data chunk to measure

    Returns:
        list[int]: list of memory sizes for each row in the chunk
    """
    if isinstance(data, pd.DataFrame):
        mem_sizes = [_get_row_mem_size_data_frame(row) for row in data.itertuples(index=False, name=None)]
    elif isinstance(data, pa.Table):
        mem_sizes = [_get_row_mem_size_pa_table(data, i) for i in range(data.num_rows)]
    else:
        raise NotImplementedError(f"Unsupported data type {type(data)} for memory size calculation")
    return mem_sizes
