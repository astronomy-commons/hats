"""General utilities for estimating size of input and output."""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
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


def _is_fixed_width_arrow_type(arrow_type):
    """Whether every value of the arrow type occupies the same number of bytes."""
    return (
        pa.types.is_integer(arrow_type)
        or pa.types.is_floating(arrow_type)
        or pa.types.is_boolean(arrow_type)
        or pa.types.is_decimal(arrow_type)
        or pa.types.is_temporal(arrow_type)
        or pa.types.is_fixed_size_binary(arrow_type)
    )


def _fixed_value_width(arrow_type):
    """Bytes per value of a fixed-width arrow type. Bit-packed types (booleans)
    are counted as one byte per value, matching their numpy/pandas representation."""
    return max(1, arrow_type.bit_width // 8)


def _string_like_offset_width(arrow_type):
    """Offset-entry bytes per row for string/binary types, or None if not string-like."""
    if pa.types.is_string(arrow_type) or pa.types.is_binary(arrow_type):
        return 4
    if pa.types.is_large_string(arrow_type) or pa.types.is_large_binary(arrow_type):
        return 8
    return None


def _arrow_column_mem_sizes(column):
    """Estimated per-row loaded-memory bytes for a pyarrow column.

    Arrow stores variable-length columns as one flat values buffer plus an
    offsets buffer, so each row's share of the loaded buffers can be read from
    the offsets (``pc.list_value_length`` / ``pc.binary_length``) without
    converting any values to Python objects. Summed over the rows of a
    partition, these shares reproduce the partition's buffer sizes.

    Args:
        column (pa.Array or pa.ChunkedArray): the column to measure.

    Returns:
        np.ndarray: per-row memory sizes, in bytes.
    """
    column_type = column.type
    num_rows = len(column)

    if _is_fixed_width_arrow_type(column_type):
        return np.full(num_rows, _fixed_value_width(column_type), dtype=np.int64)

    offset_width = _string_like_offset_width(column_type)
    if offset_width is not None:
        # binary_length counts the data bytes of each value (nulls hold no data,
        # but still occupy an offset entry).
        data_bytes = pc.fill_null(pc.binary_length(column), 0)
        return data_bytes.to_numpy(zero_copy_only=False).astype(np.int64) + offset_width

    if pa.types.is_list(column_type) or pa.types.is_large_list(column_type):
        offset_width = 8 if pa.types.is_large_list(column_type) else 4
        lengths = pc.fill_null(pc.list_value_length(column), 0)
        lengths = lengths.to_numpy(zero_copy_only=False).astype(np.int64)
        element_type = column_type.value_type
        if _is_fixed_width_arrow_type(element_type):
            return lengths * _fixed_value_width(element_type) + offset_width
        # Variable-width elements (e.g. list<string>): attribute the flattened
        # elements' total buffer size uniformly per element.
        flattened = pc.list_flatten(column)
        bytes_per_element = flattened.nbytes / len(flattened) if len(flattened) else 0
        return (lengths * bytes_per_element).astype(np.int64) + offset_width

    if pa.types.is_struct(column_type):
        # A struct row (e.g. a nested column's row) costs the sum of its fields.
        sizes = np.zeros(num_rows, dtype=np.int64)
        for field_index in range(column_type.num_fields):
            sizes += _arrow_column_mem_sizes(pc.struct_field(column, field_index))
        return sizes

    # Types outside the model (dictionary, union, ...): attribute the column's
    # total buffer size uniformly across rows.
    bytes_per_row = column.nbytes // num_rows if num_rows else 0
    return np.full(num_rows, bytes_per_row, dtype=np.int64)


def _item_mem_size(item):
    """Return the loaded-memory size of a single cell value, in bytes.

    This is the fallback measurement for pandas columns that cannot be
    represented in arrow (e.g. object columns with mixed content). Sizes are
    best-effort approximations of the loaded footprint.

    Args:
        item: a single cell value.

    Returns:
        int: the estimated memory size of ``item``, in bytes.
    """
    if isinstance(item, np.ndarray):
        # The loaded data is the array's buffer; the Python object wrapper is
        # not counted.
        return item.nbytes
    # For other types, sys.getsizeof is the best available approximation.
    return sys.getsizeof(item)


def get_mem_size_per_row(data, cols=None):
    """Estimate the memory footprint of each row when the data is loaded.

    Sizes model the columnar (arrow/numpy) representation the data occupies
    once loaded into memory: each row costs its slice of the column buffers.

    - fixed-width values (ints, floats, datetimes, ...): the value's byte
      width, with bit-packed types (booleans) counted as one byte per value;
    - strings and binary: the value's data bytes, plus one offset entry;
    - lists: element count x element byte width (variable-width elements are
      attributed their column-wide average), plus one offset entry;
    - structs (e.g. nested columns): the sum of their fields' contributions.

    Column types outside this model have their total buffer size attributed
    uniformly across rows, and null bitmaps are not counted. Per-object Python
    overheads are deliberately excluded: they describe the cost of
    materializing rows as Python objects, not of loading the data. Summing a
    group of rows' sizes therefore estimates the memory needed to load that
    group as a partition.

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

        mem_sizes = np.zeros(len(data), dtype=np.int64)
        for column in data.columns:
            series = data[column]
            try:
                # Convert to arrow (cheap for numeric and arrow-backed columns) so
                # pandas and pyarrow inputs are measured with the same model.
                arrow_column = pa.array(series, from_pandas=True)
            except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError, ValueError):
                # Columns arrow can't represent: best-effort per-value measurement.
                mem_sizes += np.fromiter((_item_mem_size(item) for item in series), np.int64, len(series))
                continue
            mem_sizes += _arrow_column_mem_sizes(arrow_column)
    elif isinstance(data, pa.Table):
        if cols is not None:
            data = data.select(cols)

        mem_sizes = np.zeros(data.num_rows, dtype=np.int64)
        for column in data.itercolumns():
            mem_sizes += _arrow_column_mem_sizes(column)
    else:
        raise NotImplementedError(f"Unsupported data type {type(data)} for memory size calculation")

    # Back to plain Python ints, matching this function's documented return type.
    return mem_sizes.tolist()
