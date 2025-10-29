from __future__ import annotations

import os
import tempfile
from collections.abc import Generator
from io import BytesIO
from pathlib import Path

import nested_pandas as npd
import numpy as np
import pandas as pd
import pyarrow.dataset as pds
import pyarrow.parquet as pq
import upath.implementations.http
from cdshealpix.skymap.skymap import Skymap
from pyarrow.dataset import Dataset
from upath import UPath

from hats.io.file_io.file_pointer import get_upath


def make_directory(file_pointer: str | Path | UPath, exist_ok: bool = False):
    """Make a directory at a given file pointer

    Will raise an error if a directory already exists, unless `exist_ok` is True in which case
    any existing directories will be left unmodified.

    Parameters
    ----------
    file_pointer: str | Path | UPath
        location in file system to make directory
    exist_ok: bool
        (Default value = False)
        If false will raise error if directory exists. If true existing
        directories will be ignored and not modified
    """
    file_pointer = get_upath(file_pointer)
    file_pointer.mkdir(parents=True, exist_ok=exist_ok)


def _rmdir_recursive(directory):
    for item in directory.iterdir():
        if item.is_dir():
            _rmdir_recursive(item)
        else:
            item.unlink()
    directory.rmdir()


def remove_directory(file_pointer: str | Path | UPath, ignore_errors=False):
    """Remove a directory, and all contents, recursively.

    Parameters
    ----------
    file_pointer: str | Path | UPath
        directory in file system to remove
    ignore_errors : bool
        (Default value = False)
        if True errors resulting from failed removals will be ignored
    """
    file_pointer = get_upath(file_pointer)
    if ignore_errors:
        try:
            _rmdir_recursive(file_pointer)
        except Exception:  # pylint: disable=broad-except
            # fsspec doesn't have a "ignore_errors" field in the rm method
            pass
    else:
        _rmdir_recursive(file_pointer)


def write_string_to_file(file_pointer: str | Path | UPath, string: str, encoding: str = "utf-8"):
    """Write a string to a text file

    Parameters
    ----------
    file_pointer: str | Path | UPath
        file location to write file to
    string: str
        string to write to file
    encoding: str
         (Default value = "utf-8") encoding method to write to file with
    """
    file_pointer = get_upath(file_pointer)
    with file_pointer.open("w", encoding=encoding) as _file:
        _file.write(string)


def load_text_file(file_pointer: str | Path | UPath, encoding: str = "utf-8"):
    """Load a text file content to a list of strings.

    Parameters
    ----------
    file_pointer: str | Path | UPath
        location of file to read
    encoding: str
         (Default value = "utf-8") string encoding method used by the file

    Returns
    -------
    str
        full string contents of the file as a list of strings, one per line.
    """
    file_pointer = get_upath(file_pointer)
    with file_pointer.open("r", encoding=encoding) as _text_file:
        text_file = _text_file.readlines()

    return text_file


def load_csv_to_pandas(file_pointer: str | Path | UPath, **kwargs) -> pd.DataFrame:
    """Load a csv file to a pandas dataframe

    Parameters
    ----------
    file_pointer: str | Path | UPath
        location of csv file to load
    **kwargs
        arguments to pass to pandas ``read_csv`` loading method

    Returns
    -------
    pd.DataFrame
        contents of the CVS file, as a dataframe.
    """
    file_pointer = get_upath(file_pointer)
    with file_pointer.open("r") as csv_file:
        frame = pd.read_csv(csv_file, **kwargs)
    return frame


def load_csv_to_pandas_generator(
    file_pointer: str | Path | UPath, *, chunksize=10_000, compression=None, **kwargs
) -> Generator[pd.DataFrame]:
    """Load a csv file to a pandas dataframe

    Parameters
    ----------
    file_pointer: str | Path | UPath
        location of csv file to load
    chunksize : int
        (Default value = 10_000) number of rows to load per chunk
    compression : str
        (Default value = None) for compressed CSVs, the manner of compression. e.g. 'gz', 'bzip'.
    **kwargs
        arguments to pass to pandas ``read_csv`` loading method

    Yields
    ------
    pd.DataFrame
        chunked contents of the CVS file, as a dataframe.
    """
    file_pointer = get_upath(file_pointer)
    with file_pointer.open(mode="rb", compression=compression) as csv_file:
        with pd.read_csv(csv_file, chunksize=chunksize, **kwargs) as reader:
            yield from reader


def write_dataframe_to_csv(dataframe: pd.DataFrame, file_pointer: str | Path | UPath, **kwargs):
    """Write a pandas DataFrame to a CSV file

    Parameters
    ----------
    dataframe: pd.DataFrame
        DataFrame to write
    file_pointer: str | Path | UPath
        location of file to write to
    **kwargs
        args to pass to pandas ``to_csv`` method
    """
    output = dataframe.to_csv(**kwargs)
    write_string_to_file(file_pointer, output)


def write_dataframe_to_parquet(dataframe: pd.DataFrame, file_pointer):
    """Write a pandas DataFrame to a parquet file

    Parameters
    ----------
    dataframe: pd.DataFrame
        DataFrame to write
    file_pointer : str | Path | UPath
        location of file to write to
    """
    file_pointer = get_upath(file_pointer)
    dataframe.to_parquet(file_pointer.path, filesystem=file_pointer.fs)


def _parquet_precache_all_bytes(file_pointer):  # pragma: no cover
    if not isinstance(file_pointer, upath.implementations.http.HTTPPath):
        return False
    cache_options = file_pointer.fs.cache_options or {}
    if "parquet_precache_all_bytes" not in cache_options:
        return False
    return cache_options["parquet_precache_all_bytes"]


def read_parquet_metadata(file_pointer: str | Path | UPath, **kwargs) -> pq.FileMetaData:
    """Read FileMetaData from footer of a single Parquet file.

    Parameters
    ----------
    file_pointer: str | Path | UPath
        location of file to read metadata from
    **kwargs
        additional arguments to be passed to pyarrow.parquet.read_metadata

    Returns
    -------
    pq.FileMetaData
        parqeut file metadata (includes schema)
    """
    file_pointer = get_upath(file_pointer)
    if file_pointer is None or not file_pointer.exists():
        raise FileNotFoundError("Parquet file does not exist")
    if _parquet_precache_all_bytes(file_pointer):  # pragma: no cover
        return pq.read_metadata(BytesIO(file_pointer.read_bytes()), **kwargs)

    return pq.read_metadata(file_pointer.path, filesystem=file_pointer.fs, **kwargs)


def read_parquet_file(file_pointer: str | Path | UPath, **kwargs) -> pq.ParquetFile:
    """Read single parquet file.

    Parameters
    ----------
    file_pointer: str | Path | UPath
        location of parquet file
    **kwargs
        additional arguments to be passed to pyarrow.parquet.ParquetFile

    Returns
    -------
    pq.ParquetFile
        full contents of parquet file
    """
    file_pointer = get_upath(file_pointer)
    if file_pointer is None or not file_pointer.exists():
        raise FileNotFoundError("Parquet file does not exist")

    if _parquet_precache_all_bytes(file_pointer):  # pragma: no cover
        return pq.ParquetFile(BytesIO(file_pointer.read_bytes()), **kwargs)

    return pq.ParquetFile(file_pointer.path, filesystem=file_pointer.fs, **kwargs)


def read_parquet_dataset(
    source: str | Path | UPath | list[str | Path | UPath], **kwargs
) -> tuple[str | list[str], Dataset]:
    """Read parquet dataset from directory pointer or list of files.

    Note that pyarrow.dataset reads require that directory pointers don't contain a
    leading slash, and the protocol prefix may additionally be removed. As such, we also return
    the directory path that is formatted for pyarrow ingestion for follow-up.

    See more info on source specification and possible kwargs at
    https://arrow.apache.org/docs/python/generated/pyarrow.dataset.dataset.html

    Parameters
    ----------
    source: str | Path | UPath | list[str | Path | UPath]
        directory, path, or list of paths to read data from
    **kwargs
        additional arguments passed to ``pyarrow.dataset.dataset``

    Returns
    -------
    tuple[str | list[str], Dataset]
        Tuple containing a path to the dataset (that is formatted for pyarrow ingestion)
        and the dataset read from disk.
    """
    if pd.api.types.is_list_like(source) and len(source) > 0:
        upaths = [get_upath(path) for path in source]
        file_system = upaths[0].fs
        source = [path.path for path in upaths]
    else:
        source = get_upath(source)
        file_system = source.fs
        source = source.path

    dataset = pds.dataset(
        source,
        filesystem=file_system,
        format="parquet",
        **kwargs,
    )
    return (source, dataset)


def write_parquet_metadata(
    schema, file_pointer: str | Path | UPath, metadata_collector: list | None = None, **kwargs
):
    """Write a metadata only parquet file from a schema

    Parameters
    ----------
    schema : pa.Schema
        pyarrow schema to be written
    file_pointer: str | Path | UPath
        location of file to be written to
    metadata_collector: list | None
        (Default value = None) where to collect metadata information
    **kwargs
        additional arguments to be passed to pyarrow.parquet.write_metadata
    """
    file_pointer = get_upath(file_pointer)
    pq.write_metadata(
        schema, file_pointer.path, metadata_collector=metadata_collector, filesystem=file_pointer.fs, **kwargs
    )


def read_fits_image(map_file_pointer: str | Path | UPath) -> np.ndarray:
    """Read the object spatial distribution information from a healpix FITS file.

    Parameters
    ----------
    map_file_pointer: str | Path | UPath
        location of file to be read

    Returns
    -------
    np.ndarray
        one-dimensional numpy array of integers where the
        value at each index corresponds to the number of objects found at the healpix pixel.
    """
    map_file_pointer = get_upath(map_file_pointer)
    with tempfile.NamedTemporaryFile(delete=False) as _tmp_file:
        with map_file_pointer.open("rb") as _map_file:
            _tmp_file.write(_map_file.read())
        tmp_path = _tmp_file.name
    try:
        return Skymap.from_fits(tmp_path).values
    finally:
        os.unlink(tmp_path)


def write_fits_image(histogram: np.ndarray, map_file_pointer: str | Path | UPath):
    """Write the object spatial distribution information to a healpix FITS file.

    Parameters
    ----------
    histogram: np.ndarray
        one-dimensional numpy array of long integers where the
        value at each index corresponds to the number of objects found at the healpix pixel.
    map_file_pointer: str | Path | UPath
        location of file to be written
    """
    map_file_pointer = get_upath(map_file_pointer)
    with tempfile.NamedTemporaryFile() as _tmp_file:
        with map_file_pointer.open("wb") as _map_file:
            skymap = Skymap.from_array(histogram)
            skymap.to_fits(_tmp_file.name)
            _map_file.write(_tmp_file.read())


def delete_file(file_handle: str | Path | UPath):
    """Deletes file from filesystem.

    Parameters
    ----------
    file_handle: str | Path | UPath
        location of file pointer
    """
    file_handle = get_upath(file_handle)
    file_handle.unlink()


def read_parquet_file_to_pandas(file_pointer: str | Path | UPath, **kwargs) -> npd.NestedFrame:
    """Reads parquet file(s) to a pandas DataFrame

    Parameters
    ----------
    file_pointer: str | Path | UPath
        File Pointer to a parquet file or a directory containing parquet files
    **kwargs
        Additional arguments to pass to pandas read_parquet method

    Returns
    -------
    NestedFrame
        Pandas DataFrame with the data from the parquet file(s)
    """
    file_pointer = get_upath(file_pointer)
    # If we are trying to read a remote directory, we need to send the explicit list of files instead.
    # We don't want to get the list unnecessarily because it can be expensive.
    if file_pointer.protocol not in ("", "file") and file_pointer.is_dir():  # pragma: no cover
        file_pointers = [f for f in file_pointer.iterdir() if f.is_file()]
        return npd.read_parquet(
            file_pointers,
            filesystem=file_pointer.fs,
            partitioning=None,  # Avoid the ArrowTypeError described in #367
            **kwargs,
        )
    if _parquet_precache_all_bytes(file_pointer):  # pragma: no cover
        return npd.read_parquet(BytesIO(file_pointer.read_bytes()), partitioning=None, **kwargs)
    return npd.read_parquet(
        file_pointer.path,
        filesystem=file_pointer.fs,
        partitioning=None,  # Avoid the ArrowTypeError described in #367
        **kwargs,
    )
