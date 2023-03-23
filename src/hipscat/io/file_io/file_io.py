import json
import os
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

from hipscat.io.file_io.file_pointer import FilePointer


def make_directory(file_pointer: FilePointer, exist_ok: bool = False):
    """Make a directory at a given file pointer

    Will raise an error if a directory already exists, unless `exist_ok` is True in which case
    any existing directories will be left unmodified

    Args:
        file_pointer: location in file system to make directory
        exist_ok: Default False. If false will raise error if directory exists. If true existing
        directories will be ignored and not modified

    Raises:
        OSError
    """
    os.makedirs(file_pointer, exist_ok=exist_ok)


def write_string_to_file(
    file_pointer: FilePointer, string: str, encoding: str = "utf-8"
):
    """Write a string to a text file

    Args:
        file_pointer: file location to write file to
        string: string to write to file
        encoding: Default: 'utf-8', encoding method to write to file with
    """
    with open(file_pointer, "w", encoding=encoding) as file:
        file.write(string)


def load_json_file(file_pointer: FilePointer, encoding: str = "utf-8") -> dict:
    """Load a json file to a dictionary

    Args:
        file_pointer: location of file to read
        encoding: string encoding method used by the file
    Returns:
        dictionary of key value pairs loaded from the JSON file
    """
    json_dict = None
    with open(file_pointer, "r", encoding=encoding) as json_file:
        json_dict = json.load(json_file)
    return json_dict


def load_csv_to_pandas(file_pointer: FilePointer, **kwargs) -> pd.DataFrame:
    """Load a csv file to a pandas dataframe

    Args:
        file_pointer: location of csv file to load
        **kwargs: arguments to pass to pandas `read_csv` loading method
    Returns:
        pandas dataframe loaded from CSV
    """
    return pd.read_csv(file_pointer, **kwargs)


def write_dataframe_to_csv(
    dataframe: pd.DataFrame, file_pointer: FilePointer, **kwargs
):
    """Write a pandas DataFrame to a CSV file

    Args:
        dataframe: DataFrame to write
        file_pointer: location of file to write to
        **kwargs: args to pass to pandas `to_csv` method
    """
    dataframe.to_csv(file_pointer, **kwargs)


def read_parquet_metadata(file_pointer: FilePointer, **kwargs) -> pq.FileMetaData:
    """Read FileMetaData from footer of a single Parquet file.

    Args:
        file_pointer: location of file to read metadata from
        **kwargs: additional arguments to be passed to pyarrow.parquet.read_metadata
    """
    return pq.read_metadata(file_pointer, **kwargs)


def write_parquet_metadata(
    schema: Any, file_pointer: FilePointer, metadata_collector: list = None, **kwargs
):
    """Write a metadata only parquet file from a schema

    Args:
        schema: schema to be written
        file_pointer: location of file to be written to
        metadata_collector: where to collect metadata information
        **kwargs: additional arguments to be passed to pyarrow.parquet.write_metadata
    """
    pq.write_metadata(
        schema, file_pointer, metadata_collector=metadata_collector, **kwargs
    )