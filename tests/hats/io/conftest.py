"""Set of convenience methods for testing file contents"""

import re

import numpy.testing as npt
import pyarrow.parquet as pq
import pytest

from hats.io import file_io
from hats.io.file_io.file_io import load_text_file
from hats.io.file_io.file_pointer import does_file_or_directory_exist

# pylint: disable=missing-function-docstring, redefined-outer-name


@pytest.fixture
def assert_text_file_matches():
    def assert_text_file_matches(expected_lines, file_name):
        """Convenience method to read a text file and compare the contents, line for line.

        When file contents get even a little bit big, it can be difficult to see
        the difference between an actual file and the expected contents without
        increased testing verbosity. This helper compares files line-by-line,
        using the provided strings or regular expressions.

        Notes:
            Because we check strings as regular expressions, you may need to escape some
            contents of `expected_lines`.

        Args:
            expected_lines(:obj:`string array`) list of strings, formatted as regular expressions.
            file_name (str): fully-specified path of the file to read
        """
        assert does_file_or_directory_exist(file_name), f"file not found [{file_name}]"
        contents = load_text_file(file_name)

        assert len(expected_lines) == len(
            contents
        ), f"files not the same length ({len(contents)} vs {len(expected_lines)})"
        for i, expected in enumerate(expected_lines):
            assert re.match(expected, contents[i]), (
                f"files do not match at line {i+1} " f"(actual: [{contents[i]}] vs expected: [{expected}])"
            )

    return assert_text_file_matches


@pytest.fixture
def check_parquet_schema():
    def check_parquet_schema(file_name, expected_schema, expected_num_row_groups=1):
        """Check parquet schema against expectations"""
        assert file_io.does_file_or_directory_exist(file_name), f"file not found [{file_name}]"

        single_metadata = file_io.read_parquet_metadata(file_name)
        schema = single_metadata.schema.to_arrow_schema()

        assert len(schema) == len(
            expected_schema
        ), f"object list not the same size ({len(schema)} vs {len(expected_schema)})"

        npt.assert_array_equal(schema.names, expected_schema.names)

        assert schema.equals(expected_schema, check_metadata=False)

        parquet_file = pq.ParquetFile(file_name)
        assert parquet_file.metadata.num_row_groups == expected_num_row_groups

        for row_index in range(0, parquet_file.metadata.num_row_groups):
            row_md = parquet_file.metadata.row_group(row_index)
            for column_index in range(0, row_md.num_columns):
                column_metadata = row_md.column(column_index)
                assert column_metadata.file_path.endswith(".parquet")

    return check_parquet_schema
