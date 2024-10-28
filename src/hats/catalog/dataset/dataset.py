from __future__ import annotations

from pathlib import Path

import pyarrow as pa
from upath import UPath

from hats.catalog.dataset.table_properties import TableProperties
from hats.io import file_io


# pylint: disable=too-few-public-methods
class Dataset:
    """A base HATS dataset that contains a properties file
    and the data contained in parquet files"""

    def __init__(
        self,
        catalog_info: TableProperties,
        catalog_path: str | Path | UPath | None = None,
        schema: pa.Schema | None = None,
    ) -> None:
        """Initializes a Dataset

        Args:
            catalog_info: A TableProperties object with the catalog metadata
            catalog_path: If the catalog is stored on disk, specify the location of the catalog
                Does not load the catalog from this path, only store as metadata
            schema (pa.Schema): The pyarrow schema for the catalog
        """
        self.catalog_info = catalog_info
        self.catalog_name = self.catalog_info.catalog_name

        self.catalog_path = catalog_path
        self.on_disk = catalog_path is not None
        self.catalog_base_dir = file_io.get_upath(self.catalog_path)

        self.schema = schema
