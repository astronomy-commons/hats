import re
from pathlib import Path
from typing import Literal, Optional

import pandas as pd
from jproperties import Properties
from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator
from typing_extensions import Self
from upath import UPath

from hats.catalog.catalog_type import CatalogType
from hats.io import file_io


class ExtensionProperties(BaseModel):
    """Container class for the metadata of a catalog extension.

    An extension, described by an ``<extension>.properties`` file, holds additional columns
    for the rows of a primary catalog, and is joined to it on a pair of key columns.

    The extension data is referenced by path. A relative path is resolved against the directory
    holding the ``<extension>.properties`` file, and an absolute path, local or remote, is used
    as it is, so the data does not need to be co-located with the file. The file sits at the root
    of a collection, and names it as the primary, unless the primary is given by absolute path,
    local or remote.
    """

    name: str = Field(alias="obs_collection")
    """Name of the extension, which names its ``<extension>.properties`` file."""

    catalog_type: Literal[CatalogType.EXTENSION] = Field(
        default=CatalogType.EXTENSION, alias="dataproduct_type", validate_default=True
    )

    primary_catalog: str = Field(alias="hats_primary_table_url")
    """Primary catalog that the extension is joined to, or the collection holding it. Either the
    name of the collection holding the ``<extension>.properties`` file, or an absolute path,
    local or remote."""

    primary_column: str = Field(alias="hats_col_assn_primary")
    """Column name in the primary catalog to join on."""

    join_catalog: str = Field(alias="hats_assn_join_table_url")
    """Path to the extension data, a HATS catalog or a collection."""

    join_column: str = Field(alias="hats_col_assn_join")
    """Column name in the extension data that matches ``primary_column``."""

    extension_columns: Optional[list[str]] = Field(default=None, alias="hats_ext_cols")
    """The list of columns provided by the extension."""

    extension_join_style: Optional[Literal["left", "inner"]] = Field(
        default=None, alias="hats_ext_join_style"
    )
    """The type of join to use when joining the extension to its primary catalog."""

    extension_product_type: Optional[str] = Field(default=None, alias="hats_product_type_served")
    """Modality of the data that the extension stores."""

    ## Allow any extra keyword args to be stored on the properties object.
    model_config = ConfigDict(extra="allow", populate_by_name=True, use_enum_values=True)

    @field_validator("extension_columns", mode="before")
    @classmethod
    def space_delimited_list(cls, str_value: str) -> list[str] | None:
        """Convert a space-delimited list string into a python list of strings.

        Parameters
        ----------
        str_value: str
            a space-delimited list string

        Returns
        -------
        list[str] | None
            a python list of strings, or None if the string is empty
        """
        if str_value is None:
            return None
        if pd.api.types.is_list_like(str_value):
            return list(str_value)
        if not str_value or not isinstance(str_value, str):
            ## Convert empty strings and empty lists to None
            return None
        # Split on a few kinds of delimiters (just to be safe), and remove duplicates
        return list(filter(None, re.split(";| |,|\n", str_value)))

    @field_serializer("extension_columns")
    def serialize_as_space_delimited_list(self, str_list: list[str] | None) -> str | None:
        """Convert a python list of strings into a space-delimited string.

        Parameters
        ----------
        str_list: list[str] | None
            a python list of strings

        Returns
        -------
        str | None
            a space-delimited string, or None if the list is empty
        """
        if str_list is None or len(str_list) == 0:
            return None
        return " ".join(str_list)

    def __str__(self):
        """Friendly string representation based on named fields."""
        explicit = self.model_dump(by_alias=False, exclude_none=True)
        extra_keys = self.__pydantic_extra__.keys()
        formatted_string = ""
        for name, value in explicit.items():
            if name not in extra_keys:
                formatted_string += f"  {name} {value}\n"
        return formatted_string

    @classmethod
    def read_from_file(cls, file_path: str | Path | UPath) -> Self:
        """Read field values from a java-style properties file.

        Parameters
        ----------
        file_path: str | Path | UPath
            path to an ``<extension>.properties`` file.

        Returns
        -------
        ExtensionProperties
            new object from the contents of the file.
        """
        file_path = file_io.get_upath(file_path)
        p = Properties()
        with file_path.open("rb") as f:
            p.load(f, "utf-8")
        return cls(**p.properties)

    def to_properties_file(self, catalog_dir: str | Path | UPath):
        """Write fields to a java-style ``<extension>.properties`` file, named after the extension.

        Parameters
        ----------
        catalog_dir: str | Path | UPath
            directory to write the file to.
        """
        # pylint: disable=protected-access,duplicate-code
        parameters = self.model_dump(by_alias=True, exclude_none=True)
        properties = Properties(process_escapes_in_values=False)
        properties.properties = parameters
        properties._key_order = parameters.keys()
        file_path = file_io.get_upath(catalog_dir) / f"{self.name}.properties"
        with file_path.open("wb") as _file:
            properties.store(_file, encoding="utf-8", initial_comments="HATS Extension", timestamp=False)
