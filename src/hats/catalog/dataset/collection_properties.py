import re
from pathlib import Path
from typing import Annotated, Optional

import pandas as pd
from pydantic import (
    BeforeValidator,
    Field,
    PlainSerializer,
    field_validator,
    model_validator,
)
from typing_extensions import Self
from upath import UPath

from hats.catalog.dataset.hats_properties import HatsProperties
from hats.io import file_io


class CollectionProperties(HatsProperties):
    """Container class for catalog metadata"""

    name: str = Field(alias="obs_collection")

    hats_primary_table_url: str = Field(..., alias="hats_primary_table_url")
    """Reference to object catalog. Relevant for nested, margin, association, and index."""

    all_margins: Annotated[
        Optional[list[str]],
        Field(default=None),
        PlainSerializer(HatsProperties.serialize_list_as_space_delimited_list),
        BeforeValidator(HatsProperties.space_delimited_list),
    ]
    default_margin: Optional[str] = Field(default=None)

    all_indexes: Annotated[
        Optional[dict[str, str]],
        Field(default=None),
        PlainSerializer(HatsProperties.serialize_dict_as_space_delimited_list),
    ]
    default_index: Optional[str] = Field(default=None)

    all_extensions: Annotated[
        Optional[list[str]],
        Field(default=None),
        PlainSerializer(HatsProperties.serialize_list_as_space_delimited_list),
        BeforeValidator(HatsProperties.space_delimited_list),
    ]
    """Extensions of this collection, each holding a set of additional columns, and described
    by an ``<extension>.properties`` file. Each is listed by the path to that file, relative to the
    collection root or absolute. The ``.properties`` suffix may be left out."""

    @field_validator("all_indexes", mode="before")
    @classmethod
    def index_tuples(cls, str_value: str) -> dict[str, str]:
        """Convert a space-delimited list string into a python list of strings.

        Parameters
        ----------
        str_value: str
            a space-delimited list string

        Returns
        -------
        dict[str, str]
            a python dict of strings

        Raises
        ------
        ValueError
            if the string list has an odd number of elements (and so is not pairs of
            field and index name)
        """
        if str_value is None:
            return None
        if pd.api.types.is_dict_like(str_value):
            return dict(str_value)
        if not str_value or not isinstance(str_value, str):
            return None
        # Split on a few kinds of delimiters (just to be safe), and remove duplicates
        str_values = list(filter(None, re.split(";| |,|\n", str_value)))
        ## Convert empty strings and empty lists to None
        if len(str_values) % 2 != 0:
            raise ValueError("Collection all_indexes map should contain pairs of field and index name")
        all_index_dict = {}
        for index_start in range(0, len(str_values), 2):
            key = str_values[index_start]
            value = str_values[index_start + 1]
            all_index_dict[key] = value
        return all_index_dict

    @model_validator(mode="after")
    def check_default_margin_exists(self) -> Self:
        """Check that the default margin is in the list of all margins."""
        if self.default_margin is not None:
            if self.all_margins is None:
                raise ValueError("all_margins needs to be set if default_margin is set")
            if self.default_margin not in self.all_margins:
                raise ValueError(f"default_margin `{self.default_margin}` not found in all_margins")
        return self

    @model_validator(mode="after")
    def check_default_index_exists(self) -> Self:
        """Check that the default index is in the list of all indexes."""
        if self.default_index is not None:
            if self.all_indexes is None:
                raise ValueError("all_indexes needs to be set if default_index is set")
            if self.default_index not in self.all_indexes:
                raise ValueError(f"default_index `{self.default_index}` not found in all_indexes")
        return self

    @classmethod
    def read_from_dir(cls, catalog_dir: str | Path | UPath) -> Self:
        """Read field values from a java-style properties file.

        Parameters
        ----------
        catalog_dir: str | Path | UPath
            base directory of catalog.

        Returns
        -------
        CollectionProperties
            new object from the contents of a ``collection.properties`` file in the directory.
        """
        return cls.read_from_file(file_io.get_upath(catalog_dir) / "collection.properties")

    def to_properties_file(self, catalog_dir: str | Path | UPath):
        """Write fields to a java-style properties file.

        Parameters
        ----------
        catalog_dir: str | Path | UPath
            base directory of catalog.
        """
        self.to_properties_file_path(
            file_io.get_upath(catalog_dir) / "collection.properties", initial_comments="HATS Collection"
        )
