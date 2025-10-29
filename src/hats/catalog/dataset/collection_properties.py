import re
from functools import reduce
from pathlib import Path
from typing import Annotated, Iterable, Optional

import pandas as pd
from jproperties import Properties
from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator, model_validator
from typing_extensions import Self
from upath import UPath

from hats.io import file_io

# All additional properties in the HATS recommendation.
EXTRA_ALLOWED_FIELDS = [
    "addendum_did",
    "bib_reference",
    "bib_reference_url",
    "creator_did",
    "data_ucd",
    "hats_builder",
    "hats_coordinate_epoch",
    "hats_copyright",
    "hats_creation_date",
    "hats_creator",
    "hats_estsize",
    "hats_progenitor_url",
    "hats_release_date",
    "hats_service_url",
    "hats_status",
    "hats_version",
    "moc_sky_fraction",
    "obs_ack",
    "obs_copyright",
    "obs_copyright_url",
    "obs_description",
    "obs_regime",
    "obs_title",
    "prov_progenitor",
    "publisher_id",
    "t_max",
    "t_min",
]


class CollectionProperties(BaseModel):
    """Container class for catalog metadata"""

    name: str = Field(alias="obs_collection")

    hats_primary_table_url: str = Field(..., alias="hats_primary_table_url")
    """Reference to object catalog. Relevant for nested, margin, association, and index."""

    all_margins: Annotated[Optional[list[str]], Field(default=None)]
    default_margin: Optional[str] = Field(default=None)

    all_indexes: Annotated[Optional[dict[str, str]], Field(default=None)]
    default_index: Optional[str] = Field(default=None)

    ## Allow any extra keyword args to be stored on the properties object.
    model_config = ConfigDict(extra="allow", populate_by_name=True, use_enum_values=True)

    @field_validator("all_margins", mode="before")
    @classmethod
    def space_delimited_list(cls, str_value: str) -> list[str]:
        """Convert a space-delimited list string into a python list of strings.

        Parameters
        ----------
        str_value: str
            a space-delimited list string

        Returns
        -------
        list[str]
            a python list of strings
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

    @field_serializer("all_margins")
    def serialize_list_as_space_delimited_list(self, str_list: Iterable[str]) -> str:
        """Convert a python list of strings into a space-delimited string.

        Parameters
        ----------
        str_list: Iterable[str]
            a python list of strings

        Returns
        -------
        str
            a space-delimited string
        """
        if str_list is None or len(str_list) == 0:
            return ""
        return " ".join(str_list)

    @field_serializer("all_indexes")
    def serialize_dict_as_space_delimited_list(self, str_dict: dict[str, str]) -> str:
        """Convert a python list of strings into a space-delimited string.

        Parameters
        ----------
        str_dict: dict[str, str]
            a python dict of strings

        Returns
        -------
        str
            a space-delimited string
        """
        if str_dict is None or len(str_dict) == 0:
            return ""
        str_list = list(reduce(lambda x, y: x + y, str_dict.items()))
        return " ".join(str_list)

    @model_validator(mode="after")
    def check_allowed_and_required(self) -> Self:
        """Check that type-specific fields are appropriate, and required fields are set."""
        # Check against all known properties - catches typos.
        non_allowed = set(self.__pydantic_extra__.keys()) - set(EXTRA_ALLOWED_FIELDS)
        if len(non_allowed) > 0:
            raise ValueError(f"Unexpected extra property: {non_allowed}")
        return self

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

    def explicit_dict(self):
        """Create a dict, based on fields that have been explicitly set, and are not "extra" keys."""
        explicit = self.model_dump(by_alias=False, exclude_none=True)
        extra_keys = self.__pydantic_extra__.keys()
        return {key: val for key, val in explicit.items() if key not in extra_keys}

    def __str__(self):
        """Friendly string representation based on named fields."""
        parameters = self.explicit_dict()
        formatted_string = ""
        for name, value in parameters.items():
            formatted_string += f"  {name} {value}\n"
        return formatted_string

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
        file_path = file_io.get_upath(catalog_dir) / "collection.properties"
        if not file_io.does_file_or_directory_exist(file_path):
            raise FileNotFoundError(f"No properties file found where expected: {str(file_path)}")
        p = Properties()
        with file_path.open("rb") as f:
            p.load(f, "utf-8")
        return cls(**p.properties)

    def to_properties_file(self, catalog_dir: str | Path | UPath):
        """Write fields to a java-style properties file.

        Parameters
        ----------
        catalog_dir: str | Path | UPath
            base directory of catalog.
        """
        # pylint: disable=protected-access
        parameters = self.model_dump(by_alias=True, exclude_none=True)
        properties = Properties(process_escapes_in_values=False)
        properties.properties = parameters
        properties._key_order = parameters.keys()
        file_path = file_io.get_upath(catalog_dir) / "collection.properties"
        with file_path.open("wb") as _file:
            properties.store(_file, encoding="utf-8", initial_comments="HATS Collection", timestamp=False)
