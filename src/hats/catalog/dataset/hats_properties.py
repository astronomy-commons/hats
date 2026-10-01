import re
from datetime import datetime, timezone
from functools import reduce
from importlib.metadata import version
from pathlib import Path
from typing import Iterable

import pandas as pd
from jproperties import Properties
from pydantic import BaseModel, ConfigDict
from typing_extensions import Self
from upath import UPath

from hats.io import size_estimates

# Fields used by data providers to describe the observations, or creation of
# the dataset.
KNOWN_PROVENANCE_FIELDS = [
    "addendum_description",
    "addendum_did",
    "bib_reference",
    "bib_reference_url",
    "creator_did",
    "creator_did",
    "hats_builder",
    "hats_copyright",
    "hats_creation_date",
    "hats_creator",
    "hats_data_semver",
    "hats_progenitor_url",
    "hats_release_date",
    "hats_successor_url",
    "hats_successor_description",
    "hats_version",
    "obs_ack",
    "obs_copyright",
    "obs_copyright_url",
    "obs_description",
    "obs_title",
    "prov_progenitor",
    "publisher_id",
]


class HatsProperties(BaseModel):
    """Container class for catalog metadata"""

    ## Allow any extra keyword args to be stored on the properties object.
    model_config = ConfigDict(extra="allow", populate_by_name=True, use_enum_values=True)

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

    @classmethod
    def space_delimited_int_list(cls, str_value: str | list[int]) -> list[int]:
        """Convert a space-delimited list string into a python list of integers.

        Parameters
        ----------
        str_value : str | list[int]
            string representation of a list of integers, delimited by
            space, comma, or semicolon, or a list of integers.

        Returns
        -------
        list[int]
            a python list of integers

        Raises
        ------
        ValueError
            if any non-digit characters are encountered
        """
        if not str_value:
            return None
        if isinstance(str_value, int):
            return [str_value]
        if isinstance(str_value, str):
            # Split on a few kinds of delimiters (just to be safe)
            int_list = [int(token) for token in list(filter(None, re.split(";| |,|\n", str_value)))]
        elif isinstance(str_value, list) and all(isinstance(elem, int) for elem in str_value):
            int_list = str_value
        else:
            raise ValueError(f"Unsupported type of skymap_alt_orders {type(str_value)}")
        if len(int_list) == 0:
            return None
        int_list = list(set(int_list))
        int_list.sort()
        return int_list

    @classmethod
    def serialize_list_as_space_delimited_list(cls, str_list: Iterable[str]) -> str:
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
            return None
        return " ".join([str(element) for element in str_list])

    @classmethod
    def serialize_dict_as_space_delimited_list(cls, str_dict: dict[str, str]) -> str:
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

    def explicit_dict(self, by_alias=False, exclude_none=True):
        """Create a dict, based on fields that have been explicitly set, and are not "extra" keys.

        Parameters
        ----------
        by_alias : bool
            (Default value = False)
        exclude_none : bool
            (Default value = True)

        Returns
        -------
        dict
            all keys that are attributes of this class and not "extra".
        """
        explicit = self.model_dump(by_alias=by_alias, exclude_none=exclude_none)
        extra_keys = self.__pydantic_extra__.keys()
        return {key: val for key, val in explicit.items() if key not in extra_keys}

    def provenance_dict(self, by_alias=False, exclude_none=True):
        """Create a dict, based on known provenance fields, that have been explicitly set.

        Parameters
        ----------
        by_alias : bool
            (Default value = False)
        exclude_none : bool
            (Default value = True)

        Returns
        -------
        dict
            all keys that are attributes of this class and in a list of known provenance fields.
        """
        explicit = self.model_dump(by_alias=by_alias, exclude_none=exclude_none)
        return {key: val for key, val in explicit.items() if key in KNOWN_PROVENANCE_FIELDS}

    def extra_dict(self, by_alias=False, exclude_none=True):
        """Create a dict, based on fields that are "extra" keys.

        Parameters
        ----------
        by_alias : bool
            (Default value = False)
        exclude_none : bool
            (Default value = True)

        Returns
        -------
        dict
            all keys that are *not* attributes of this class, e.g. "extra".
        """
        explicit = self.model_dump(by_alias=by_alias, exclude_none=exclude_none)
        extra_keys = self.__pydantic_extra__.keys()
        return {key: val for key, val in explicit.items() if key in extra_keys}

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        """Friendly string representation based on named fields."""
        parameters = self.explicit_dict()
        longest_length = max(len(key) for key in parameters.keys())
        formatted_string = ""
        for name, value in parameters.items():
            formatted_string += f"{name.ljust(longest_length)} {value}\n"
        return formatted_string

    @classmethod
    def read_from_file(cls, file_path: UPath) -> Self:
        """Read field values from a java-style properties file.

        Parameters
        ----------
        file_path: UPath
            path to properties file.

        Returns
        -------
        CollectionProperties
            new object from the contents of a ``collection.properties`` file in the directory.
        """
        p = Properties()
        with file_path.open("rb") as f:
            p.load(f, "utf-8")
        return cls(**p.properties)

    def to_properties_file_path(self, file_path: UPath, **kwargs):
        """Write fields to a java-style properties file.

        Parameters
        ----------
        file_path: UPath
            path to properties file.
        """
        # pylint: disable=protected-access
        parameters = self.model_dump(by_alias=True, exclude_none=True)
        properties = Properties(process_escapes_in_values=False)
        properties.properties = parameters
        properties._key_order = parameters.keys()
        with file_path.open("wb") as _file:
            properties.store(_file, encoding="utf-8", timestamp=False, **kwargs)

    @staticmethod
    def new_provenance_dict(
        path: str | Path | UPath | None = None, builder: str | None = None, **kwargs
    ) -> dict:
        """Constructs the provenance properties for a HATS catalog.

        Parameters
        ----------
        path: str | Path | UPath | None
            The path to the catalog directory.
        builder : str | None
            The name and version of the tool that created the catalog.
        **kwargs
            Additional properties to include/override in the dictionary.

        Returns
        -------
        dict
            A dictionary with properties for the HATS catalog.
        """
        builder_str = ""
        if builder is not None:
            builder_str = f"{builder}, "
        builder_str += f"hats v{version('hats')}"

        properties = {}
        now = datetime.now(tz=timezone.utc)
        properties["hats_builder"] = builder_str
        properties["hats_creation_date"] = now.strftime("%Y-%m-%dT%H:%M%Z")
        properties["hats_estsize"] = size_estimates.estimate_dir_size(path, divisor=1024)
        properties["hats_release_date"] = "2025-08-22"
        properties["hats_version"] = "v1.0"
        return kwargs | properties
