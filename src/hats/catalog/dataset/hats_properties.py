import re
from functools import reduce
from typing import Iterable

import pandas as pd
from jproperties import Properties
from pydantic import BaseModel, ConfigDict
from typing_extensions import Self
from upath import UPath


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
