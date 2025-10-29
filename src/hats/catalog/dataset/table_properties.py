import re
from datetime import datetime, timezone
from importlib.metadata import version
from pathlib import Path
from typing import Iterable, Optional

from jproperties import Properties
from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator, model_validator
from typing_extensions import Self
from upath import UPath

from hats.catalog.catalog_type import CatalogType
from hats.io import file_io

## catalog_name, catalog_type, and total_rows are required for ALL types
CATALOG_TYPE_REQUIRED_FIELDS = {
    CatalogType.OBJECT: ["ra_column", "dec_column"],
    CatalogType.SOURCE: ["ra_column", "dec_column"],
    CatalogType.ASSOCIATION: [
        "primary_catalog",
        "primary_column",
        "join_catalog",
        "join_column",
        "contains_leaf_files",
    ],
    CatalogType.INDEX: ["primary_catalog", "indexing_column"],
    CatalogType.MARGIN: ["primary_catalog", "margin_threshold"],
    CatalogType.MAP: [],
}


class TableProperties(BaseModel):
    """Container class for catalog metadata"""

    catalog_name: str = Field(alias="obs_collection")
    catalog_type: CatalogType = Field(alias="dataproduct_type")
    total_rows: int = Field(alias="hats_nrows")

    ra_column: Optional[str] = Field(default=None, alias="hats_col_ra")
    dec_column: Optional[str] = Field(default=None, alias="hats_col_dec")
    default_columns: Optional[list[str]] = Field(default=None, alias="hats_cols_default")
    """Which columns should be read from parquet files, when user doesn't otherwise specify."""

    healpix_column: Optional[str] = Field(default=None, alias="hats_col_healpix")
    """Column name that provides a spatial index of healpix values at some fixed, high order.
    A typical value would be ``_healpix_29``, but can vary."""

    healpix_order: Optional[int] = Field(default=None, alias="hats_col_healpix_order")
    """For the spatial index of healpix values in ``hats_col_healpix``
    what is the fixed, high order. A typicaly value would be 29, but can vary."""

    primary_catalog: Optional[str] = Field(default=None, alias="hats_primary_table_url")
    """Reference to object catalog. Relevant for nested, margin, association, and index."""

    margin_threshold: Optional[float] = Field(default=None, alias="hats_margin_threshold")
    """Threshold of the pixel boundary, expressed in arcseconds."""

    primary_column: Optional[str] = Field(default=None, alias="hats_col_assn_primary")
    """Column name in the primary (left) side of join."""

    primary_column_association: Optional[str] = Field(default=None, alias="hats_col_assn_primary_assn")
    """Column name in the association table that matches the primary (left) side of join."""

    join_catalog: Optional[str] = Field(default=None, alias="hats_assn_join_table_url")
    """Catalog name for the joining (right) side of association."""

    join_column: Optional[str] = Field(default=None, alias="hats_col_assn_join")
    """Column name in the joining (right) side of join."""

    join_column_association: Optional[str] = Field(default=None, alias="hats_col_assn_join_assn")
    """Column name in the association table that matches the joining (right) side of join."""

    assn_max_separation: Optional[float] = Field(default=None, alias="hats_assn_max_separation")
    """The maximum separation between two points in an association catalog, expressed in arcseconds."""

    contains_leaf_files: Optional[bool] = Field(default=None, alias="hats_assn_leaf_files")
    """Whether or not the association catalog contains leaf parquet files."""

    indexing_column: Optional[str] = Field(default=None, alias="hats_index_column")
    """Column that we provide an index over."""

    extra_columns: Optional[list[str]] = Field(default=None, alias="hats_index_extra_column")
    """Any additional payload columns included in index."""

    npix_suffix: str = Field(default=".parquet", alias="hats_npix_suffix")
    """Suffix of the Npix partitions.
    In the standard HATS directory structure, this is ``'.parquet'`` because there is a single file
    in each Npix partition and it is named like ``'Npix=313.parquet'``.
    Other valid directory structures include those with the same single file per partition but
    which use a different suffix (e.g., ``'npix_suffix' = '.parq'`` or ``'.snappy.parquet'``),
    and also those in which the Npix partitions are actually directories containing 1+ files
    underneath (and then ``'npix_suffix' = '/'``).
    """

    skymap_order: Optional[int] = Field(default=None, alias="hats_skymap_order")
    """Nested Order of the healpix skymap stored in the default skymap.fits."""

    skymap_alt_orders: Optional[list[int]] = Field(default=None, alias="hats_skymap_alt_orders")
    """Nested Order (K) of the healpix skymaps stored in altnernative skymap.K.fits."""

    ## Allow any extra keyword args to be stored on the properties object.
    model_config = ConfigDict(extra="allow", populate_by_name=True, use_enum_values=True)

    @field_validator("default_columns", "extra_columns", mode="before")
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
            python list of strings
        """
        if isinstance(str_value, str):
            # Split on a few kinds of delimiters (just to be safe), and remove duplicates
            return list(filter(None, re.split(";| |,|\n", str_value)))
        ## Convert empty strings and empty lists to None
        return str_value if str_value else None

    @field_validator("skymap_alt_orders", mode="before")
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

    @field_serializer("default_columns", "extra_columns", "skymap_alt_orders")
    def serialize_as_space_delimited_list(self, str_list: Iterable) -> str:
        """Convert a python list of strings into a space-delimited string.

        Parameters
        ----------
        str_list: Iterable
            a python list of strings

        Returns
        -------
        str
            a space-delimited string.
        """
        if str_list is None or len(str_list) == 0:
            return None
        return " ".join([str(element) for element in str_list])

    @model_validator(mode="after")
    def check_required(self) -> Self:
        """Check that type-specific fields are appropriate, and required fields are set."""
        explicit_keys = set(
            self.model_dump(by_alias=False, exclude_none=True).keys() - self.__pydantic_extra__.keys()
        )

        required_keys = set(
            CATALOG_TYPE_REQUIRED_FIELDS[self.catalog_type] + ["catalog_name", "catalog_type", "total_rows"]
        )
        missing_required = required_keys - explicit_keys
        if len(missing_required) > 0:
            raise ValueError(
                f"Missing required property for table type {self.catalog_type}: {missing_required}"
            )

        return self

    def copy_and_update(self, **kwargs):
        """Create a validated copy of these table properties, updating the fields provided in kwargs.

        Parameters
        ----------
        **kwargs
            values to update

        Returns
        -------
        TableProperties
            new instance of properties object
        """
        new_properties = self.model_copy(update=kwargs)
        TableProperties.model_validate(new_properties)
        return new_properties

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
    def read_from_dir(cls, catalog_dir: str | Path | UPath) -> Self:
        """Read field values from a java-style properties file.

        Parameters
        ----------
        catalog_dir: str | Path | UPath
            path to a catalog directory.

        Returns
        -------
        TableProperties
            object created from the contents of a ``hats.properties`` file in
            the given directory
        """
        catalog_path = file_io.get_upath(catalog_dir)
        file_path = catalog_path / "hats.properties"
        if not file_io.does_file_or_directory_exist(file_path):
            file_path = catalog_path / "properties"
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
            directory to write the file
        """
        # pylint: disable=protected-access
        parameters = self.model_dump(by_alias=True, exclude_none=True)
        properties = Properties(process_escapes_in_values=False)
        properties.properties = parameters
        properties._key_order = parameters.keys()

        catalog_path = file_io.get_upath(catalog_dir)
        file_path = catalog_path / "hats.properties"
        with file_path.open("wb") as _file:
            properties.store(_file, encoding="utf-8", initial_comments="HATS catalog", timestamp=False)
        file_path = catalog_path / "properties"
        with file_path.open("wb") as _file:
            properties.store(_file, encoding="utf-8", initial_comments="HATS catalog", timestamp=False)

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

        def _estimate_dir_size(target_dir):
            total_size = 0
            for item in target_dir.iterdir():
                if item.is_dir():
                    total_size += _estimate_dir_size(item)
                else:
                    total_size += item.stat().st_size
            return total_size

        path = file_io.get_upath(path)

        builder_str = ""
        if builder is not None:
            builder_str = f"{builder}, "
        builder_str += f"hats v{version('hats')}"

        properties = {}
        now = datetime.now(tz=timezone.utc)
        properties["hats_builder"] = builder_str
        properties["hats_creation_date"] = now.strftime("%Y-%m-%dT%H:%M%Z")
        properties["hats_estsize"] = int(_estimate_dir_size(path) / 1024) if path else 0
        properties["hats_release_date"] = "2025-08-22"
        properties["hats_version"] = "v1.0"
        return properties | kwargs
