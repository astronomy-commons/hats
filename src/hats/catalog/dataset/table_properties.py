import re
from pathlib import Path
from typing import Iterable, List, Optional, Union

from jproperties import Properties
from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator, model_validator
from typing_extensions import Self
from upath import UPath

from hats.catalog.catalog_type import CatalogType
from hats.io import file_io

## catalog_name, catalog_type, and total_rows are allowed for ALL types
CATALOG_TYPE_ALLOWED_FIELDS = {
    CatalogType.OBJECT: ["ra_column", "dec_column", "default_columns"],
    CatalogType.SOURCE: ["primary_catalog", "ra_column", "dec_column", "default_columns"],
    CatalogType.ASSOCIATION: [
        "primary_catalog",
        "primary_column",
        "primary_column_association",
        "join_catalog",
        "join_column",
        "join_column_association",
        "contains_leaf_files",
    ],
    CatalogType.INDEX: ["primary_catalog", "indexing_column", "extra_columns"],
    CatalogType.MARGIN: ["primary_catalog", "margin_threshold", "ra_column", "dec_column", "default_columns"],
}

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
}

# All additional properties in the HATS recommendation.
EXTRA_ALLOWED_FIELDS = [
    "addendum_did",
    "bib_reference",
    "bib_reference_url",
    "creator_did",
    "data_ucd",
    "hats_builder",
    "hats_cols_sort",
    "hats_cols_survey_id",
    "hats_coordinate_epoch",
    "hats_copyright",
    "hats_creation_date",
    "hats_creator",
    "hats_estsize",
    "hats_frame",
    "hats_max_rows",
    "hats_order",
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


class TableProperties(BaseModel):
    """Container class for catalog metadata"""

    catalog_name: str = Field(alias="obs_collection")
    catalog_type: CatalogType = Field(alias="dataproduct_type")
    total_rows: int = Field(alias="hats_nrows")

    ra_column: Optional[str] = Field(default=None, alias="hats_col_ra")
    dec_column: Optional[str] = Field(default=None, alias="hats_col_dec")
    default_columns: Optional[List[str]] = Field(default=None, alias="hats_cols_default")
    """Which columns should be read from parquet files, when user doesn't otherwise specify."""

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

    contains_leaf_files: Optional[bool] = Field(default=None, alias="hats_assn_leaf_files")
    """Whether or not the association catalog contains leaf parquet files."""

    indexing_column: Optional[str] = Field(default=None, alias="hats_index_column")
    """Column that we provide an index over."""

    extra_columns: Optional[List[str]] = Field(default=None, alias="hats_index_extra_column")
    """Any additional payload columns included in index."""

    ## Allow any extra keyword args to be stored on the properties object.
    model_config = ConfigDict(extra="allow", populate_by_name=True, use_enum_values=True)

    @field_validator("default_columns", "extra_columns", mode="before")
    @classmethod
    def space_delimited_list(cls, str_value: str) -> List[str]:
        """Convert a space-delimited list string into a python list of strings."""
        if isinstance(str_value, str):
            # Split on a few kinds of delimiters (just to be safe), and remove duplicates
            return list(filter(None, re.split(";| |,|\n", str_value)))
        ## Convert empty strings and empty lists to None
        return str_value if str_value else None

    @field_serializer("default_columns", "extra_columns")
    def serialize_as_space_delimited_list(self, str_list: Iterable[str]) -> str:
        """Convert a python list of strings into a space-delimited string."""
        if str_list is None or len(str_list) == 0:
            return None
        return " ".join(str_list)

    @model_validator(mode="after")
    def check_allowed_and_required(self) -> Self:
        """Check that type-specific fields are appropriate, and required fields are set."""
        explicit_keys = set(
            self.model_dump(by_alias=False, exclude_none=True).keys() - self.__pydantic_extra__.keys()
        )

        allowed_keys = set(
            CATALOG_TYPE_ALLOWED_FIELDS[self.catalog_type] + ["catalog_name", "catalog_type", "total_rows"]
        )
        non_allowed = explicit_keys - allowed_keys
        if len(non_allowed) > 0:
            raise ValueError(f"Unexpected property for table type {self.catalog_type}: {non_allowed}")

        required_keys = set(
            CATALOG_TYPE_REQUIRED_FIELDS[self.catalog_type] + ["catalog_name", "catalog_type", "total_rows"]
        )
        missing_required = required_keys - explicit_keys
        if len(missing_required) > 0:
            raise ValueError(
                f"Missing required property for table type {self.catalog_type}: {missing_required}"
            )

        # Check against all known properties - catches typos.
        non_allowed = set(self.__pydantic_extra__.keys()) - set(EXTRA_ALLOWED_FIELDS)
        if len(non_allowed) > 0:
            raise ValueError(f"Unexpected extra property: {non_allowed}")
        return self

    def copy_and_update(self, **kwargs):
        """Create a validated copy of these table properties, updating the fields provided in kwargs."""
        new_properties = self.model_copy(update=kwargs)
        TableProperties.model_validate(new_properties)
        return new_properties

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
    def read_from_dir(cls, catalog_dir: Union[str, Path, UPath]) -> Self:
        """Read field values from a java-style properties file."""
        file_path = file_io.get_upath(catalog_dir) / "properties"
        if not file_io.does_file_or_directory_exist(file_path):
            raise FileNotFoundError(f"No properties file found where expected: {str(file_path)}")
        p = Properties()
        with file_path.open("rb") as f:
            p.load(f, "utf-8")
        return cls(**p.properties)

    def to_properties_file(self, catalog_dir: Union[str, Path, UPath]) -> Self:
        """Write fields to a java-style properties file."""
        # pylint: disable=protected-access
        parameters = self.model_dump(by_alias=True, exclude_none=True)
        properties = Properties()
        properties.properties = parameters
        properties._key_order = parameters.keys()
        file_path = file_io.get_upath(catalog_dir) / "properties"
        with file_path.open("wb") as _file:
            properties.store(_file, encoding="utf-8", initial_comments="HATS catalog", timestamp=False)
