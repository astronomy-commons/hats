from pathlib import Path
from typing import Annotated, Optional

from pydantic import BeforeValidator, Field, PlainSerializer, model_validator
from typing_extensions import Self
from upath import UPath

from hats.catalog.catalog_type import CatalogType
from hats.catalog.dataset.hats_properties import HatsProperties
from hats.io import file_io

## catalog_name and catalog_type are required for ALL types
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


class TableProperties(HatsProperties):
    """Container class for catalog metadata"""

    catalog_name: str = Field(alias="obs_collection")
    catalog_type: CatalogType = Field(alias="dataproduct_type")
    total_rows: Optional[int] = Field(default=None, alias="hats_nrows")

    ra_column: Optional[str] = Field(default=None, alias="hats_col_ra")
    dec_column: Optional[str] = Field(default=None, alias="hats_col_dec")
    default_columns: Annotated[
        Optional[list[str]],
        Field(default=None, alias="hats_cols_default"),
        PlainSerializer(HatsProperties.serialize_list_as_space_delimited_list),
        BeforeValidator(HatsProperties.space_delimited_list),
    ]
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

    extra_columns: Annotated[
        Optional[list[str]],
        Field(default=None, alias="hats_index_extra_column"),
        PlainSerializer(HatsProperties.serialize_list_as_space_delimited_list),
        BeforeValidator(HatsProperties.space_delimited_list),
    ]
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

    skymap_alt_orders: Annotated[
        Optional[list[int]],
        Field(default=None, alias="hats_skymap_alt_orders"),
        PlainSerializer(HatsProperties.serialize_list_as_space_delimited_list),
        BeforeValidator(HatsProperties.space_delimited_int_list),
    ]
    """Nested Order (K) of the healpix skymaps stored in altnernative skymap.K.fits."""

    hats_max_rows: Optional[int] = Field(default=None, alias="hats_max_rows")
    """Maximum number of rows in any partition of the catalog."""

    hats_max_bytes: Optional[int] = Field(default=None, alias="hats_max_bytes")
    """Maximum number of bytes in any partition of the catalog."""

    hats_estsize: Optional[int] = Field(default=None, alias="hats_estsize")
    """Estimated size of the catalog on disk, in kilobytes."""

    moc_sky_fraction: Optional[float] = Field(default=None)

    @model_validator(mode="after")
    def check_required(self) -> Self:
        """Check that type-specific fields are appropriate, and required fields are set."""
        explicit_keys = set(
            self.model_dump(by_alias=False, exclude_none=True).keys() - self.__pydantic_extra__.keys()
        )

        required_keys = set(
            CATALOG_TYPE_REQUIRED_FIELDS[self.catalog_type] + ["catalog_name", "catalog_type"]
        )
        missing_required = required_keys - explicit_keys
        if len(missing_required) > 0:
            raise ValueError(
                "Missing required property for table type "
                f"'{self.catalog_type}': {', '.join(missing_required)}"
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

        Raises
        ------
        FileNotFoundError
            if there is no properties or hats.properties file in the directory
        """
        catalog_path = file_io.get_upath(catalog_dir)
        file_path = catalog_path / "hats.properties"
        if not file_path.exists():
            file_path = catalog_path / "properties"
            if not file_path.exists():
                raise FileNotFoundError(f"No properties file found where expected: {str(file_path)}")
        return cls.read_from_file(file_path)

    # pylint: disable=duplicate-code
    def to_properties_file(self, catalog_dir: str | Path | UPath):
        """Write fields to a java-style properties file.

        Parameters
        ----------
        catalog_dir: str | Path | UPath
            directory to write the file
        """
        catalog_path = file_io.get_upath(catalog_dir)
        self.to_properties_file_path(catalog_path / "hats.properties", initial_comments="HATS catalog")
        self.to_properties_file_path(catalog_path / "properties", initial_comments="HATS catalog")
