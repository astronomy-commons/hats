from pathlib import Path
from typing import Annotated, Literal, Optional

from pydantic import (
    BeforeValidator,
    Field,
    PlainSerializer,
)
from upath import UPath

from hats.catalog.catalog_type import CatalogType
from hats.catalog.dataset.hats_properties import HatsProperties
from hats.io import file_io


class ExtensionProperties(HatsProperties):
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

    extension_columns: Annotated[
        Optional[list[str]],
        Field(default=None, alias="hats_ext_cols"),
        PlainSerializer(HatsProperties.serialize_list_as_space_delimited_list),
        BeforeValidator(HatsProperties.space_delimited_list),
    ]
    """The list of columns provided by the extension."""

    extension_join_style: Optional[Literal["left", "inner"]] = Field(
        default=None, alias="hats_ext_join_style"
    )
    """The type of join to use when joining the extension to its primary catalog."""

    extension_product_type: Optional[str] = Field(default=None, alias="hats_product_type_served")
    """Modality of the data that the extension stores."""

    def to_properties_file(self, catalog_dir: str | Path | UPath):
        """Write fields to a java-style ``<extension>.properties`` file, named after the extension.

        Parameters
        ----------
        catalog_dir: str | Path | UPath
            directory to write the file to.
        """
        self.to_properties_file_path(
            file_io.get_upath(catalog_dir) / f"{self.name}.properties", initial_comments="HATS Extension"
        )
