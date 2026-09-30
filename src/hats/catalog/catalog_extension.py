from __future__ import annotations

from upath import UPath

from hats.catalog.catalog import Catalog
from hats.catalog.catalog_collection import CatalogCollection
from hats.catalog.dataset.extension_properties import ExtensionProperties


class CatalogExtension:
    """An extension of a HATS Catalog, holding additional columns for its rows

    Extensions are described by an ``<extension>.properties`` file, which references the
    primary catalog, points to the extension data, and names the columns to join them on.
    The extension data is a HATS catalog of its own (or a collection, when it has margins),
    which is valid and can be read on its own.

    The reference to the extension data is resolved against the directory holding the
    ``<extension>.properties`` file, unless it is absolute, so the file can point to data
    hosted elsewhere. The file sits at the root of a collection, and names that collection as
    its primary, unless it gives the primary by absolute path (e.g. a remote one)::

        catalog_collection/
        ├── main_catalog/
        ├── extension_catalog.properties
        ├── collection.properties
    """

    def __init__(
        self,
        config_path: UPath,
        extension_info: ExtensionProperties,
        catalog: Catalog | CatalogCollection,
        storage_options: dict | None = None,
    ):
        self.config_path = config_path
        self.extension_info = extension_info
        self.storage_options = storage_options

        if not isinstance(catalog, (Catalog, CatalogCollection)):
            raise TypeError(
                f"HATS extension at {self.join_catalog_dir} is not of type `Catalog` or `CatalogCollection`"
            )
        self.catalog = catalog

    @property
    def join_catalog_dir(self) -> UPath:
        """Path to the extension data"""
        return CatalogCollection.resolve_inner_path(
            self.config_path.parent, self.extension_info.join_catalog, self.storage_options
        )
