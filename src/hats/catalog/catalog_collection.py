from __future__ import annotations

from upath import UPath

from hats.catalog import Catalog
from hats.catalog.dataset.collection_properties import CollectionProperties
from hats.pixel_math import HealpixPixel


class CatalogCollection:
    """A collection of HATS Catalog with data stored in a HEALPix Hive partitioned structure

    Catalogs of this type are described by a `collection.properties` file which specifies
    the underlying main catalog, margin catalog and index catalog paths. These catalogs are
    stored at the root of the collection, each in its separate directory::

        catalog_collection/
        ├── main_catalog/
        ├── margin_catalog/
        ├── index_catalog/
        ├── collection.properties

    Margin and index catalogs are optional but there could also be multiple of them. The
    catalogs used by default are specified in the `collection.properties` file in the
    `default_margin` and `default_index` keywords.
    """

    def __init__(
        self,
        collection_path: UPath,
        collection_properties: CollectionProperties,
        main_catalog: Catalog,
    ):
        self.collection_path = collection_path
        self.collection_properties = collection_properties

        if not isinstance(main_catalog, Catalog):
            raise TypeError(f"HATS at {main_catalog.catalog_path} is not of type `Catalog`")
        self.main_catalog = main_catalog

    @property
    def main_catalog_dir(self) -> UPath:
        """Path to the main catalog directory"""
        return self.collection_path / self.collection_properties.hats_primary_table_url

    @property
    def all_margins(self) -> list[str] | None:
        """The list of margin catalog names in the collection"""
        return self.collection_properties.all_margins

    @property
    def default_margin(self) -> str | None:
        """The name of the default margin"""
        return self.collection_properties.default_margin

    @property
    def default_margin_catalog_dir(self) -> UPath | None:
        """Path to the default margin catalog directory"""
        if self.default_margin is None:
            return None
        return self.collection_path / self.default_margin

    @property
    def all_indexes(self) -> dict[str, str] | None:
        """The mapping of indexes in the collection"""
        return self.collection_properties.all_indexes

    @property
    def default_index_field(self) -> str | None:
        """The name of the default index field"""
        return self.collection_properties.default_index

    @property
    def default_index_catalog_dir(self) -> UPath | None:
        """Path to the default index catalog directory"""
        if self.default_index_field is None:
            return None
        default_index_dir = self.all_indexes[self.default_index_field]
        return self.collection_path / default_index_dir

    def get_index_dir_for_field(self, field_name: str | None = None) -> UPath | None:
        """Path to the field's index catalog directory"""
        if field_name is None:
            return self.default_index_catalog_dir
        if self.all_indexes is None or field_name not in self.all_indexes:
            raise ValueError(f"Index for field `{field_name}` is not specified in all_indexes")
        index_dir = self.all_indexes[field_name]
        return self.collection_path / index_dir

    def get_healpix_pixels(self) -> list[HealpixPixel]:
        """The list of HEALPix pixels of the main catalog"""
        return self.main_catalog.get_healpix_pixels()
