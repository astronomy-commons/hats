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
    catalogs loaded by default are specified in the `collection.properties` file in the
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
    def all_margins(self) -> str:
        """The list of margin catalog names in the collection"""
        return self.collection_properties.all_margins

    @property
    def default_margin_catalog_dir(self) -> UPath:
        """Path to the default margin catalog directory"""
        return self.collection_path / self.collection_properties.default_margin

    @property
    def all_indexes(self) -> str:
        """The mapping of indexes in the collection"""
        return self.collection_properties.all_indexes

    @property
    def default_index_field(self) -> str:
        """The name of the default index field"""
        return self.collection_properties.default_index

    @property
    def default_index_catalog_dir(self) -> UPath:
        """Path to the default index catalog directory"""
        default_index_dir = self.all_indexes[self.default_index_field]
        return self.collection_path / default_index_dir

    def get_healpix_pixels(self) -> list[HealpixPixel]:
        """The list of HEALPix pixels of the main catalog"""
        return self.main_catalog.get_healpix_pixels()
