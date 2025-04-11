from __future__ import annotations

from hats.catalog import Catalog, MarginCatalog
from hats.catalog.dataset.collection_properties import CollectionProperties
from hats.catalog.healpix_dataset.healpix_dataset import HealpixDataset
from hats.catalog.index.index_catalog import IndexCatalog
from hats.pixel_math import HealpixPixel


class CatalogCollection:
    """A collection of HATS Catalog with data stored in a HEALPix Hive partitioned structure

    Catalogs of this type are described by a `collection.properties` file which specifies
    the underlying main catalog, margin catalog and index catalog paths. These catalogs are
    stored at the root of the collection, one in its separate directory:

    catalog_collection/
    ├── main_catalog/
    ├── margin_catalog/
    ├── index_catalog/
    ├── collection.properties

    Margin and index catalogs are optional but there could also be multiple of them. The
    catalogs loaded by default are also specified in the `collection.properties` file in
    the `default_margin` and `default_index` keywords.
    """

    def __init__(
        self,
        collection_properties: CollectionProperties,
        main_catalog: HealpixDataset | None = None,
        margin_catalog: MarginCatalog | None = None,
        index_catalog: IndexCatalog | None = None,
    ):
        self.collection_properties = collection_properties

        if main_catalog and not isinstance(main_catalog, Catalog):
            raise ValueError("Main catalog is not of type `Catalog`")
        self.main_catalog = main_catalog

        if margin_catalog and not isinstance(margin_catalog, MarginCatalog):
            raise ValueError("Main catalog is not of type `MarginCatalog`")
        self.margin_catalog = margin_catalog

        if index_catalog and not isinstance(index_catalog, IndexCatalog):
            raise ValueError("Index catalog is not of type `IndexCatalog`")
        self.index_catalog = index_catalog

    @property
    def main_catalog_dir(self) -> str:
        """Path to the main catalog directory"""
        return self.collection_properties.main_catalog_dir

    @property
    def default_index_field(self) -> str:
        """The name of the default index field"""
        return self.collection_properties.default_index

    @property
    def default_index_catalog_dir(self) -> str:
        """Path to the default index catalog directory"""
        return self.collection_properties.all_indexes[self.default_index_field]

    def get_healpix_pixels(self) -> list[HealpixPixel] | None:
        """The list of HEALPix pixels of the main catalog"""
        return self.main_catalog.get_healpix_pixels() if self.main_catalog else None
