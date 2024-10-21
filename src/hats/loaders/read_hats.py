from __future__ import annotations

from pathlib import Path
from typing import Type

from upath import UPath

from hats.catalog import AssociationCatalog, Catalog, CatalogType, Dataset, MarginCatalog
from hats.catalog.dataset.table_properties import TableProperties
from hats.catalog.index.index_catalog import IndexCatalog

CATALOG_TYPE_TO_CLASS = {
    CatalogType.OBJECT: Catalog,
    CatalogType.SOURCE: Catalog,
    CatalogType.ASSOCIATION: AssociationCatalog,
    CatalogType.INDEX: IndexCatalog,
    CatalogType.MARGIN: MarginCatalog,
}


def read_hats(catalog_path: str | Path | UPath, catalog_type: CatalogType | None = None) -> Dataset:
    """Reads a HATS Catalog from a HATS directory

    Args:
        catalog_path (str): path to the root directory of the catalog
        catalog_type (CatalogType): Default `None`. By default, the type of the catalog is loaded
            from the catalog info and the corresponding object type is returned. Python's type hints
            cannot allow a return type specified by a loaded value, so to use the correct return
            type for type checking, the type of the catalog can be specified here. Use by specifying
            the hats class for that catalog.

    Returns:
        The initialized catalog object
    """
    catalog_type_to_use = (
        _read_dataset_class_from_metadata(catalog_path) if catalog_type is None else catalog_type
    )
    loader = _get_loader_from_catalog_type(catalog_type_to_use)
    return loader.read_hats(catalog_path)


def _read_dataset_class_from_metadata(catalog_base_path: str) -> CatalogType:
    return TableProperties.read_from_dir(catalog_base_path).catalog_type


def _get_loader_from_catalog_type(catalog_type: CatalogType) -> Type[Dataset]:
    if catalog_type not in CATALOG_TYPE_TO_CLASS:
        raise NotImplementedError(f"Cannot load catalog of type {catalog_type}")
    return CATALOG_TYPE_TO_CLASS[catalog_type]
