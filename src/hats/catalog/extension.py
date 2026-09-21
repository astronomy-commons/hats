from __future__ import annotations

from hats.catalog.catalog import Catalog


class ExtensionCatalog(Catalog):
    """An extension of a HATS Catalog with data stored in a HEALPix Hive partitioned structure

    An Extension Catalog complements a Catalog. They are valid HATS catalogs on their own and
    can be hosted independently. To join an Extension to its corresponding Catalog, we use a
    set of join keys, as specified in the Extension `hats.properties`.
    """
