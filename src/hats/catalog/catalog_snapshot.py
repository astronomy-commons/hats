from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa

from hats.catalog.partition_info import PartitionInfo

if TYPE_CHECKING:
    from hats.catalog.dataset.table_properties import TableProperties


@dataclass(frozen=True)
class CatalogSnapshot:
    """Immutable snapshot of a catalog's original on-disk state.

    Stores the schema, catalog info, and pixel partition info as they existed when
    the catalog was first loaded, before any column selection or spatial filtering.
    """

    schema: pa.Schema | None
    catalog_info: TableProperties | None = None
    partition_info: PartitionInfo | None = None
