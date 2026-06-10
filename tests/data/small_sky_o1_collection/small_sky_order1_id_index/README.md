

# small_sky_order1_id_index HATS Index Catalog

> **Note:** This is a secondary index catalog for `small_sky_o1_collection/small_sky_order1` and is not intended to be used as a standalone catalog.

This index maps id values to partitions in the small_sky_o1_collection/small_sky_order1 catalog for fast non-spatial lookups.

### Use the index catalog

This index catalog cannot be opened directly with LSDB. Instead, load the primary catalog
(or its collection) with LSDB and call `id_search()` to query by `id`:

```python
import lsdb

catalog = lsdb.read_hats("<CATALOG_COLLECTION_PATH>")
ids = [...]
result = catalog.id_search(values={"id": ids})
result.compute()
```

### File structure

This catalog is represented by the following files and directories:

- [`small_sky_order1_id_index/`](.) — index catalog directory
  - [`dataset/`](dataset/) — Apache Parquet dataset directory
    - ... parquet metadata and data files in sub directories ...
  - [`hats.properties`](hats.properties) — textual metadata file describing this index catalog
