

# small_sky_order1_margin HATS Margin Catalog

> **Note:** This is auxiliary data. It is a margin catalog used for cross-matching and is not intended to be used as a standalone catalog. 

This is the margin catalog for small_sky_order1.

### Access the catalog

We recommend the use of the [LSDB](https://lsdb.io) Python framework to access HATS catalogs.
LSDB can be installed via `pip install lsdb` or `conda install conda-forge::lsdb`,
see more details [in the docs](https://docs.lsdb.io/).
The following code provides a minimal example of reading this margin catalog:

```python
import lsdb

catalog = lsdb.read_hats("<PATH>")
```

This catalog is represented as an [Apache Parquet dataset](https://arrow.apache.org/docs/python/dataset.html) and can be accessed with a variety of tools, including `pandas`, `pyarrow`, `dask`, `Spark`, `DuckDB`.

### File structure

This catalog is represented by the following files and directories:

- [`small_sky_order1_margin/`](.) — margin catalog directory
  - [`dataset/`](dataset/) — Apache Parquet dataset directory
    - ... parquet metadata and data files in sub directories ...
  - [`hats.properties`](hats.properties) — textual metadata file describing this margin catalog
  - [`partition_info.csv`](partition_info.csv) — CSV file with a list of catalog HEALPix tiles

### Catalog metadata

Margin Catalog metadata

| **Number of rows** | **Number of columns** | **Number of partitions** | **HATS Builder** |
| --- | --- | --- | --- |
| 47 | 5 | 7 | hats-import v0.6.8.dev6+g9754d7879.d20251029, hats v0.6.8.dev15+gea9a91e6a.d20251029 |


### Margin Catalog Columns

| **Name** |  **`_healpix_29`** | **`id`** | **`ra`** | **`dec`** | **`ra_error`** | **`dec_error`** |
| --- |  --- | --- | --- | --- | --- | --- |
| **Data Type** |  int64 | int64 | double | double | int64 | int64 |
| **Example row** |  3302771647141634889 | 779 | 347.5 | -29.5 | 0 | 0 |
| **Minimum value** |  3.194e+18 | 708 | 280.5 | -67.5 | 0 | 0 |
| **Maximum value** |  3.399e+18 | 830 | 348.5 | -25.5 | 0 | 0 |




