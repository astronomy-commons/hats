# HATS Guide

**Last updated:** 2026-05-29 | **HATS version:** v0.9.0

Canonical reference for AI coding assistants working on HATS (Hierarchical Adaptive Tiling Scheme). 
Tool-specific files (`CLAUDE.md`, `.github/copilot-instructions.md`) contain only tool-specific overrides
and reference this file for shared guidance. **Edit this file** for changes that should
apply to all AI assistants; edit tool-specific files only for tool-specific behavior.

> **Version notice for AI assistants:** If anything in this guide conflicts with what
> you observe in the actual code (missing methods, different signatures, changed
> behaviour), the guide may be outdated. Check the version above against the installed
> package version (`python -c "import hats; print(hats.__version__)"`) and **alert the
> user** about the changes that `HATS_GUIDE.md` might need. Do not silently assume the
> guide is correct.

## What is HATS

HATS is a storage format and Python library for partitioning large
astronomical point-source catalogs on the celestial sphere. It divides the sky into HEALPix pixels and stores each pixel's data as a separate Parquet file,
enabling efficient spatial queries and distributed computation without loading
the full dataset.

The HATS Python library provides:
- Classes for reading and representing catalog structure and metadata
- Path utilities for navigating the on-disk layout
- HEALPix pixel math and utilities
- MOC (Multi-Order Coverage) map support
- Plotting utilities for visualizing sky maps

[LSDB](https://github.com/astronomy-commons/lsdb) operates on top of HATS: every `lsdb.Catalog` holds a `hc_structure` attribute that is
a `hats.catalog.Catalog` instance.

## Core concepts

### HEALPix partitioning

HATS partitions the sky using the
[HEALPix](https://healpix.sourceforge.io/) pixelisation scheme in **NESTED**
ordering. At order `k`, the sky is divided into `12 × 4^k` equal-area pixels.
Higher orders mean finer resolution and more (smaller) pixels.

Each pixel at order `k` has exactly 4 children at order `k+1`, forming a
hierarchy. HATS exploits this to store catalogs at mixed orders: dense regions
use high-order (small) pixels and sparse regions use low-order (large) pixels,
keeping each Parquet file at a manageable row count.

A single partition is identified by the pair `(Norder, Npix)`:
- `Norder` - HEALPix order (integer, typically 0–10 for catalog partitions)
- `Npix` - pixel number in NESTED ordering at that order

The high-precision sky position of every row is stored separately in a
`_healpix_29` column (order-29 NESTED pixel), which LSDB uses for spatial
indexing independent of the partition order.

### Catalog types

| Type | `dataproduct_type` | Purpose |
|---|---|---|
| Object | `object` | Standard point-source catalog |
| Margin | `margin` | Boundary objects duplicated from adjacent pixels |
| Index | `index` | Secondary index on a non-spatial column |
| Association | `association` | Cross-catalog join table |

### Catalog collections

A collection groups a primary catalog with its associated margin and index
catalogs under a single root directory. A `collection.properties` file at the
root lists the members. When LSDB opens a collection with `open_catalog()`, the
default margin is automatically loaded and attached as `catalog.margin`.

## On-disk directory layout

### Object catalog

```
my_catalog/
├── hats.properties          # Primary metadata (key=value format)
├── partition_info.csv       # One row per partition: Norder,Npix
├── skymap.fits              # Counts per pixel as a FITS image
└── dataset/
    ├── _metadata            # Parquet consolidated metadata
    ├── _common_metadata     # Parquet schema metadata
    ├── Norder=1/
    │   └── Dir=0/
    │       ├── Npix=44.parquet
    │       └── Npix=45.parquet
    └── Norder=2/
        └── Dir=0/
            └── Npix=176.parquet
```

The `Dir=` level is a HIVE-partitioning optimisation: files are grouped into
subdirectories of at most a fixed number of pixels to avoid filesystem
limitations with very large catalogs. `Dir` is computed as
`Npix // pixels_per_dir`.

### Margin catalog

Same layout as an object catalog, with two differences in `hats.properties`:

```
dataproduct_type=margin
hats_margin_threshold=3600.0      # margin radius in arcseconds
hats_primary_table_url=../my_catalog
```

### Catalog collection

```
my_collection/
├── collection.properties    # Lists member catalog paths
├── my_catalog/              # Primary object catalog (full layout above)
├── my_catalog_margin/       # Margin catalog (full layout above)
└── my_catalog_index/        # Index catalog (optional)
```

## `hats.properties` file

The primary metadata file uses a simple `key=value` format. Key fields:

```properties
obs_collection=my_catalog          # Catalog name
dataproduct_type=object            # Catalog type
hats_nrows=1000000                 # Total row count
hats_col_ra=ra                     # RA column name
hats_col_dec=dec                   # Dec column name
hats_col_healpix=_healpix_29       # High-precision HEALPix index column
hats_col_healpix_order=29          # Order of the index column (always 29)
hats_order=1                       # Predominant partition order
hats_npix_suffix=.parquet          # Partition file extension
hats_estsize=512                   # Estimated size in KiB
moc_sky_fraction=0.083             # Fraction of sky covered
hats_version=v1.0
hats_creation_date=2025-10-06T14:20UTC
```

## `partition_info.csv`

Lists every partition present in the catalog, one per line:

```csv
Norder,Npix
1,44
1,45
1,46
2,176
```

This is the authoritative list of which Parquet files should exist under
`dataset/`. LSDB reads this at open time to build the Dask task graph.

## Key HATS classes

### `hats.catalog.Catalog` - the `hc_structure` object

Every `lsdb.Catalog` exposes `.hc_structure`, which is an instance of
`hats.catalog.Catalog`. It holds the full structural description of the catalog
without any row data.

```python
hc = cat.hc_structure

hc.catalog_info        # TableProperties - metadata (see below)
hc.pixel_tree          # PixelTree - which pixels exist and at what order
hc.schema              # pyarrow.Schema - column names and types
hc.catalog_path        # pathlib.Path - root directory on disk
hc.moc                 # mocpy.MOC | None - sky coverage as MOC
hc.on_disk             # bool - True if loaded from disk

# Convenience delegation
hc.get_healpix_pixels()           # list[HealpixPixel]
hc.filter_from_pixel_list(pixels) # new HatsCatalog restricted to pixels
```

### `hats.catalog.TableProperties` - catalog metadata

Accessed as `cat.hc_structure.catalog_info`. Contains every field from
`hats.properties` as typed Python attributes:

```python
info = cat.hc_structure.catalog_info

info.catalog_name        # str  - "my_catalog"
info.catalog_type        # str  - "object" | "margin" | "index" | "association"
info.ra_column           # str  - "ra"
info.dec_column          # str  - "dec"
info.healpix_column      # str  - "_healpix_29"
info.healpix_order       # int  - 29
info.total_rows          # int  - total row count
info.hats_estsize        # float - estimated size in KiB
info.default_columns     # list[str] | None - columns loaded by default
info.npix_suffix         # str  - ".parquet"

# Margin-catalog-specific
info.margin_threshold    # float - radius in arcseconds

# Index-catalog-specific
info.indexing_column     # str  - column being indexed

# Association-catalog-specific
info.primary_column           # str
info.join_column              # str
info.assn_max_separation      # float - arcseconds
```

### `hats.pixel_tree.PixelTree` - the pixel tree

Represents which HEALPix pixels exist in the catalog and their order. Accessed
as `cat.hc_structure.pixel_tree`.

```python
tree = cat.hc_structure.pixel_tree

tree.get_healpix_pixels()   # list[HealpixPixel] - all partitions
tree.get_max_depth()        # int - highest order in the catalog
tree.to_moc()               # mocpy.MOC - sky coverage
```

The tree is hierarchical: a pixel at order `k` implicitly covers its 4 children
at order `k+1`. Mixed-order catalogs (where different regions are stored at
different orders) are represented naturally.

### `hats.pixel_math.HealpixPixel` - a single partition

```python
from hats.pixel_math import HealpixPixel

pix = HealpixPixel(order=1, pixel=44)

pix.order    # int - 1
pix.pixel    # int - 44

# Pixel is identified by (order, pixel) - used as dict keys in LSDB's
# _ddf_pixel_map to map a partition to its Dask partition index.
```

## Margin catalogs and why they matter

A margin catalog stores a copy of every object that lies within
`margin_threshold` arcseconds of a pixel boundary, duplicated into the adjacent
pixel's margin file. This ensures that spatial operations spanning partition
edges (crossmatch, cone search near a boundary) see all relevant objects without
loading the entire neighboring partition.

**Rule of thumb:** `margin_threshold` must be ≥ the search radius used in any
crossmatch or spatial query. When in doubt, use a margin with a generous
threshold.

```python
# The margin is attached automatically when opening a collection
cat = lsdb.open_catalog("/path/to/collection")
cat.margin                         # MarginCatalog | None
cat.margin.hc_structure.catalog_info.margin_threshold  # float, arcseconds

# Crossmatch uses the margin automatically when cat.margin is set
xmatch = cat.crossmatch(other, radius_arcsec=1.0)
```

## Path utilities

`hats.io.paths` provides helpers for constructing file paths from
`(Norder, Npix)` pairs without hard-coding the layout:

```python
from hats.io import paths

# Path to a single partition file
paths.pixel_catalog_file(catalog_base_path, HealpixPixel(order=1, pixel=44))
# → catalog_base_path/dataset/Norder=1/Dir=0/Npix=44.parquet

# Path to the metadata file
paths.get_catalog_info_pointer(catalog_base_path)
# → catalog_base_path/hats.properties
```

## Inspecting a catalog's structure in practice

```python
import lsdb

cat = lsdb.open_catalog("/path/to/catalog")

# Partition layout
for pix in cat.hc_structure.get_healpix_pixels():
    print(pix.order, pix.pixel)

# Maximum partition order
max_order = cat.hc_structure.pixel_tree.get_max_depth()

# Metadata
info = cat.hc_structure.catalog_info
print(info.catalog_name, info.total_rows, info.ra_column, info.dec_column)

# Arrow schema (column names + types, no data loaded)
print(cat.hc_structure.schema)

# Sky coverage as a MOC
moc = cat.hc_structure.moc
```
