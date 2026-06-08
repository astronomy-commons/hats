# HATS Guide

**Last updated:** 2026-06-08 | **HATS version:** v0.9.2

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
astronomical catalogs on the celestial sphere. It divides the sky into HEALPix pixels and stores each pixel's data as a separate Parquet file,
enabling efficient spatial queries and distributed computation without loading
the full dataset.

The HATS Python library provides:
- Classes for reading and representing catalog structure and metadata
- Path utilities for navigating the on-disk layout
- HEALPix pixel math and utilities
- MOC (Multi-Order Coverage) map support
- Plotting utilities for visualizing sky maps

[LSDB](https://github.com/astronomy-commons/lsdb) operates on top of HATS - every `lsdb.Catalog` holds a `hc_structure` attribute that is
a `hats.catalog.Catalog` instance.

## Design goals and north stars

**CRITICAL: Always keep these design principles in mind when making changes to HATS.**

**Storage format correctness is paramount.** HATS defines the on-disk layout that LSDB
and downstream tools rely on. The partition layout, metadata conventions, and path structure
must be kept consistent. Any format change must be backward-compatible or explicitly versioned.

**HEALPix NESTED ordering is the backbone.** All partition identification, MOC operations,
and pixel-math utilities must be consistent with the HEALPix NESTED scheme. Never mix
RING and NESTED ordering silently. Use the `cdshealpix` and `mocpy` Python libraries for core HEALPix math operations.

**Metadata, not computation.** HATS classes describe what is on disk - they do not load
row data or perform analytics. Keep catalog classes lightweight: they hold structural
metadata and provide path resolution. Row-level computation belongs in LSDB.

**LSDB compatibility is the primary consumer.** LSDB depends on HATS classes extensively.
API changes cascade to LSDB and downstream users. Maintain backward compatibility; if
breaking changes are necessary, be explicit and loud about it.

**Slim API surface.** Do not add new public API methods unless asked. Prefer composing
existing primitives. If you think a new method is needed, propose it first and get
agreement on the design before implementing.

**Backwards compatibility.** Maintain backward compatibility where possible! If breaking
changes are necessary, be loud about it.

**Document current behavior.** When migrating away from old patterns, use `@deprecated`
with a helpful message rather than silently removing behavior.

**Docstrings and type safety.** All public methods must have complete NumPy-style
docstrings and accurate type annotations.

## Coding advice

- **Do not push or open PRs** unless explicitly asked.
- When changing code, ensure that the current assumptions of the change appear to have always 
been true. 
- Leave code better than you find it over keeping old assumptions around.

## Development setup

> HATS and LSDB are typically developed in the same local environment. **Prefer installing HATS into the existing
> LSDB environment** rather than creating a new one. Only create a fresh environment if you need.

- **Python ≥ 3.11** (see `pyproject.toml` `requires-python`)
- If you need a new env: `conda create -n hats python=3.11 && conda activate hats`
- Clone and install: `git clone https://github.com/astronomy-commons/hats.git && cd hats`
- Run the setup script: `echo 'y' | bash .setup_dev.sh`
  - Installs the package in editable mode with dev and full extras
  - Installs pre-commit hooks
- Alternative manual install: `pip install -e .'[dev]' && pre-commit install`
- For full optional features (e.g. plotting, polygon search): `pip install -e '.[full]'`
- For bleeding-edge dependency versions (nested-pandas from `main`): `pip install -r requirements.txt`
- For documentation dependencies: `pip install -r docs/requirements.txt`

## Common commands

```bash
# Run the full test suite (includes doctests in src/ and docs/)
python -m pytest

# Run only unit tests (skip doctest collection from docs/)
python -m pytest tests/

# Run with coverage reporting
python -m pytest --cov=hats --cov-report=xml

# Lint
pylint src/ --rcfile=./src/.pylintrc
pylint tests/ --rcfile=./tests/.pylintrc

# Format
black src/ tests/ && isort src/ tests/

# Type check
mypy src/ tests/ --ignore-missing-imports

# Pre-commit (runs black, isort, pylint, mypy ...)
pre-commit run --all-files

# Build docs. Requires `docs/requirements.txt` dependencies installed.
cd docs && make html

# Run ASV benchmarks
cd benchmarks && asv run --quick
```

## Repository structure

```
src/hats/               Main package
src/hats/catalog/       Catalog class hierarchy (Catalog, MarginCatalog, MapCatalog, AssociationCatalog, IndexCatalog)
src/hats/pixel_math/    HEALPix pixel math and spatial index utilities
src/hats/pixel_tree/    PixelTree, MOC filtering, and pixel alignment
src/hats/io/            File I/O, path helpers, and metadata utilities
src/hats/inspection/    Plotting and visualization utilities
src/hats/loaders/       Catalog loading from disk or object store
src/hats/search/        Region search utilities (cone, box, polygon, MOC)
tests/hats/             Test suite (mirrors src/ layout)
tests/data/             Small HATS-formatted test catalogs
benchmarks/             ASV performance benchmarks
docs/                   Sphinx documentation sources
docs/notebooks/         Jupyter notebook tutorials
```

Key files:

| File                                                              | Purpose                                                                           |
|-------------------------------------------------------------------|-----------------------------------------------------------------------------------|
| `pyproject.toml`                                                  | Project metadata, dependencies, pytest/black/mypy config                          |
| `src/hats/__init__.py`                                            | Public API - everything exported here is stable public surface                    |
| `src/hats/catalog/catalog.py`                                     | Main `Catalog` class                                                              |
| `src/hats/catalog/healpix_dataset/healpix_dataset.py`             | Base `HealpixDataset` class, shared by all catalog types                          |
| `src/hats/loaders/read_hats.py`                                   | `read_hats` entry point for loading catalog metadata from disk                    |
| `src/hats/catalog/dataset/table_properties.py`                    | `TableProperties` - typed catalog metadata from `hats.properties`                 |
| `src/hats/catalog/dataset/collection_properties.py`               | `CollectionProperties` - catalog collection metadata from `collection.properties` |
| `src/hats/pixel_tree/pixel_tree.py`                               | `PixelTree` - which pixels exist and at what order                                |
| `src/hats/pixel_math/healpix_pixel.py`                            | `HealpixPixel` - pixel identifier `(order, pixel)`                                |
| `src/hats/io/paths.py`                                            | Path helpers for constructing on-disk file paths                                  |

## Core concepts

### HEALPix partitioning

HATS partitions the sky using the
[HEALPix](https://healpix.sourceforge.io/) pixelisation scheme in **NESTED**
ordering:
- At order `k`, the sky is divided into `12 × 4^k` equal-area pixels.
- Each pixel at order `k` has exactly 4 children at order `k+1`.
- Higher orders mean finer resolution and more (smaller) pixels.

HATS catalogs are multi-order:
- Dense regions use high-order (small) pixels
and sparse regions use low-order (large) pixels. 
- Pixels are balanced in the number of rows or size in memory.

Each partition is identified by a HEALPix `(Norder, Npix)` pair:
- `Norder` - HEALPix order (integer)
- `Npix` - pixel number in NESTED ordering at that order (integer)

### Catalog types

| Type            | `dataproduct_type` | Purpose                                                  |
|-----------------|--------------------|----------------------------------------------------------|
| Object / Source | `object` / `source` | Standard point-source catalog                            |
| Margin          | `margin`           | Boundary objects duplicated from adjacent pixels         |
| Index           | `index`            | Secondary index on a non-spatial column (e.g. object ID) |
| Map             | `map`              | Continuous sky map (non-point-source data)               |
| Association     | `association`      | Cross-catalog join table (with extra columns)            |

## On-disk directory layout

```
my_catalog/
├── hats.properties          # Primary metadata (key=value format)
├── partition_info.csv       # One row per partition: Norder,Npix
├── skymap.fits              # FITS image with counts per pixel at a fixed high order (e.g. 10)
└── dataset/
    ├── _metadata            # Parquet metadata with statistics
    ├── _common_metadata     # Parquet schema metadata
    ├── Norder=1/
    │   └── Dir=0/
    │       ├── Npix=44.parquet
    │       └── Npix=45.parquet
    └── Norder=2/
        └── Dir=0/
            └── Npix=176.parquet
```

### `hats.properties` file

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

### `partition_info.csv`

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

### Margin catalog

Same layout as an object catalog, with two differences in `hats.properties`:

```
dataproduct_type=margin
hats_margin_threshold=3600.0      # margin radius in arcseconds
hats_primary_table_url=../my_catalog
```

### Catalog collection

A collection groups a primary catalog with its associated margin and index catalogs under a single root directory. A `collection.properties` file at the root lists the members. When LSDB opens a collection with `open_catalog()`, the default margin is automatically loaded and attached as `catalog.margin`.

```
my_collection/
├── collection.properties    # Lists member catalog paths
├── my_catalog/              # Primary object catalog (full layout above)
├── my_catalog_margin/       # Margin catalog (full layout above)
└── my_catalog_index/        # Index catalog (optional)
```

## Architecture: Catalog class hierarchy

All catalog types inherit from `HealpixDataset`. The class hierarchy is:

```
Dataset
└── HealpixDataset
    ├── Catalog            (object / source)
    ├── MarginCatalog      (margin)
    ├── MapCatalog         (map)
    ├── AssociationCatalog (association)
    └── IndexCatalog       (index)
```

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
info.primary_catalog             # str  - path to the primary (left) catalog
info.primary_column              # str  - ID column in the primary catalog
info.primary_column_association  # str  - matching column in the association table
info.join_catalog                # str  - path to the join (right) catalog
info.join_column                 # str  - ID column in the join catalog
info.join_column_association     # str  - matching column in the association table
info.assn_max_separation         # float - maximum match separation in arcseconds
info.contains_leaf_files         # bool  - whether leaf parquet files are present
```

### `hats.pixel_tree.PixelTree` - the pixel tree

Represents which HEALPix pixels exist in the catalog and their order. Accessed
as `cat.hc_structure.pixel_tree`.

```python
tree = cat.hc_structure.pixel_tree

tree.get_healpix_pixels()   # list[HealpixPixel] - all partitions
tree.get_max_depth()        # int - highest order in the catalog
tree.to_moc()               # mocpy.MOC - sky coverage
tree.to_depth29_ranges()    # np.ndarray of shape (N, 2) - intervals at order 29
HealpixPixel(1, 44) in tree # bool - O(log N) containment check
```

#### How the pixel tree works

The pixel tree stores each healpix pixel as a range of order-29 pixels. For example, a pixel at order 1 with 
Npix=44 corresponds to an interval of 4^28 pixels at order 29. The pixel tree is an ordered list of these
intervals. To check if a pixel is in the tree, we convert it to its order-29 interval and do a binary search 
to see if it matches any of the stored intervals.

#### Why use order-29 intervals?

Using order-29 intervals allows us to represent any pixel at any order as a contiguous range of pixels at 
a fixed high order. This simplifies the logic since we only need to deal with one fixed order internally.
Order 29 is chosen because it is the highest order that can fit within a 64-bit integer, allowing us to use
efficient integer arithmetic for pixel math and containment checks.

#### PixelAlignment

One of the most important uses of the pixel tree is 'aligning' multiple catalogs to each other, figuring
out which pixels overlap between them, and creating a mapping of which pixels in one catalog correspond to 
which pixels in the other. This is important for crossmatching and other operations that need to combine data 
from multiple catalogs.

To do this, we use the `hats.pixel_tree.pixel_alignment.align_trees` method which takes two pixel trees,
iterates through both of their pixel interval lists in order, checks for each pair of intervals whether they
overlap, and if so computes the intersection of those intervals and converts it back to the corresponding
pixels at the original orders, and iterating to the next intervals in one or both trees depending on which
one has the smaller next interval. This is an efficient O(N) operation where N is the total number of pixels 
in both trees.

The result is a mapping of which pixels in one catalog correspond to which pixels in the other. The other
result from aligning pixel trees is an output aligned pixel tree which is the union of the two input trees,
covering the intersection of the two catalogs, with pixels split as needed to ensure that any pixel in the
aligned tree is fully contained in a single pixel in each of the input trees. This means that the the aligned
tree can be the output structure for a crossmatched catalog, ensuring that each partition in the output is 
roughly no bigger than a single partition in either input catalog, which keeps file sizes manageable.

The pixel alignment also supports specifying a 'how' parameter which controls how to handle pixels that are 
present in one tree but not the other. The default is 'inner' which only includes pixels that are present 
in both trees. 'left' includes all pixels from the first tree, with empty partitions for pixels not in the 
second tree. 'right' includes all pixels from the second tree, with empty partitions for pixels not in the 
first tree. 'outer' includes all pixels from both trees, with empty partitions for pixels not in the other 
tree.

### `hats.pixel_math.HealpixPixel` - a single HEALPix pixel identifier

```python
from hats.pixel_math import HealpixPixel

pix = HealpixPixel(order=1, pixel=44)

pix.order    # int - 1
pix.pixel    # int - 44
```

## Margin catalogs and why they matter

A margin catalog stores a copy of every object that lies within
`margin_threshold` arcseconds of a pixel boundary, duplicated into the adjacent
pixel's margin file. This ensures that spatial operations spanning partition
edges (crossmatch or joins near a boundary) see all relevant objects without
loading the entire neighboring partition.

**Rule of thumb:** To ensure completeness of the result, `margin_threshold`
must be ≥ the search radius used in any cross-catalog operation.

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

# Path to the parquet common metadata file
paths.get_common_metadata_pointer(catalog_base_path)
# → catalog_base_path/dataset/_common_metadata

# File pointer to FITS image file
paths.get_skymap_file_pointer(catalog_base_path)
# → catalog_base_path/skymap.fits or catalog_base_path/skymap.K.fits
```

## Typical HATS workflow

A typical HATS workflow (outside of LSDB) involves:

1. **Loading a catalog structure** from disk or object store.
2. **Exploring metadata** to understand the catalog shape, coverage, and schema.
3. **Filtering by pixel or region** to get a subset of partitions.
4. **Constructing file paths** for downstream readers using path utilities.

### Load a catalog

```python
from hats.loaders import read_hats

# Load from local disk or object store (returns CatalogCollection or Dataset)
cat = read_hats("/path/to/catalog")

# Catalog collection
cat = read_hats("/path/to/collection")
cat.main_catalog                            # Main catalog (`hats.catalog.Catalog`)
cat.all_margins                             # All margins
cat.default_margin                          # Default margin name
cat.all_indexes                             # All indexes
cat.default_index_field                     # Default index field name
cat.get_index_dir_for_field("object_id")    # Pointer to "object_id" index field catalog
```

### Explore metadata

```python
# Partition layout
print(cat.get_healpix_pixels())

# Maximum partition order
max_order = cat.pixel_tree.get_max_depth()

# Metadata
info = cat.catalog_info
print(info.catalog_name, info.total_rows, info.ra_column, info.dec_column)

# Arrow schema (column names + types, no data loaded)
print(cat.schema)

# Sky coverage fraction
print(info.moc_sky_fraction)
```

### Visualize coverage

```python
# Plot HEALPix partition map
cat.plot_pixels()

# Plot MOC sky coverage
cat.plot_moc()

# Plot point-density map
hats.inspection.plot_density(cat)
```

### Filter by pixel or region

```python
from hats.pixel_math import HealpixPixel

# Restrict to a specific list of pixels
pixels = [HealpixPixel(order=1, pixel=44), HealpixPixel(order=1, pixel=45)]
filtered = cat.filter_from_pixel_list(pixels)

# Filter using region filters
filtered = cat.filter_by_cone(ra=47.1, dec=6, radius_arcsec=30 * 3600)                          # Cone
filtered = cat.filter_by_box(ra=(280, 300), dec=(-40, -30))                                     # Box
filtered = cat.filter_by_polygon(vertices=[(300, -50), (300, -55), (272, -55), (272, -50)])     # Polygon

# Filter using any other MOC
from mocpy import MOC
orders = np.array([1, 1, 2])
pixels = np.array([45, 46, 128])
max_depth = 2
moc = MOC.from_healpix_cells(pixels, orders, max_depth)
filtered = cat.filter_by_moc(moc)
```

### Construct partition file paths

```python
from hats.io import paths
from hats.pixel_math import HealpixPixel

for pixel in cat.get_healpix_pixels():
    path = paths.pixel_catalog_file(cat.catalog_path, pixel)
    # pass `path` to pyarrow.parquet.read_table or similar
```

## Testing Conventions

- **File naming:** `tests/hats/test_<name>.py`, mirroring the `src/hats/` layout.
- **Fixtures:** defined in `tests/conftest.py`. Use existing fixtures; do not
  duplicate test data. All fixtures are backed by tiny HATS catalogs in `tests/data/`.
- Default test run: `python -m pytest`
- **Doctest enforcement:** `pytest` is configured with `--doctest-modules` and
  `--doctest-glob=*.rst`. All public docstring examples must be runnable and correct.
- **No network in unit tests.** Test data lives in `tests/data/`; do not fetch from
  the internet in unit tests.

## Key Conventions

- **Line length:** 110 characters (`black` and `isort` both enforce this).
- **Import style:** `isort` with `profile = "black"`. Do not hand-tune import order.
- **Docstrings:** NumPy style. All public functions and methods require a complete
  docstring including `Parameters` and `Returns`. Try to also include an `Examples` block.
- **Deprecation:** use `@deprecated(version="X.Y", reason="...")` from the
  `deprecated` package. Never silently remove behavior.
- **`_version.py` is auto-generated** by `setuptools_scm` from git tags. Never
  edit it by hand; it is excluded from coverage and linting.
- **All file paths use `UPath`** from `universal-pathlib`. Do not use raw `str` paths
  in internal code; wrap with `UPath` to support both local and remote (S3, GCS) stores.
- **`CatalogType` enum** is the canonical source for catalog type strings. Do not
  compare `catalog_type` against raw string literals; use `CatalogType.OBJECT` etc.

## CI/CD and GitHub Workflows

- **`testing-and-coverage.yml`** - runs on every PR and push to `main`; matrix over
  Python 3.11–3.14; uploads coverage to Codecov.
- **`smoke-test.yml`** - daily at 06:45 UTC; tests both `[dev]` and `[full]` extras
  across Python 3.11–3.14.
- **`testing-windows.yml`** - Windows-specific test matrix.
- **`asv-main.yml` / `asv-pr.yml` / `asv-nightly.yml`** - ASV performance benchmarks;
  PR results are posted back to the PR.
- **`publish-to-pypi.yml`** - triggered on tagged releases.
- **`pre-commit-ci.yml`** - automated pre-commit hook checks for format/lint/mypy.
