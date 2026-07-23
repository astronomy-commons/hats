HATS Directory Scheme
===============================================================================

You can read in more detail about the parts of the HATS directory structure
in the `IVOA Note <https://www.ivoa.net/documents/Notes/HATS/>`__.
This page provides a summary of what you can expect inside a HATS-structured
catalog.

Partitioning Scheme
-------------------------------------------------------------------------------

We use healpix (`Hierarchical Equal Area isoLatitude Pixelization <https://healpix.jpl.nasa.gov/>`__)
for the spherical pixelation, and adaptively size the partitions based on the number of objects.

In areas of the sky with more objects, we use smaller pixels, so that all the 
resulting pixels should contain similar counts of objects (within an order of 
magnitude).

The following figure is a possible HATS partitioning. Note: 

* darker/bluer areas are stored in low order / high area tiles
* lighter/yellower areas are stored in higher order / lower area tiles
* the galactic plane is very prominent!

.. figure:: /_static/gaia.png
   :class: no-scaled-link
   :scale: 80 %
   :align: center
   :alt: A possible HEALPix distribution for Gaia DR3

   A possible HEALPix distribution for Gaia DR3.

File structure
-------------------------------------------------------------------------------

The catalog reader expects to find files according to the following partitioned 
structure:

.. code-block:: 
    :class: no-copybutton

      /path/to/catalogs/<catalog_name>/
      ├── dataset
      │   ├── _common_metadata
      │   ├── _metadata
      │   ├── Norder=0
      │   │   └── Dir=0
      │   │       ├── Npix=0.parquet
      │   │       └── Npix=1.parquet
      │   └── Norder=J
      │       └── Dir=10000
      │           ├── Npix=K.parquet
      │           └── Npix=M.parquet
      ├── hats.properties
      ├── partition_info.csv
      └── skymap.fits

As you can notice, ``dataset/`` has the following heirarchy:

1. ``Norder=k`` directory contains all tiles of the HEALPix order ``k``.
2. ``Dir=m`` directory contains tiles grouped by their pixel numbers, where ``m`` is
   the result of integer division of the pixel number by 10,000. This avoids directories
   becoming too large for some file systems.
3. ``Npix=n`` is the leaf node containing data for a tile with HEALPix pixel number ``n`` at order ``k``.
   Note: instead of being a single Parquet file, this can be a directory containing
   one or more Parquet files, representing a single data partition, i.e., they should
   be read together as a single data unit.

Collections
-------------------------------------------------------------------------------

HATS also makes use of supplemental tables for catalogs, and these are grouped into
catalog Collections. 

.. code-block:: 
    :class: no-copybutton

      /path/to/catalogs/<catalog_name>/
      ├── collection.properties
      ├── primary_catalog
      │   ├── dataset
      |   |   └── ...
      │   ├── hats.properties
      │   ├── partition_info.csv
      │   └── skymap.fits
      ├── id_index
      │   ├── dataset
      |   |   └── ...
      │   └── hats.properties
      └── margin_10arcs
         ├── dataset
         |   └── ...
         ├── hats.properties
         └── partition_info.csv

The ``primary_catalog`` directory will be the same format as the catalog described above.
Here we have additional supplemental tables:

* ``id_index`` - a secondary index to enable finding rows by non-spatial queries
* ``margin_10arcs`` - a cache of rows that are within a limited angular threshold
  around the primary catalog data partition, useful for cross-matching to catalogs
  with slightly different astrometry.

Properties files
-------------------------------------------------------------------------------

``hats.properties`` (per catalog) and ``collection.properties`` (per collection) are
plain-text ``key=value`` files, one property per line, holding the catalog metadata.
Most keys are required or otherwise well-defined by the HATS format; a few are optional.

``collection_version``
    *Optional, and lives only in* ``collection.properties``. A **data** version
    identifying which release of the collection's contents you have loaded (for example,
    ``v2.1.0`` for a given data release). A data release is a property of the collection
    as a whole — its main catalog, margins, and indexes are released together — so the
    version is recorded once, on the collection, rather than on each constituent catalog.
    It is read back via the ``collection_version`` property on a collection, and returns
    ``None`` for collections written before the field existed.

    This is independent of ``hats_version``, which is the version of the HATS format
    specification itself (with respect to the HATS IVOA note) and is expected to be
    stable. The two fields do not interact: a collection can bump its
    ``collection_version`` for a new data release without changing ``hats_version``.