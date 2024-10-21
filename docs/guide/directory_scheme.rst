HATS Directory Scheme
===============================================================================

Partitioning Scheme
-------------------------------------------------------------------------------

We use healpix (`Hierarchical Equal Area isoLatitude Pixelization <https://healpix.jpl.nasa.gov/>`_)
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
    
    __ /path/to/catalogs/<catalog_name>/
       |__ partition_info.csv
       |__ properties
       |__ dataset/
           |__ _common_metadata
           |__ _metadata
           |__ Norder=1/
           |   |__ Dir=0/
           |       |__ Npix=0.parquet
           |       |__ Npix=1.parquet
           |__ Norder=J/
               |__ Dir=10000/
                   |__ Npix=K.parquet
                   |__ Npix=M.parquet