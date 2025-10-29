from pathlib import Path
from typing import Iterable

import numpy as np
from upath import UPath

import hats.pixel_math.healpix_shim as hp
from hats.io import file_io, paths


def read_skymap(catalog, order):
    """Read the object spatial distribution information from a healpix skymap FITS file.

    Parameters
    ----------
    catalog : Catalog
        Catalog object corresponding to an on-disk catalog.
    order : int
        healpix order to read the skymap at. If None, the order of the default
        skymap will be used. We will try to load from alternative skymap orders,
        where appropriate.

    Returns
    -------
    np.ndarray
        one-dimensional numpy array of long integers where the value at each index
        corresponds to the number of objects found at the healpix pixel.
    """
    if order is not None and catalog.catalog_info.skymap_alt_orders:
        available_orders = catalog.catalog_info.skymap_alt_orders
        best_order_idx = np.searchsorted(available_orders, order)
        if best_order_idx < len(available_orders):
            best_order = available_orders[best_order_idx]

            ## We have a file with the same order - just use it
            if best_order == order:
                return file_io.read_fits_image(
                    paths.get_skymap_file_pointer(catalog_base_dir=catalog.catalog_base_dir, order=order)
                )

            ## We have a file with a greater order - downsample it
            skymap = file_io.read_fits_image(
                paths.get_skymap_file_pointer(catalog_base_dir=catalog.catalog_base_dir, order=best_order)
            )
            return skymap.reshape(hp.order2npix(order), -1).sum(axis=1)

    if catalog.catalog_info.skymap_order:
        if order is None or order == catalog.catalog_info.skymap_order:
            return file_io.read_fits_image(
                paths.get_skymap_file_pointer(catalog_base_dir=catalog.catalog_base_dir)
            )
        if order > catalog.catalog_info.skymap_order:
            raise ValueError(
                f"order should be less than stored skymap order ({catalog.catalog_info.skymap_order})"
            )
        skymap = file_io.read_fits_image(
            paths.get_skymap_file_pointer(catalog_base_dir=catalog.catalog_base_dir)
        )
        return skymap.reshape(hp.order2npix(order), -1).sum(axis=1)

    ## Deprecated - prefer reading skymap.fits to reading point_map.fits
    map_file_pointer = paths.get_point_map_file_pointer(catalog.catalog_base_dir)
    point_map = file_io.read_fits_image(map_file_pointer)
    point_map_order = hp.npix2order(len(point_map))
    if order is None or order == point_map_order:
        return point_map
    if point_map_order < order:
        raise ValueError(f"order should be less than stored skymap order ({point_map_order})")
    return point_map.reshape(hp.order2npix(order), -1).sum(axis=1)


def write_skymap(histogram: np.ndarray, catalog_dir: str | Path | UPath, orders: list | int | None = None):
    """Write the object spatial distribution information to a healpix SKYMAP FITS file.

    Parameters
    ----------
    histogram : np.ndarray
        one-dimensional numpy array of long integers where the
        value at each index corresponds to the number of objects found at the healpix pixel.
    catalog_dir : str | Path | UPath
        base directory of the catalog in which to write the skymap file(s)
    orders : list | int | None
        list of orders to write additional skymap files. if provided and not empty,
        we will write a `skymap.K.fits` for each integer K in the list. if empty or None,
        we will not write additional files.
    """
    catalog_dir = file_io.get_upath(catalog_dir)
    map_file_pointer = paths.get_skymap_file_pointer(catalog_dir)
    file_io.write_fits_image(histogram=histogram, map_file_pointer=map_file_pointer)
    if orders:
        original_order = hp.npix2order(len(histogram))
        if not isinstance(orders, Iterable):
            ## allow input of a single order.
            orders = [orders]
        for order in orders:
            if order > original_order:
                raise ValueError(
                    f"sub-sampling skymap order should be less than overall order ({original_order})"
                )
            sampled_histogram = histogram.reshape(hp.order2npix(order), -1).sum(axis=1)
            map_file_pointer = paths.get_skymap_file_pointer(catalog_dir, order=order)
            file_io.write_fits_image(histogram=sampled_histogram, map_file_pointer=map_file_pointer)
