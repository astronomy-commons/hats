"""Generate a molleview map with the pixel densities of the catalog

NB: Testing validity of generated plots is currently not tested in our unit test suite.
"""

from __future__ import annotations

from typing import Dict, List, Tuple, TYPE_CHECKING

import numpy as np
from astropy.coordinates import SkyCoord, Angle, ICRS
from astropy.visualization.wcsaxes.frame import EllipticalFrame
from astropy.wcs.utils import skycoord_to_pixel
import cdshealpix
from matplotlib import pyplot as plt, colors
from matplotlib.collections import PathCollection
from matplotlib.path import Path
import astropy.units as u
from mocpy import WCS, MOC
from mocpy.moc.plot.culling_backfacing_cells import backface_culling
from mocpy.moc.plot.fill import compute_healpix_vertices
from mocpy.moc.plot.utils import _set_wcs


from hats.io import file_io, paths
from hats.pixel_math import HealpixPixel

if TYPE_CHECKING:
    from hats.catalog import Catalog


def _read_point_map(catalog_base_dir):
    """Read the object spatial distribution information from a healpix FITS file.

    Args:
        catalog_base_dir: path to a catalog
    Returns:
        one-dimensional numpy array of long integers where the value at each index
        corresponds to the number of objects found at the healpix pixel.
    """
    map_file_pointer = paths.get_point_map_file_pointer(catalog_base_dir)
    return file_io.read_fits_image(map_file_pointer)


def plot_points(catalog: Catalog, projection="MOL", **kwargs):
    """Create a visual map of the input points of an in-memory catalog.

    Args:
        catalog (`hats.catalog.Catalog`) Catalog to display
        projection (str) The map projection to use. Valid values include:
            - moll - Molleweide projection (default)
            - gnom - Gnomonic projection
            - cart - Cartesian projection
            - orth - Orthographic projection
    """
    if not catalog.on_disk:
        raise ValueError("on disk catalog required for point-wise visualization")
    point_map = _read_point_map(catalog.catalog_base_dir)
    return plot_healpix_map(
        point_map, projection, f"Catalog point density map - {catalog.catalog_name}", **kwargs
    )


def plot_pixels(catalog: Catalog, projection="MOL", **kwargs):
    """Create a visual map of the pixel density of the catalog.

    Args:
        catalog (`hats.catalog.Catalog`) Catalog to display
        projection (str) The map projection to use. Valid values include:
            - moll - Molleweide projection (default)
            - gnom - Gnomonic projection
            - cart - Cartesian projection
            - orth - Orthographic projection
    """
    pixels = catalog.get_healpix_pixels()
    return plot_pixel_list(
        pixels=pixels,
        plot_title=f"Catalog pixel density map - {catalog.catalog_name}",
        projection=projection,
        **kwargs,
    )


def plot_pixel_list(pixels: List[HealpixPixel], plot_title: str = "", projection="MOL", **kwargs):
    """Create a visual map of the pixel density of a list of pixels.

    Args:
        pixels: healpix pixels (order and pixel number) to visualize
        plot_title (str): heading for the plot
        projection (str) The map projection to use. Valid values include:
            - moll - Molleweide projection (default)
            - gnom - Gnomonic projection
            - cart - Cartesian projection
            - orth - Orthographic projection
    """
    orders = np.array([p.order for p in pixels])
    ipix = np.array([p.pixel for p in pixels])
    order_map = orders.copy()
    fig, ax = plot_healpix_map(
        order_map, projection, plot_title, ipix=ipix, depth=orders, cbar=False, **kwargs
    )
    col = ax.collections[0]
    plt.colorbar(
        col,
        boundaries=np.arange(np.min(order_map) - 0.5, np.max(order_map) + 0.6, 1),
        ticks=np.arange(np.min(order_map), np.max(order_map) + 1),
        label="order",
    )
    return fig, ax


def cull_from_pixel_map(depth_ipix_d: Dict[int, Tuple[np.ndarray, np.ndarray]], wcs, max_split_depth=7):
    """Modified from mocpy.moc.plot.culling_backfacing_cells.from_moc

    Create a new MOC that do not contain the HEALPix cells that are backfacing the projection."""
    depths = list(depth_ipix_d.keys())
    min_depth = min(depths)
    max_depth = max(depths)
    ipixels, vals = depth_ipix_d[min_depth]

    # Split the cells located at the border of the projection
    # until at least the max_split_depth
    max_split_depth = max(max_split_depth, max_depth)

    ipix_d = {}
    for depth in range(min_depth, max_split_depth + 1):
        if depth < 3:
            too_large_ipix = ipixels
            too_large_vals = vals
        else:
            ipix_lon, ipix_lat = cdshealpix.vertices(ipixels, depth)

            ipix_lon = ipix_lon[:, [2, 3, 0, 1]]
            ipix_lat = ipix_lat[:, [2, 3, 0, 1]]
            ipix_vertices = SkyCoord(ipix_lon, ipix_lat, frame=ICRS())

            # Projection on the given WCS
            xp, yp = skycoord_to_pixel(coords=ipix_vertices, wcs=wcs)
            _, _, frontface_id = backface_culling(xp, yp)

            # Get the pixels which are backfacing the projection
            backfacing_ipix = ipixels[~frontface_id]  # backfacing
            backfacing_vals = vals[~frontface_id]
            frontface_ipix = ipixels[frontface_id]
            frontface_vals = vals[frontface_id]

            ipix_d.update({depth: (frontface_ipix, frontface_vals)})

            too_large_ipix = backfacing_ipix
            too_large_vals = backfacing_vals

        next_depth = depth + 1
        ipixels = np.array([], dtype=ipixels.dtype)
        vals = np.array([], dtype=vals.dtype)

        if next_depth in depth_ipix_d:
            ipixels, vals = depth_ipix_d[next_depth]

        # split too large ipix into next order, with each child getting the same map value as parent

        too_large_child_ipix = np.repeat(too_large_ipix << 2, 4) + np.tile(
            np.array([0, 1, 2, 3]), len(too_large_ipix)
        )
        too_large_child_vals = np.repeat(too_large_vals, 4)

        ipixels = np.concatenate((ipixels, too_large_child_ipix))
        vals = np.concatenate((vals, too_large_child_vals))

    return ipix_d


def plot_healpix_map(
    healpix_map,
    projection="MOL",
    title="",
    cmap="viridis",
    ipix=None,
    depth=None,
    cbar=True,
    fov=None,
    center=None,
    wcs=None,
    ax=None,
    **kwargs,
):
    if ipix is None or depth is None:
        order = int(np.ceil(np.log2(len(healpix_map) / 12) / 2))
        ipix = np.arange(len(healpix_map))
        depth = np.full(len(healpix_map), fill_value=order)
    fig = plt.figure(figsize=(10, 5))
    if fov is None:
        fov = (320 * u.deg, 160 * u.deg)
    if center is None:
        center = SkyCoord(0, 0, unit="deg", frame="icrs")
    if ax is None:
        if wcs is None:
            wcs = WCS(
                fig,
                fov=fov,
                center=center,
                coordsys="icrs",
                rotation=Angle(0, u.degree),
                projection=projection,
            ).w
        ax = fig.add_subplot(1, 1, 1, projection=wcs, frame_class=EllipticalFrame)
    elif wcs is None:
        raise ValueError(
            "if ax is provided, wcs must also be provided with the projection used in initializing ax"
        )
    _plot_healpix_value_map(ipix, depth, healpix_map, ax, wcs, cmap=cmap, cbar=cbar, **kwargs)
    plt.grid()
    plt.ylabel("dec")
    plt.xlabel("ra")
    plt.title(title)
    return fig, ax


def _plot_healpix_value_map(ipix, depth, values, ax, wcs, cmap="viridis", norm=None, cbar=True, **kwargs):
    """Perform the plotting of a healpix pixel map."""

    # create dict mapping depth to ipix and values
    depth_ipix_d = {}

    for d in np.unique(depth):
        mask = depth == d
        depth_ipix_d[d] = (ipix[mask], values[mask])

    # cull backfacing cells
    culled_d = cull_from_pixel_map(depth_ipix_d, wcs)

    # Generate Paths for each pixel and add to ax
    paths = []
    cum_vals = []
    for d, (ip, vals) in culled_d.items():
        vertices, codes = compute_healpix_vertices(depth=d, ipix=ip, wcs=wcs)
        for i in range(len(ip)):
            paths.append(Path(vertices[5 * i : 5 * (i + 1)], codes[5 * i : 5 * (i + 1)]))
        cum_vals.append(vals)
    col = PathCollection(paths, cmap=cmap, norm=norm, **kwargs)
    col.set_array(np.concatenate(cum_vals))
    ax.add_collection(col)

    # Add color bar
    if cbar:
        plt.colorbar(col)

    # Set projection
    _set_wcs(ax, wcs)

    return ax
