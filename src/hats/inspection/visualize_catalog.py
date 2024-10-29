"""Generate a molleview map with the pixel densities of the catalog

NB: Testing validity of generated plots is currently not tested in our unit test suite.
"""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Dict, List, Tuple, Type

import astropy.units as u
import astropy.wcs
import cdshealpix
import numpy as np
from astropy.coordinates import ICRS, Angle, SkyCoord
from astropy.units import Quantity
from astropy.visualization.wcsaxes.frame import BaseFrame, EllipticalFrame
from astropy.wcs.utils import pixel_to_skycoord, skycoord_to_pixel
from matplotlib import pyplot as plt
from matplotlib.axes import Axes
from matplotlib.collections import PathCollection
from matplotlib.colors import Colormap, Normalize
from matplotlib.figure import Figure
from matplotlib.path import Path
from mocpy import MOC, WCS
from mocpy.moc.plot.culling_backfacing_cells import backface_culling
from mocpy.moc.plot.fill import compute_healpix_vertices
from mocpy.moc.plot.utils import _set_wcs

from hats.io import file_io, paths
from hats.pixel_math import HealpixPixel
from hats.pixel_tree.moc_filter import perform_filter_by_moc
from hats.pixel_tree.pixel_tree import PixelTree

if TYPE_CHECKING:
    from hats.catalog import Catalog
    from hats.catalog.healpix_dataset.healpix_dataset import HealpixDataset


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


def plot_points(catalog: Catalog, plot_title: str | None = None, **kwargs):
    """Create a visual map of the input points of an in-memory catalog.

    Args:
        catalog (`hats.catalog.Catalog`) Catalog to display
        plot_title (str): Optional title for the plot
        kwargs: Additional args to pass to `plot_healpix_map`
    """
    if not catalog.on_disk:
        raise ValueError("on disk catalog required for point-wise visualization")
    point_map = _read_point_map(catalog.catalog_base_dir)
    default_title = f"Catalog point density map - {catalog.catalog_name}"
    return plot_healpix_map(point_map, title=default_title if plot_title is None else plot_title, **kwargs)


def plot_pixels(catalog: HealpixDataset, plot_title: str | None = None, **kwargs):
    """Create a visual map of the pixel density of the catalog.

    Args:
        catalog (`hats.catalog.Catalog`) Catalog to display
        plot_title (str): Optional title for the plot
        kwargs: Additional args to pass to `plot_healpix_map`
    """
    pixels = catalog.get_healpix_pixels()
    default_title = f"Catalog pixel density map - {catalog.catalog_name}"
    return plot_pixel_list(
        pixels=pixels,
        plot_title=default_title if plot_title is None else plot_title,
        **kwargs,
    )


def plot_pixel_list(pixels: List[HealpixPixel], plot_title: str = "", projection="MOL", **kwargs):
    """Create a visual map of the pixel density of a list of pixels.

    Args:
        pixels: healpix pixels (order and pixel number) to visualize
        plot_title (str): heading for the plot
        projection (str): The projection to use. Available projections listed at
            https://docs.astropy.org/en/stable/wcs/supported_projections.html
        kwargs: Additional args to pass to `plot_healpix_map`
    """
    orders = np.array([p.order for p in pixels])
    ipix = np.array([p.pixel for p in pixels])
    order_map = orders.copy()
    fig, ax = plot_healpix_map(
        order_map, projection=projection, title=plot_title, ipix=ipix, depth=orders, cbar=False, **kwargs
    )
    col = ax.collections[0]
    col_array = col.get_array()
    plt.colorbar(
        col,
        boundaries=np.arange(np.min(col_array) - 0.5, np.max(col_array) + 0.6, 1),
        ticks=np.arange(np.min(col_array), np.max(col_array) + 1),
        label="HEALPix Order",
    )
    return fig, ax


def cull_to_fov(depth_ipix_d: Dict[int, Tuple[np.ndarray, np.ndarray]], wcs):
    """Culls a mapping of ipix to values to pixels that are inside the plot window defined by a WCS

    Modified from mocpy.moc.plot.utils.build_plotting_moc

    Any pixels too small are merged to a lower order, with the map values within a lower order pixel being
    sampled

    Args:
        depth_ipix_d (Dict[int, Tuple[np.ndarray, np.ndarray]]): Map of HEALPix order to a tuple of 2 arrays
            (the ipix array of pixel numbers in NESTED ordering, and the values of the pixels)
        wcs (astropy.WCS): The wcs object with the plot's projection

    Returns:
        A new map with the same datatype of depth_ipix_d, with any pixels outside the plot's FOV removed,
        and any pixels too small merged with their map values subsampled.
    """

    depth_ipix_d = _merge_too_small_pixels(depth_ipix_d, wcs)

    # Get the MOC delimiting the FOV polygon
    width_px = int(wcs.wcs.crpix[0] * 2.0)  # Supposing the wcs is centered in the axis
    height_px = int(wcs.wcs.crpix[1] * 2.0)

    # Compute the sky coordinate path delimiting the viewport.
    # It consists of a closed polygon of (4 - 1)*4 = 12 vertices
    x_px = np.linspace(0, width_px, 4)
    y_px = np.linspace(0, height_px, 4)

    x, y = np.meshgrid(x_px, y_px)

    x_px = np.append(x[0, :-1], x[:-1, -1])
    x_px = np.append(x_px, x[-1, 1:][::-1])
    x_px = np.append(x_px, x[:-1, 0])

    y_px = np.append(y[0, :-1], y[:-1, -1])
    y_px = np.append(y_px, y[-1, :-1])
    y_px = np.append(y_px, y[1:, 0][::-1])

    # Disable the output of warnings when encoutering NaNs.
    warnings.filterwarnings("ignore")
    # Inverse projection from pixel coordinate space to the world coordinate space
    viewport = pixel_to_skycoord(x_px, y_px, wcs)
    # If one coordinate is a NaN we exit the function and do not go further
    ra_deg, dec_deg = viewport.icrs.ra.deg, viewport.icrs.dec.deg
    warnings.filterwarnings("default")

    if np.isnan(ra_deg).any() or np.isnan(dec_deg).any():
        return depth_ipix_d

    # Create a rough MOC (depth=3 is sufficient) from the viewport
    moc_viewport = MOC.from_polygon_skycoord(viewport, max_depth=3)

    output_dict = {}

    # The moc to plot is the INPUT_MOC & MOC_VIEWPORT. For small FOVs this can reduce
    # a lot the time to draw the MOC along with its borders.
    for d, (ip, vals) in depth_ipix_d.items():
        ip_argsort = np.argsort(ip)
        ip_sorted = ip[ip_argsort]
        pixel_tree = PixelTree(np.vstack([ip_sorted, ip_sorted + 1]).T, order=d)
        ip_viewport_mask = perform_filter_by_moc(
            pixel_tree.to_depth29_ranges(), moc_viewport.to_depth29_ranges
        )
        output_dict[d] = (ip_sorted[ip_viewport_mask], vals[ip_argsort][ip_viewport_mask])
    return output_dict


def _merge_too_small_pixels(depth_ipix_d: Dict[int, Tuple[np.ndarray, np.ndarray]], wcs):
    """Merges any pixels too small in a map to a lower order, with the map values within a lower order pixel
    being sampled"""
    # Get the WCS cdelt giving the deg.px^(-1) resolution.
    cdelt = wcs.wcs.cdelt
    # Convert in rad.px^(-1)
    cdelt = np.abs((2 * np.pi / 360) * cdelt[0])
    # Get the minimum depth such as the resolution of a cell is contained in 1px.
    depth_res = int(np.floor(np.log2(np.sqrt(np.pi / 3) / cdelt)))
    depth_res = max(depth_res, 0)

    max_depth = max(depth_ipix_d.keys())

    # Combine healpix pixels smaller than 1px in the plot
    if max_depth > depth_res:
        warnings.warn(
            "This plot contains HEALPix pixels smaller than a pixel of the plot. Some values may be lost"
        )
        new_ipix_d = {}
        for d, (ip, vals) in depth_ipix_d.items():
            if d <= depth_res:
                new_ipix_d[d] = (ip, vals)
            else:
                ipix_depth_res = ip >> (2 * (d - depth_res))
                # Get the unique pixels at the maximum depth resolution
                unique_ipix, unique_indices = np.unique(ipix_depth_res, return_index=True)
                # Get the first values from the map for each lower order pixel
                vals_depth_res = vals[unique_indices]
                if depth_res not in new_ipix_d:
                    new_ipix_d[depth_res] = (unique_ipix, vals_depth_res)
                else:
                    # combine with existing pixels if they exist
                    ipix_depth_res = np.concatenate([new_ipix_d[depth_res][0], unique_ipix])
                    vals_depth_res = np.concatenate([new_ipix_d[depth_res][1], vals_depth_res])
                    ip_argsort = np.argsort(ipix_depth_res)
                    new_ipix_d[depth_res] = (ipix_depth_res[ip_argsort], vals_depth_res[ip_argsort])
        depth_ipix_d = new_ipix_d
    return depth_ipix_d


def cull_from_pixel_map(depth_ipix_d: Dict[int, Tuple[np.ndarray, np.ndarray]], wcs, max_split_depth=7):
    """Modified from mocpy.moc.plot.culling_backfacing_cells.from_moc

    Create a new MOC that do not contain the HEALPix cells that are backfacing the projection.

    Args:
        depth_ipix_d (Dict[int, Tuple[np.ndarray, np.ndarray]]): Map of HEALPix order to a tuple of 2 arrays
            (the ipix array of pixel numbers in NESTED ordering, and the values of the pixels)
        wcs (astropy.WCS): The wcs object with the plot's projection
        max_split_depth: the max depth to split backfacing cells to

    Returns:
        A new map with the same datatype of depth_ipix_d, with backfacing cells split into higher order
    """
    depths = list(depth_ipix_d.keys())
    min_depth = min(depths)
    max_depth = max(depths)
    ipixels, vals = depth_ipix_d[min_depth]

    # Split the cells located at the border of the projection
    # until at least the max_split_depth
    max_split_depth = max(max_split_depth, max_depth)

    ipix_d = {}
    for depth in range(min_depth, max_split_depth + 1):
        # for each depth, check if pixels are too large, or wrap around projection, and split into pixels at
        # higher order
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

        # get next depth if there is one, or use empty array as default
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
    healpix_map: np.ndarray,
    *,
    projection: str = "MOL",
    title: str = "",
    cmap: str | Colormap = "viridis",
    norm: Normalize | None = None,
    ipix: np.ndarray | None = None,
    depth: np.ndarray | None = None,
    cbar: bool = True,
    fov: Quantity | Tuple[Quantity, Quantity] = None,
    center: SkyCoord | None = None,
    wcs: astropy.wcs.WCS = None,
    frame_class: Type[BaseFrame] | None = None,
    ax: Axes | None = None,
    fig: Figure | None = None,
    **kwargs,
):
    """Plot a map of HEALPix pixels to values as a colormap across a projection of the sky

    Plots the given healpix pixels on a spherical projection defined by a WCS. Colors each pixel based on the
    corresponding value in a map.

    The map can be across all healpix pixels at a given order, or specify a subset of healpix pixels with the
    `ipix` and `depth` parameters.

    By default, a new matplotlib figure and axis will be created, and the projection will be a Molleweide
    projection across the whole sky.

    Additional kwargs will be passed to the creation of a matplotlib `PathCollection` object, which is the
    artist that draws the tiles.

    Args:
        healpix_map (np.ndarray): Array of map values for the healpix tiles. If ipix and depth are not
            specified, the length of this array will be used to determine the healpix order, and will plot
            healpix pixels with pixel index corresponding to the array index in NESTED ordering. If ipix and
            depth are specified, all arrays must be of the same length, and the pixels specified by the
            ipix and depth arrays will be plotted with their values specified in the healpix_map array.
        projection (str): The projection to use in the WCS. Available projections listed at
            https://docs.astropy.org/en/stable/wcs/supported_projections.html
        title (str): The title of the plot
        cmap (str | Colormap): The matplotlib colormap to plot with
        norm (Normalize | None): The matplotlib normalization to plot with
        ipix (np.ndarray | None): Array of HEALPix NESTED pixel indices. Must be used with depth, and arrays
            must be the same length
        depth (np.ndarray | None): Array of HEALPix pixel orders. Must be used with ipix, and arrays
            must be the same length
        cbar (bool): If True, includes a color bar in the plot (Default: True)
        fov (Quantity or Sequence[Quantity, Quantity] | None): The Field of View of the WCS. Must be an
            astropy Quantity with an angular unit, or a tuple of quantities for different longitude and \
            latitude FOVs (Default covers the full sky)
        center (SkyCoord | None): The center of the projection in the WCS (Default: SkyCoord(0, 0))
        wcs (WCS | None): The WCS to specify the projection of the plot. If used, all other WCS parameters
            are ignored and the parameters from the WCS object is used.
        frame_class (Type[BaseFrame] | None): The class of the frame for the WCSAxes to be initialized with.
            if the `ax` kwarg is used, this value is ignored (By Default uses EllipticalFrame for full
            sky projection. If FOV is set, RectangularFrame is used)
        ax (Axes | None): The matplotlib axes to plot onto. If None, an axes will be created to be used. If
            specified, the axes must be initialized with a WCS for the projection, and passed to the method
            with the WCS parameter. (Default: None)
        fig (Figure | None): The matplotlib figure to add the axes to. If None, one will be created, unless
            ax is specified (Default: None)
        **kwargs: Additional kwargs to pass to creating the matplotlib `PathCollection` artist

    Returns:
        Tuple[Figure, Axes] - The figure and axes used to plot the healpix map
    """
    if ipix is None or depth is None:
        order = int(np.ceil(np.log2(len(healpix_map) / 12) / 2))
        ipix = np.arange(len(healpix_map))
        depth = np.full(len(healpix_map), fill_value=order)
    if fig is None:
        if ax is not None:
            fig = ax.get_figure()
        else:
            fig = plt.figure(figsize=(10, 5))
    if frame_class is None and fov is None and wcs is None:
        frame_class = EllipticalFrame
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
        ax = fig.add_subplot(1, 1, 1, projection=wcs, frame_class=frame_class)
    elif wcs is None:
        raise ValueError(
            "if ax is provided, wcs must also be provided with the projection used in initializing ax"
        )
    _plot_healpix_value_map(ipix, depth, healpix_map, ax, wcs, cmap=cmap, norm=norm, cbar=cbar, **kwargs)
    plt.grid()
    plt.ylabel("Dec")
    plt.xlabel("RA")
    plt.title(title)
    return fig, ax


def _plot_healpix_value_map(ipix, depth, values, ax, wcs, cmap="viridis", norm=None, cbar=True, **kwargs):
    """Perform the plotting of a healpix pixel map."""

    # create dict mapping depth to ipix and values
    depth_ipix_d = {}

    for d in np.unique(depth):
        mask = depth == d
        depth_ipix_d[d] = (ipix[mask], values[mask])

    # cull backfacing and out of fov cells
    fov_culled_d = cull_to_fov(depth_ipix_d, wcs)
    culled_d = cull_from_pixel_map(fov_culled_d, wcs)

    # Generate Paths for each pixel and add to ax
    plt_paths = []
    cum_vals = []
    for d, (ip, vals) in culled_d.items():
        vertices, codes = compute_healpix_vertices(depth=d, ipix=ip, wcs=wcs)
        for i in range(len(ip)):
            plt_paths.append(Path(vertices[5 * i : 5 * (i + 1)], codes[5 * i : 5 * (i + 1)]))
        cum_vals.append(vals)
    col = PathCollection(plt_paths, cmap=cmap, norm=norm, **kwargs)
    col.set_array(np.concatenate(cum_vals))
    ax.add_collection(col)

    # Add color bar
    if cbar:
        plt.colorbar(col)

    # Set projection
    _set_wcs(ax, wcs)

    ax.coords[0].set_format_unit("deg")
