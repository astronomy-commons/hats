"""Generate a molleview map with the pixel densities of the catalog

NB: Testing validity of generated plots is currently not tested in our unit test suite.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Type

import astropy.units as u
import astropy.wcs
import numpy as np
from astropy.coordinates import SkyCoord
from astropy.units import Quantity
from mocpy import MOC

import hats.pixel_math.healpix_shim as hp
from hats.inspection._plotting import _initialize_wcs_axes, _plot_healpix_value_map
from hats.io import skymap
from hats.pixel_math import HealpixPixel

if TYPE_CHECKING:
    from astropy.visualization.wcsaxes import WCSAxes
    from astropy.visualization.wcsaxes.frame import BaseFrame
    from matplotlib.colors import Colormap, Normalize
    from matplotlib.figure import Figure

    from hats.catalog import Catalog
    from hats.catalog.healpix_dataset.healpix_dataset import HealpixDataset


# pylint: disable=import-outside-toplevel,import-error
def plot_density(catalog: Catalog, *, plot_title: str | None = None, order=None, unit=None, **kwargs):
    """Create a visual map of the density of input points of a catalog on-disk.

    Parameters
    ----------
    catalog: Catalog
        on-disk catalog object
    plot_title : str | None
        Optional title for the plot
    order : int
        Optionally reduce the display healpix order, and aggregate smaller tiles. (Default value = None)
    unit : astropy.units.Unit
        Unit to show for the angle for angular density. (Default value = None)
    **kwargs
        Additional args to pass to `plot_healpix_map`
    """
    try:
        from matplotlib import pyplot as plt
    except ImportError as exc:
        raise ImportError("matplotlib is required to use this method. Install with pip or conda.") from exc

    if catalog is None or not catalog.on_disk:
        raise ValueError("on disk catalog required for point-wise visualization")
    point_map = skymap.read_skymap(catalog, order)
    order = hp.npix2order(len(point_map))

    if unit is None:
        unit = u.deg * u.deg

    pix_area = hp.order2pixarea(order, unit=unit)

    point_map = point_map / pix_area
    default_title = f"Angular density of catalog {catalog.catalog_name}"
    fig, ax = plot_healpix_map(
        point_map, title=default_title if plot_title is None else plot_title, cbar=False, **kwargs
    )
    col = ax.collections[0]
    plt.colorbar(
        col,
        label=f"count / {unit}",
    )
    return fig, ax


def plot_pixels(catalog: HealpixDataset, plot_title: str | None = None, **kwargs):
    """Create a visual map of the pixel density of the catalog.

    Parameters
    ----------
    plot_title : str | None
        Optional title for the plot
    catalog: HealpixDataset
        on-disk or in-memory catalog, with healpix pixels.
    **kwargs
        Additional args to pass to `plot_healpix_map`
    """
    pixels = catalog.get_healpix_pixels()
    default_title = f"Catalog pixel map - {catalog.catalog_name}"
    title = default_title if plot_title is None else plot_title
    if len(pixels) == 0:
        raise ValueError(f"No pixels to plot for '{title}'. Cannot generate plot.")
    return plot_pixel_list(
        pixels=pixels,
        plot_title=title,
        **kwargs,
    )


# pylint: disable=import-outside-toplevel,import-error
def plot_pixel_list(
    pixels: list[HealpixPixel], plot_title: str = "", projection="MOL", color_by_order=True, **kwargs
):
    """Create a visual map of the pixel density of a list of pixels.

    Parameters
    ----------
    pixels : list[HealpixPixel]
        healpix pixels (order and pixel number) to visualize
    plot_title : str
        (Default value = "") heading for the plot
    projection : str
        The projection to use. Available projections listed at
        https://docs.astropy.org/en/stable/wcs/supported_projections.html (Default value = "MOL")
    color_by_order : bool
        Whether to color the pixels by their order. True by default.
    **kwargs
        Additional args to pass to `plot_healpix_map`
    """
    try:
        from matplotlib import pyplot as plt
    except ImportError as exc:
        raise ImportError("matplotlib is required to use this method. Install with pip or conda.") from exc

    orders = np.array([p.order for p in pixels])
    ipix = np.array([p.pixel for p in pixels])
    order_map = orders.copy()
    fig, ax = plot_healpix_map(
        order_map, projection=projection, title=plot_title, ipix=ipix, depth=orders, cbar=False, **kwargs
    )
    col = ax.collections[0]
    if color_by_order:
        col_array = col.get_array()
        plt.colorbar(
            col,
            boundaries=np.arange(np.min(col_array) - 0.5, np.max(col_array) + 0.6, 1),
            ticks=np.arange(np.min(col_array), np.max(col_array) + 1),
            label="HEALPix Order",
        )
    else:
        col.set(cmap=None, norm=None, array=None)
    return fig, ax


# pylint: disable=import-outside-toplevel
def plot_moc(
    moc: MOC,
    *,
    projection: str = "MOL",
    title: str = "",
    fov: Quantity | tuple[Quantity, Quantity] = None,
    center: SkyCoord | None = None,
    wcs: astropy.wcs.WCS = None,
    frame_class: Type[BaseFrame] | None = None,
    ax: WCSAxes | None = None,
    fig: Figure | None = None,
    **kwargs,
) -> tuple[Figure, WCSAxes]:
    """Plots a moc

    By default, a new matplotlib figure and axis will be created, and the projection will be a Molleweide
    projection across the whole sky.

    Parameters
    ----------
    moc : mocpy.MOC
        MOC to plot
    projection : str
        The projection to use in the WCS. Available projections listed at
        https://docs.astropy.org/en/stable/wcs/supported_projections.html
        (Default value = "MOL")
    title : str
        The title of the plot (Default value = "")
    fov : Quantity | tuple[Quantity, Quantity] = None
        The Field of View of the WCS. Must be an astropy Quantity with an angular unit,
        or a tuple of quantities for different longitude and latitude FOVs (Default covers the full sky)
    center : SkyCoord | None
        The center of the projection in the WCS (Default: SkyCoord(0, 0))
    wcs : WCS | None
        The WCS to specify the projection of the plot. If used, all other WCS parameters
        are ignored and the parameters from the WCS object is used.
    frame_class : Type[BaseFrame] | None
        The class of the frame for the WCSAxes to be initialized with.
        if the `ax` kwarg is used, this value is ignored (By Default uses EllipticalFrame for full
        sky projection. If FOV is set, RectangularFrame is used)
    ax : WCSAxes | None
        The matplotlib axes to plot onto. If None, an axes will be created to be used. If
        specified, the axes must be an astropy WCSAxes, and the `wcs` parameter must be set with the WCS
        object used in the axes. (Default: None)
    fig : Figure | None
        The matplotlib figure to add the axes to. If None, one will be created, unless
        ax is specified (Default: None)
    **kwargs
        Additional kwargs to pass to `mocpy.MOC.fill`

    Returns
    -------
    Tuple[Figure, WCSAxes]
        The figure and axes used to plot the healpix map

    """
    try:
        from matplotlib import pyplot as plt
    except ImportError as exc:
        raise ImportError("matplotlib is required to use this method. Install with pip or conda.") from exc

    fig, ax, wcs = _initialize_wcs_axes(
        projection=projection,
        fov=fov,
        center=center,
        wcs=wcs,
        frame_class=frame_class,
        ax=ax,
        fig=fig,
        figsize=(9, 5),
    )

    mocpy_args = {"alpha": 0.5, "fill": True, "color": "teal"}
    mocpy_args.update(**kwargs)

    moc.fill(ax, wcs, **mocpy_args)

    ax.coords[0].set_format_unit("deg")

    plt.grid()
    plt.ylabel("Dec")
    plt.xlabel("RA")
    plt.title(title)
    return fig, ax


# pylint: disable=import-outside-toplevel,import-error
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
    fov: Quantity | tuple[Quantity, Quantity] = None,
    center: SkyCoord | None = None,
    wcs: astropy.wcs.WCS = None,
    frame_class: Type[BaseFrame] | None = None,
    ax: WCSAxes | None = None,
    fig: Figure | None = None,
    **kwargs,
):
    """Plot a map of HEALPix pixels to values as a colormap across a projection of the sky

    Plots the given healpix pixels on a spherical projection defined by a WCS. Colors each pixel based on the
    corresponding value in a map. The map can be across all healpix pixels at a given order, or
    specify a subset of healpix pixels with the `ipix` and `depth` parameters.

    By default, a new matplotlib figure and axis will be created, and the projection will be a Molleweide
    projection across the whole sky. Additional kwargs will be passed to the creation of a matplotlib
    ``PathCollection`` object, which is the artist that draws the tiles.

    Parameters
    ----------
    healpix_map : np.ndarray
        Array of map values for the healpix tiles. If ipix and depth are not
        specified, the length of this array will be used to determine the healpix order, and will plot
        healpix pixels with pixel index corresponding to the array index in NESTED ordering. If ipix and
        depth are specified, all arrays must be of the same length, and the pixels specified by the
        ipix and depth arrays will be plotted with their values specified in the healpix_map array.
    projection : str
        The projection to use in the WCS. Available projections listed at
        https://docs.astropy.org/en/stable/wcs/supported_projections.html
    title : str
        The title of the plot
    cmap : str | Colormap
        The matplotlib colormap to plot with
    norm : Normalize | None
        The matplotlib normalization to plot with
    ipix : np.ndarray | None
        Array of HEALPix NESTED pixel indices. Must be used with depth, and arrays
        must be the same length
    depth : np.ndarray | None
        Array of HEALPix pixel orders. Must be used with ipix, and arrays
        must be the same length
    cbar : bool
        If True, includes a color bar in the plot (Default: True)
    fov : Quantity or Quantity | tuple[Quantity, Quantity]
        The Field of View of the WCS. Must be an astropy Quantity with an angular unit,
        or a tuple of quantities for different longitude and latitude FOVs (Default covers the full sky)
    center : SkyCoord | None
        The center of the projection in the WCS (Default: SkyCoord(0, 0))
    wcs : WCS | None
        The WCS to specify the projection of the plot. If used, all other WCS parameters
        are ignored and the parameters from the WCS object is used.
    frame_class : Type[BaseFrame] | None
        The class of the frame for the WCSAxes to be initialized with.
        if the `ax` kwarg is used, this value is ignored (By Default uses EllipticalFrame for full
        sky projection. If FOV is set, RectangularFrame is used)
    ax : WCSAxes | None
        The matplotlib axes to plot onto. If None, an axes will be created to be used. If
        specified, the axes must be an astropy WCSAxes, and the `wcs` parameter must be set with the WCS
        object used in the axes. (Default: None)
    fig : Figure | None
        The matplotlib figure to add the axes to. If None, one will be created, unless
        ax is specified (Default: None)
    **kwargs :
        Additional kwargs to pass to creating the matplotlib `PathCollection` artist

    Returns
    -------
    Tuple[Figure, WCSAxes]
        The figure and axes used to plot the healpix map
    """
    try:
        from matplotlib import pyplot as plt
    except ImportError as exc:
        raise ImportError("matplotlib is required to use this method. Install with pip or conda.") from exc

    if ipix is None or depth is None:
        order = int(np.ceil(np.log2(len(healpix_map) / 12) / 2))
        ipix = np.arange(len(healpix_map))
        depth = np.full(len(healpix_map), fill_value=order)

    fig, ax, wcs = _initialize_wcs_axes(
        projection=projection,
        fov=fov,
        center=center,
        wcs=wcs,
        frame_class=frame_class,
        ax=ax,
        fig=fig,
        figsize=(10, 5),
    )

    _plot_healpix_value_map(ipix, depth, healpix_map, ax, wcs, cmap=cmap, norm=norm, cbar=cbar, **kwargs)
    plt.grid()
    plt.ylabel("Dec")
    plt.xlabel("RA")
    plt.title(title)
    return fig, ax
