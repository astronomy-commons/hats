from unittest.mock import MagicMock

import astropy.units as u
import matplotlib.pyplot as plt
import numpy as np
import pytest
from astropy.coordinates import Angle, SkyCoord
from astropy.visualization.wcsaxes.frame import EllipticalFrame, RectangularFrame
from matplotlib import colors
from matplotlib.colors import LogNorm, Normalize
from matplotlib.path import Path
from matplotlib.pyplot import get_cmap
from mocpy import MOC, WCS
from mocpy.moc.plot import fill
from mocpy.moc.plot.culling_backfacing_cells import from_moc
from mocpy.moc.plot.utils import build_plotting_moc

from hats.inspection import plot_pixels
from hats.inspection.visualize_catalog import (
    compute_healpix_vertices,
    cull_from_pixel_map,
    cull_to_fov,
    plot_healpix_map,
    plot_moc,
)

# pylint: disable=no-member


DEFAULT_CMAP_NAME = "viridis"
DEFAULT_FOV = (320 * u.deg, 160 * u.deg)
DEFAULT_CENTER = SkyCoord(0, 0, unit="deg", frame="icrs")
DEFAULT_COORDSYS = "icrs"
DEFAULT_ROTATION = Angle(0, u.degree)
DEFAULT_PROJECTION = "MOL"


@pytest.fixture(autouse=True)
def reset_matplotlib():
    yield
    plt.close("all")


def test_healpix_vertices():
    depth = 3
    ipix = np.array([10, 11])
    fig = plt.figure()
    wcs = WCS(
        fig,
        fov=DEFAULT_FOV,
        center=DEFAULT_CENTER,
        coordsys=DEFAULT_COORDSYS,
        rotation=DEFAULT_ROTATION,
        projection=DEFAULT_PROJECTION,
    ).w
    paths, codes = compute_healpix_vertices(depth, ipix, wcs)
    mocpy_paths, mocpy_codes = fill.compute_healpix_vertices(depth, ipix, wcs)
    np.testing.assert_array_equal(paths, mocpy_paths)
    np.testing.assert_array_equal(codes, mocpy_codes)


def test_healpix_vertices_step():
    depth = 1
    ipix = np.array([10, 11])
    fig = plt.figure()
    step = 4
    wcs = WCS(
        fig,
        fov=DEFAULT_FOV,
        center=DEFAULT_CENTER,
        coordsys=DEFAULT_COORDSYS,
        rotation=DEFAULT_ROTATION,
        projection=DEFAULT_PROJECTION,
    ).w
    paths, codes = compute_healpix_vertices(depth, ipix, wcs, step=step)
    # checks the codes match the expected matplotlib path codes
    np.testing.assert_array_equal(
        codes, np.tile(np.array([Path.MOVETO] + [Path.LINETO] * (step * 4 - 1) + [Path.CLOSEPOLY]), len(ipix))
    )
    mocpy_paths, _ = fill.compute_healpix_vertices(depth, ipix, wcs)
    # mocpy only generates path points at the healpix corner vertices. So we subsample our generated vertices
    # to check the corners match the expected mocpy generated ones
    first_path_vertex_indices = np.array([0, step, 2 * step, 3 * step, 4 * step])
    start_path_index = np.array(([0] * 5) + ([first_path_vertex_indices[-1] + 1] * 5))
    vertex_indices = start_path_index + np.tile(first_path_vertex_indices, len(ipix))
    np.testing.assert_array_equal(paths[vertex_indices], mocpy_paths)


def test_plot_healpix_pixels():
    order = 3
    length = 10
    ipix = np.arange(length)
    pix_map = np.arange(length)
    depth = np.full(length, fill_value=order)
    fig, ax = plot_healpix_map(pix_map, ipix=ipix, depth=depth)
    assert len(ax.collections) > 0
    col = ax.collections[0]
    paths = col.get_paths()
    assert len(paths) == length
    assert col.get_cmap() == get_cmap(DEFAULT_CMAP_NAME)
    assert isinstance(col.norm, Normalize)
    assert col.norm.vmin == min(pix_map)
    assert col.norm.vmax == max(pix_map)
    assert col.colorbar is not None
    assert col.colorbar.cmap == get_cmap(DEFAULT_CMAP_NAME)
    assert col.colorbar.norm == col.norm
    wcs = WCS(
        fig,
        fov=DEFAULT_FOV,
        center=DEFAULT_CENTER,
        coordsys=DEFAULT_COORDSYS,
        rotation=DEFAULT_ROTATION,
        projection=DEFAULT_PROJECTION,
    ).w
    for path, ipix in zip(paths, ipix):
        verts, codes = compute_healpix_vertices(order, np.array([ipix]), wcs)
        np.testing.assert_array_equal(path.vertices, verts)
        np.testing.assert_array_equal(path.codes, codes)
    np.testing.assert_array_equal(col.get_array(), pix_map)
    assert ax.frame_class == EllipticalFrame


def test_plot_healpix_pixels_different_order():
    order = 6
    length = 1000
    ipix = np.arange(length)
    pix_map = np.arange(length)
    depth = np.full(length, fill_value=order)
    fig, ax = plot_healpix_map(pix_map, ipix=ipix, depth=depth)
    assert len(ax.collections) > 0
    col = ax.collections[0]
    paths = col.get_paths()
    assert len(paths) == length
    wcs = WCS(
        fig,
        fov=DEFAULT_FOV,
        center=DEFAULT_CENTER,
        coordsys=DEFAULT_COORDSYS,
        rotation=DEFAULT_ROTATION,
        projection=DEFAULT_PROJECTION,
    ).w

    all_verts, all_codes = compute_healpix_vertices(order, ipix, wcs)
    for i, (path, ipix) in enumerate(zip(paths, ipix)):
        verts, codes = all_verts[i * 5 : (i + 1) * 5], all_codes[i * 5 : (i + 1) * 5]
        np.testing.assert_array_equal(path.vertices, verts)
        np.testing.assert_array_equal(path.codes, codes)
    np.testing.assert_array_equal(col.get_array(), pix_map)


def test_order_0_pixel_plots_with_step():
    map_value = 0.5
    order_0_pix = 4
    ipix = np.array([order_0_pix])
    pix_map = np.array([map_value])
    depth = np.array([0])
    fig, ax = plot_healpix_map(pix_map, ipix=ipix, depth=depth)
    assert len(ax.collections) > 0
    col = ax.collections[0]
    paths = col.get_paths()
    length = 1
    assert len(paths) == length
    wcs = WCS(
        fig,
        fov=DEFAULT_FOV,
        center=DEFAULT_CENTER,
        coordsys=DEFAULT_COORDSYS,
        rotation=DEFAULT_ROTATION,
        projection=DEFAULT_PROJECTION,
    ).w
    all_verts, all_codes = compute_healpix_vertices(0, ipix, wcs, step=2**3)
    # assert the number of vertices == number of pixels * 4 sides per pixel * steps per side + 1 for the
    # close polygon
    assert len(all_verts) == len(ipix) * 4 * (2**3) + 1
    np.testing.assert_array_equal(paths[0].vertices, all_verts)
    np.testing.assert_array_equal(paths[0].codes, all_codes)
    np.testing.assert_array_equal(col.get_array(), np.full(length, fill_value=map_value))


def test_edge_pixels_split_to_order_7():
    map_value = 0.5
    order_0_pix = 2
    ipix = np.array([order_0_pix])
    pix_map = np.array([map_value])
    depth = np.array([0])
    fig, ax = plot_healpix_map(pix_map, ipix=ipix, depth=depth)
    assert len(ax.collections) > 0

    # Generate a dictionary of pixel indices that have sides that align with the meridian at ra = 180deg, the
    # right edge of the plot
    edge_pixels = {0: [order_0_pix]}
    for iter_ord in range(1, 8):
        edge_pixels[iter_ord] = [p * 4 + i for p in edge_pixels[iter_ord - 1] for i in (2, 3)]

    # Generate a dictionary of subpixels of the order 0 pixel that are not on the edge of the plot, i.e. the
    # pixels that should be in the culled plot
    non_edge_pixels = {}
    pixels_ord1 = np.arange(4 * order_0_pix, 4 * (order_0_pix + 1))
    non_edge_pixels[1] = pixels_ord1[~np.isin(pixels_ord1, edge_pixels[1])]
    for iter_ord in range(2, 8):
        pixels_ord = np.concatenate([np.arange(4 * pix, 4 * (pix + 1)) for pix in edge_pixels[iter_ord - 1]])
        non_edge_pixels[iter_ord] = pixels_ord[~np.isin(pixels_ord, edge_pixels[iter_ord])]
    col = ax.collections[0]
    paths = col.get_paths()

    # Check that the plotted paths match the non_edge_pixels generated
    length = sum(len(x) for x in non_edge_pixels.values())
    assert len(paths) == length
    wcs = WCS(
        fig,
        fov=DEFAULT_FOV,
        center=DEFAULT_CENTER,
        coordsys=DEFAULT_COORDSYS,
        rotation=DEFAULT_ROTATION,
        projection=DEFAULT_PROJECTION,
    ).w
    ords = np.concatenate([np.full(len(x), fill_value=o) for o, x in non_edge_pixels.items()])
    pixels = np.concatenate([np.array(x) for _, x in non_edge_pixels.items()])
    for path, iter_ord, pix in zip(paths, ords, pixels):
        step = 1 if iter_ord >= 3 else 2 ** (3 - iter_ord)
        verts, codes = compute_healpix_vertices(iter_ord, np.array([pix]), wcs, step=step)
        np.testing.assert_array_equal(path.vertices, verts)
        np.testing.assert_array_equal(path.codes, codes)
    np.testing.assert_array_equal(col.get_array(), np.full(length, fill_value=map_value))


def test_cull_from_pixel_map():
    order = 3
    ipix = np.arange(12 * 4**order)
    pix_map = np.arange(12 * 4**order)
    map_dict = {order: (ipix, pix_map)}
    fig = plt.figure(figsize=(10, 5))
    wcs = WCS(
        fig,
        fov=DEFAULT_FOV,
        center=DEFAULT_CENTER,
        coordsys=DEFAULT_COORDSYS,
        rotation=DEFAULT_ROTATION,
        projection=DEFAULT_PROJECTION,
    ).w
    culled_dict = cull_from_pixel_map(map_dict, wcs)
    mocpy_culled = from_moc({str(order): ipix}, wcs)
    for iter_ord, (pixels, m) in culled_dict.items():
        np.testing.assert_array_equal(pixels, mocpy_culled[str(iter_ord)])
        map_indices = pixels >> (2 * (iter_ord - order))
        np.testing.assert_array_equal(m, pix_map[map_indices])


def test_cull_to_fov():
    order = 4
    ipix = np.arange(12 * 4**order)
    pix_map = np.arange(12 * 4**order)
    map_dict = {order: (ipix, pix_map)}
    fig = plt.figure(figsize=(10, 5))
    wcs = WCS(
        fig,
        fov=(20 * u.deg, 10 * u.deg),
        center=DEFAULT_CENTER,
        coordsys=DEFAULT_COORDSYS,
        rotation=DEFAULT_ROTATION,
        projection=DEFAULT_PROJECTION,
    ).w
    culled_dict = cull_to_fov(map_dict, wcs)
    moc = MOC.from_healpix_cells(ipix, np.full(ipix.shape, fill_value=order), max_depth=order)
    mocpy_culled = build_plotting_moc(moc, wcs)
    for iter_ord, (pixels, m) in culled_dict.items():
        for p in pixels:
            assert (
                len(
                    MOC.from_healpix_cells(np.array([p]), np.array([iter_ord]), max_depth=iter_ord)
                    .intersection(mocpy_culled)
                    .to_depth29_ranges
                )
                > 0
            )
        ord_ipix = np.arange(12 * 4**iter_ord)
        ord_non_pixels = ord_ipix[~np.isin(ord_ipix, pixels)]
        for p in ord_non_pixels:
            assert (
                len(
                    MOC.from_healpix_cells(np.array([p]), np.array([iter_ord]), max_depth=iter_ord)
                    .intersection(mocpy_culled)
                    .to_depth29_ranges
                )
                == 0
            )
        map_indices = pixels >> (2 * (iter_ord - order))
        np.testing.assert_array_equal(m, pix_map[map_indices])


def test_cull_to_fov_subsamples_high_order():
    order = 10
    ipix = np.arange(12 * 4**order)
    pix_map = np.arange(12 * 4**order)
    map_dict = {order: (ipix, pix_map)}
    fig = plt.figure(figsize=(10, 5))
    wcs = WCS(
        fig,
        fov=DEFAULT_FOV,
        center=DEFAULT_CENTER,
        coordsys=DEFAULT_COORDSYS,
        rotation=DEFAULT_ROTATION,
        projection=DEFAULT_PROJECTION,
    ).w
    with pytest.warns(match="smaller"):
        culled_dict = cull_to_fov(map_dict, wcs)
    # Get the WCS cdelt giving the deg.px^(-1) resolution.
    cdelt = wcs.wcs.cdelt
    # Convert in rad.px^(-1)
    cdelt = np.abs((2 * np.pi / 360) * cdelt[0])
    # Get the minimum depth such as the resolution of a cell is contained in 1px.
    depth_res = int(np.floor(np.log2(np.sqrt(np.pi / 3) / cdelt)))
    depth_res = max(depth_res, 0)
    assert depth_res < order

    for iter_ord, (pixels, m) in culled_dict.items():
        assert iter_ord == depth_res
        assert np.all(np.isin(ipix >> (2 * (order - depth_res)), pixels))
        map_indices = pixels << (2 * (order - depth_res))
        np.testing.assert_array_equal(m, pix_map[map_indices])


def test_cull_to_fov_subsamples_multiple_orders():
    depth = np.array([0, 5, 8, 10])
    ipix = np.array([10, 5, 4, 2])
    pix_map = np.array([1, 2, 3, 4])
    map_dict = {depth[i]: (ipix[[i]], pix_map[[i]]) for i in range(len(depth))}
    fig = plt.figure(figsize=(10, 5))
    wcs = WCS(
        fig,
        fov=DEFAULT_FOV,
        center=DEFAULT_CENTER,
        coordsys=DEFAULT_COORDSYS,
        rotation=DEFAULT_ROTATION,
        projection=DEFAULT_PROJECTION,
    ).w
    with pytest.warns(match="smaller"):
        culled_dict = cull_to_fov(map_dict, wcs)
    # Get the WCS cdelt giving the deg.px^(-1) resolution.
    cdelt = wcs.wcs.cdelt
    # Convert in rad.px^(-1)
    cdelt = np.abs((2 * np.pi / 360) * cdelt[0])
    # Get the minimum depth such as the resolution of a cell is contained in 1px.
    depth_res = int(np.floor(np.log2(np.sqrt(np.pi / 3) / cdelt)))
    depth_res = max(depth_res, 0)
    assert depth_res < max(depth)

    assert list(culled_dict.keys()) == [0, 5, depth_res]

    assert culled_dict[0] == (np.array([10]), np.array([1]))
    assert culled_dict[5] == (np.array([5]), np.array([2]))
    small_pixels_map = pix_map[2:]
    small_pixels_converted = ipix[2:] >> (2 * (depth[2:] - depth_res))
    small_pixels_argsort = np.argsort(small_pixels_converted)
    assert np.all(culled_dict[depth_res][0] == small_pixels_converted[small_pixels_argsort])
    assert np.all(culled_dict[depth_res][1] == small_pixels_map[small_pixels_argsort])


def test_plot_healpix_map():
    order = 1
    ipix = np.arange(12 * 4**order)
    pix_map = np.arange(12 * 4**order)
    fig, ax = plot_healpix_map(pix_map)
    assert len(ax.collections) > 0
    col = ax.collections[0]
    paths = col.get_paths()
    wcs = WCS(
        fig,
        fov=DEFAULT_FOV,
        center=DEFAULT_CENTER,
        coordsys=DEFAULT_COORDSYS,
        rotation=DEFAULT_ROTATION,
        projection=DEFAULT_PROJECTION,
    ).w
    map_dict = {order: (ipix, pix_map)}
    culled_dict = cull_from_pixel_map(map_dict, wcs)
    all_vals = []
    start_i = 0
    for iter_ord, (pixels, pix_map) in culled_dict.items():
        step = 1 if iter_ord >= 3 else 2 ** (3 - iter_ord)
        vert_len = step * 4 + 1
        all_verts, all_codes = compute_healpix_vertices(iter_ord, pixels, wcs, step=step)
        for i, _ in enumerate(pixels):
            verts, codes = (
                all_verts[i * vert_len : (i + 1) * vert_len],
                all_codes[i * vert_len : (i + 1) * vert_len],
            )
            path = paths[start_i + i]
            np.testing.assert_array_equal(path.vertices, verts)
            np.testing.assert_array_equal(path.codes, codes)
        all_vals.append(pix_map)
        start_i += len(pixels)
    assert start_i == len(paths)
    np.testing.assert_array_equal(np.concatenate(all_vals), col.get_array())


def test_plot_wcs_params():
    order = 3
    length = 10
    ipix = np.arange(length)
    pix_map = np.arange(length)
    depth = np.full(length, fill_value=order)
    fig, ax = plot_healpix_map(
        pix_map,
        ipix=ipix,
        depth=depth,
        fov=(100 * u.deg, 50 * u.deg),
        center=SkyCoord(10, 10, unit="deg", frame="icrs"),
        projection="AIT",
    )
    assert len(ax.collections) > 0
    col = ax.collections[0]
    paths = col.get_paths()
    assert len(paths) == length
    wcs = WCS(
        fig,
        fov=(100 * u.deg, 50 * u.deg),
        center=SkyCoord(10, 10, unit="deg", frame="icrs"),
        coordsys=DEFAULT_COORDSYS,
        rotation=DEFAULT_ROTATION,
        projection="AIT",
    ).w
    for path, ipix in zip(paths, np.arange(len(pix_map))):
        verts, codes = compute_healpix_vertices(order, np.array([ipix]), wcs)
        np.testing.assert_array_equal(path.vertices, verts)
        np.testing.assert_array_equal(path.codes, codes)
    np.testing.assert_array_equal(col.get_array(), pix_map)
    assert ax.get_transform("icrs") is not None
    assert ax.frame_class == RectangularFrame


def test_plot_wcs_params_frame():
    order = 3
    length = 10
    ipix = np.arange(length)
    pix_map = np.arange(length)
    depth = np.full(length, fill_value=order)
    fig, ax = plot_healpix_map(
        pix_map,
        ipix=ipix,
        depth=depth,
        fov=(100 * u.deg, 50 * u.deg),
        center=SkyCoord(10, 10, unit="deg", frame="icrs"),
        projection="AIT",
        frame_class=EllipticalFrame,
    )
    assert len(ax.collections) > 0
    col = ax.collections[0]
    paths = col.get_paths()
    assert len(paths) == length
    wcs = WCS(
        fig,
        fov=(100 * u.deg, 50 * u.deg),
        center=SkyCoord(10, 10, unit="deg", frame="icrs"),
        coordsys=DEFAULT_COORDSYS,
        rotation=DEFAULT_ROTATION,
        projection="AIT",
    ).w
    for path, ipix in zip(paths, np.arange(len(pix_map))):
        verts, codes = compute_healpix_vertices(order, np.array([ipix]), wcs)
        np.testing.assert_array_equal(path.vertices, verts)
        np.testing.assert_array_equal(path.codes, codes)
    np.testing.assert_array_equal(col.get_array(), pix_map)
    assert ax.get_transform("icrs") is not None
    assert ax.frame_class == EllipticalFrame


def test_plot_fov_culling():
    order = 3
    length = 10
    ipix = np.arange(length)
    pix_map = np.arange(length)
    depth = np.full(length, fill_value=order)
    fig, ax = plot_healpix_map(
        pix_map,
        ipix=ipix,
        depth=depth,
        fov=(30 * u.deg, 20 * u.deg),
        center=SkyCoord(10, 10, unit="deg", frame="icrs"),
        projection="AIT",
    )
    assert len(ax.collections) > 0
    col = ax.collections[0]
    paths = col.get_paths()
    wcs = WCS(
        fig,
        fov=(30 * u.deg, 20 * u.deg),
        center=SkyCoord(10, 10, unit="deg", frame="icrs"),
        coordsys=DEFAULT_COORDSYS,
        rotation=DEFAULT_ROTATION,
        projection="AIT",
    ).w
    map_dict = {order: (ipix, pix_map)}
    culled_dict = cull_to_fov(map_dict, wcs)
    all_vals = []
    start_i = 0
    for iter_ord, (pixels, pix_map) in culled_dict.items():
        all_verts, all_codes = compute_healpix_vertices(iter_ord, pixels, wcs)
        for i, _ in enumerate(pixels):
            verts, codes = all_verts[i * 5 : (i + 1) * 5], all_codes[i * 5 : (i + 1) * 5]
            path = paths[start_i + i]
            np.testing.assert_array_equal(path.vertices, verts)
            np.testing.assert_array_equal(path.codes, codes)
        all_vals.append(pix_map)
        start_i += len(pixels)
    assert start_i == len(paths)
    np.testing.assert_array_equal(np.concatenate(all_vals), col.get_array())
    assert ax.get_transform("icrs") is not None
    assert ax.frame_class == RectangularFrame


def test_plot_wcs():
    order = 3
    length = 10
    ipix = np.arange(length)
    pix_map = np.arange(length)
    depth = np.full(length, fill_value=order)
    fig = plt.figure(figsize=(10, 5))
    wcs = WCS(
        fig,
        fov=(100 * u.deg, 50 * u.deg),
        center=SkyCoord(10, 10, unit="deg", frame="icrs"),
        coordsys=DEFAULT_COORDSYS,
        rotation=DEFAULT_ROTATION,
        projection="AIT",
    ).w
    fig2, ax = plot_healpix_map(pix_map, ipix=ipix, depth=depth, fig=fig, wcs=wcs)
    assert fig2 is fig
    assert len(ax.collections) > 0
    col = ax.collections[0]
    paths = col.get_paths()
    assert len(paths) == length
    for path, ipix in zip(paths, np.arange(len(pix_map))):
        verts, codes = compute_healpix_vertices(order, np.array([ipix]), wcs)
        np.testing.assert_array_equal(path.vertices, verts)
        np.testing.assert_array_equal(path.codes, codes)
    np.testing.assert_array_equal(col.get_array(), pix_map)
    assert ax.get_transform("icrs") is not None


def test_plot_wcs_and_ax():
    order = 3
    length = 10
    ipix = np.arange(length)
    pix_map = np.arange(length)
    depth = np.full(length, fill_value=order)
    fig = plt.figure(figsize=(10, 5))
    wcs = WCS(
        fig,
        fov=(100 * u.deg, 50 * u.deg),
        center=SkyCoord(10, 10, unit="deg", frame="icrs"),
        coordsys=DEFAULT_COORDSYS,
        rotation=DEFAULT_ROTATION,
        projection="AIT",
    ).w
    ax = fig.add_subplot(1, 1, 1, projection=wcs, frame_class=EllipticalFrame)
    assert len(ax.collections) == 0
    fig2, ax2 = plot_healpix_map(pix_map, ipix=ipix, depth=depth, fig=fig, wcs=wcs, ax=ax)
    assert fig2 is fig
    assert ax2 is ax
    assert len(ax.collections) > 0
    col = ax.collections[0]
    paths = col.get_paths()
    assert len(paths) == length
    for path, ipix in zip(paths, np.arange(len(pix_map))):
        verts, codes = compute_healpix_vertices(order, np.array([ipix]), wcs)
        np.testing.assert_array_equal(path.vertices, verts)
        np.testing.assert_array_equal(path.codes, codes)
    np.testing.assert_array_equal(col.get_array(), pix_map)
    assert ax.get_transform("icrs") is not None


def test_plot_ax_no_wcs():
    order = 3
    length = 10
    ipix = np.arange(length)
    pix_map = np.arange(length)
    depth = np.full(length, fill_value=order)
    fig = plt.figure(figsize=(10, 5))
    wcs = WCS(
        fig,
        fov=(100 * u.deg, 50 * u.deg),
        center=SkyCoord(10, 10, unit="deg", frame="icrs"),
        coordsys=DEFAULT_COORDSYS,
        rotation=DEFAULT_ROTATION,
        projection="AIT",
    ).w
    ax = fig.add_subplot(1, 1, 1, projection=wcs, frame_class=EllipticalFrame)
    assert len(ax.collections) == 0
    fig2, ax2 = plot_healpix_map(pix_map, ipix=ipix, depth=depth, fig=fig, ax=ax)
    assert fig2 is fig
    assert ax2 is ax
    assert len(ax.collections) > 0
    col = ax.collections[0]
    paths = col.get_paths()
    assert len(paths) == length
    for path, ipix in zip(paths, np.arange(len(pix_map))):
        verts, codes = compute_healpix_vertices(order, np.array([ipix]), wcs)
        np.testing.assert_array_equal(path.vertices, verts)
        np.testing.assert_array_equal(path.codes, codes)
    np.testing.assert_array_equal(col.get_array(), pix_map)
    assert ax.get_transform("icrs") is not None


def test_plot_cmaps():
    order = 3
    length = 10
    cmap_name = "plasma"
    ipix = np.arange(length)
    pix_map = np.arange(length)
    depth = np.full(length, fill_value=order)
    fig, ax = plot_healpix_map(pix_map, ipix=ipix, depth=depth, cmap=cmap_name)
    assert len(ax.collections) > 0
    col = ax.collections[0]
    paths = col.get_paths()
    assert col.get_cmap() == get_cmap(cmap_name)
    assert col.colorbar is not None
    assert col.colorbar.cmap == get_cmap(cmap_name)
    assert len(paths) == length
    wcs = WCS(
        fig,
        fov=DEFAULT_FOV,
        center=DEFAULT_CENTER,
        coordsys=DEFAULT_COORDSYS,
        rotation=DEFAULT_ROTATION,
        projection=DEFAULT_PROJECTION,
    ).w
    for path, ipix in zip(paths, np.arange(len(pix_map))):
        verts, codes = compute_healpix_vertices(order, np.array([ipix]), wcs)
        np.testing.assert_array_equal(path.vertices, verts)
        np.testing.assert_array_equal(path.codes, codes)
    np.testing.assert_array_equal(col.get_array(), pix_map)

    ipix = np.arange(length)
    pix_map = np.arange(length)
    depth = np.full(length, fill_value=order)
    fig, ax = plot_healpix_map(pix_map, ipix=ipix, depth=depth, cmap=get_cmap(cmap_name))
    assert len(ax.collections) > 0
    col = ax.collections[0]
    paths = col.get_paths()
    assert col.get_cmap() == get_cmap(cmap_name)
    assert col.colorbar is not None
    assert col.colorbar.cmap == get_cmap(cmap_name)
    assert len(paths) == length
    for path, ipix in zip(paths, np.arange(len(pix_map))):
        verts, codes = compute_healpix_vertices(order, np.array([ipix]), wcs)
        np.testing.assert_array_equal(path.vertices, verts)
        np.testing.assert_array_equal(path.codes, codes)
    np.testing.assert_array_equal(col.get_array(), pix_map)


def test_plot_norm():
    order = 3
    length = 10
    norm = LogNorm()
    ipix = np.arange(length)
    pix_map = np.arange(length)
    depth = np.full(length, fill_value=order)
    fig, ax = plot_healpix_map(pix_map, ipix=ipix, depth=depth, norm=norm)
    assert len(ax.collections) > 0
    col = ax.collections[0]
    paths = col.get_paths()
    assert col.norm == norm
    assert col.colorbar is not None
    assert col.colorbar.norm == norm
    assert len(paths) == length
    wcs = WCS(
        fig,
        fov=DEFAULT_FOV,
        center=DEFAULT_CENTER,
        coordsys=DEFAULT_COORDSYS,
        rotation=DEFAULT_ROTATION,
        projection=DEFAULT_PROJECTION,
    ).w
    for path, ipix in zip(paths, np.arange(len(pix_map))):
        verts, codes = compute_healpix_vertices(order, np.array([ipix]), wcs)
        np.testing.assert_array_equal(path.vertices, verts)
        np.testing.assert_array_equal(path.codes, codes)
    np.testing.assert_array_equal(col.get_array(), pix_map)


def test_plot_no_cbar():
    order = 3
    length = 10
    ipix = np.arange(length)
    pix_map = np.arange(length)
    depth = np.full(length, fill_value=order)
    fig, ax = plot_healpix_map(pix_map, ipix=ipix, depth=depth, cbar=False)
    assert len(ax.collections) > 0
    col = ax.collections[0]
    assert col.colorbar is None
    paths = col.get_paths()
    assert len(paths) == length
    wcs = WCS(
        fig,
        fov=DEFAULT_FOV,
        center=DEFAULT_CENTER,
        coordsys=DEFAULT_COORDSYS,
        rotation=DEFAULT_ROTATION,
        projection=DEFAULT_PROJECTION,
    ).w
    for path, ipix in zip(paths, np.arange(len(pix_map))):
        verts, codes = compute_healpix_vertices(order, np.array([ipix]), wcs)
        np.testing.assert_array_equal(path.vertices, verts)
        np.testing.assert_array_equal(path.codes, codes)
    np.testing.assert_array_equal(col.get_array(), pix_map)


def test_plot_kwargs():
    order = 3
    length = 10
    label = "test"
    ipix = np.arange(length)
    pix_map = np.arange(length)
    depth = np.full(length, fill_value=order)
    fig, ax = plot_healpix_map(pix_map, ipix=ipix, depth=depth, label=label)
    assert len(ax.collections) > 0
    col = ax.collections[0]
    assert col.get_label() == label
    paths = col.get_paths()
    assert len(paths) == length
    wcs = WCS(
        fig,
        fov=DEFAULT_FOV,
        center=DEFAULT_CENTER,
        coordsys=DEFAULT_COORDSYS,
        rotation=DEFAULT_ROTATION,
        projection=DEFAULT_PROJECTION,
    ).w
    for path, ipix in zip(paths, np.arange(len(pix_map))):
        verts, codes = compute_healpix_vertices(order, np.array([ipix]), wcs)
        np.testing.assert_array_equal(path.vertices, verts)
        np.testing.assert_array_equal(path.codes, codes)
    np.testing.assert_array_equal(col.get_array(), pix_map)


def test_plot_existing_fig():
    order = 3
    length = 10
    ipix = np.arange(length)
    pix_map = np.arange(length)
    depth = np.full(length, fill_value=order)
    fig = plt.figure()
    assert len(fig.axes) == 0
    fig_ret, ax_ret = plot_healpix_map(pix_map, ipix=ipix, depth=depth)
    assert fig is fig_ret
    ax = fig.get_axes()[0]
    assert ax is ax_ret
    assert len(ax.collections) > 0
    col = ax.collections[0]
    paths = col.get_paths()
    assert len(paths) == length
    wcs = WCS(
        fig,
        fov=DEFAULT_FOV,
        center=DEFAULT_CENTER,
        coordsys=DEFAULT_COORDSYS,
        rotation=DEFAULT_ROTATION,
        projection=DEFAULT_PROJECTION,
    ).w
    for path, ipix in zip(paths, np.arange(len(pix_map))):
        verts, codes = compute_healpix_vertices(order, np.array([ipix]), wcs)
        np.testing.assert_array_equal(path.vertices, verts)
        np.testing.assert_array_equal(path.codes, codes)
    np.testing.assert_array_equal(col.get_array(), pix_map)


def test_plot_existing_wcsaxes():
    order = 3
    length = 10
    ipix = np.arange(length)
    pix_map = np.arange(length)
    depth = np.full(length, fill_value=order)
    fig = plt.figure()
    wcs = WCS(
        fig,
        fov=DEFAULT_FOV,
        center=DEFAULT_CENTER,
        coordsys=DEFAULT_COORDSYS,
        rotation=DEFAULT_ROTATION,
        projection=DEFAULT_PROJECTION,
    ).w
    ax = fig.add_subplot(1, 1, 1, projection=wcs)
    assert len(fig.axes) == 1
    assert len(ax.collections) == 0
    fig_ret, ax_ret = plot_healpix_map(pix_map, ipix=ipix, depth=depth)
    assert fig is fig_ret
    assert ax is ax_ret
    assert len(ax.collections) > 0
    col = ax.collections[0]
    paths = col.get_paths()
    assert len(paths) == length
    for path, ipix in zip(paths, np.arange(len(pix_map))):
        verts, codes = compute_healpix_vertices(order, np.array([ipix]), wcs)
        np.testing.assert_array_equal(path.vertices, verts)
        np.testing.assert_array_equal(path.codes, codes)
    np.testing.assert_array_equal(col.get_array(), pix_map)


def test_plot_existing_wrong_axes():
    order = 3
    length = 10
    ipix = np.arange(length)
    pix_map = np.arange(length)
    depth = np.full(length, fill_value=order)
    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)
    assert len(fig.axes) == 1
    assert len(ax.collections) == 0
    with pytest.warns(match="WCSAxes"):
        fig_ret, ax_ret = plot_healpix_map(pix_map, ipix=ipix, depth=depth)
    assert fig is not fig_ret
    assert ax is not ax_ret
    assert len(ax.collections) == 0
    assert len(ax_ret.collections) == 1
    col = ax_ret.collections[0]
    paths = col.get_paths()
    wcs = WCS(
        fig_ret,
        fov=DEFAULT_FOV,
        center=DEFAULT_CENTER,
        coordsys=DEFAULT_COORDSYS,
        rotation=DEFAULT_ROTATION,
        projection=DEFAULT_PROJECTION,
    ).w
    assert len(paths) == length
    for path, ipix in zip(paths, np.arange(len(pix_map))):
        verts, codes = compute_healpix_vertices(order, np.array([ipix]), wcs)
        np.testing.assert_array_equal(path.vertices, verts)
        np.testing.assert_array_equal(path.codes, codes)
    np.testing.assert_array_equal(col.get_array(), pix_map)


def test_catalog_plot(small_sky_order1_catalog):
    fig, ax = plot_pixels(small_sky_order1_catalog)
    pixels = sorted(small_sky_order1_catalog.get_healpix_pixels())
    col = ax.collections[0]
    paths = col.get_paths()
    assert len(paths) == len(pixels)
    wcs = WCS(
        fig,
        fov=DEFAULT_FOV,
        center=DEFAULT_CENTER,
        coordsys=DEFAULT_COORDSYS,
        rotation=DEFAULT_ROTATION,
        projection=DEFAULT_PROJECTION,
    ).w
    for p, path in zip(pixels, paths):
        step = 2 ** (3 - p.order)
        verts, codes = compute_healpix_vertices(p.order, np.array([p.pixel]), wcs, step=step)
        np.testing.assert_array_equal(path.vertices, verts)
        np.testing.assert_array_equal(path.codes, codes)
    np.testing.assert_array_equal(col.get_array(), np.array([p.order for p in pixels]))
    assert ax.get_title() == f"Catalog pixel density map - {small_sky_order1_catalog.catalog_name}"


def test_catalog_plot_no_color_by_order(small_sky_order1_catalog):
    fc = "white"
    ec = "black"
    fig, ax = plot_pixels(small_sky_order1_catalog, color_by_order=False, facecolor=fc, edgecolor=ec)
    pixels = sorted(small_sky_order1_catalog.get_healpix_pixels())
    col = ax.collections[0]
    paths = col.get_paths()
    assert len(paths) == len(pixels)
    wcs = WCS(
        fig,
        fov=DEFAULT_FOV,
        center=DEFAULT_CENTER,
        coordsys=DEFAULT_COORDSYS,
        rotation=DEFAULT_ROTATION,
        projection=DEFAULT_PROJECTION,
    ).w
    for p, path in zip(pixels, paths):
        step = 2 ** (3 - p.order)
        verts, codes = compute_healpix_vertices(p.order, np.array([p.pixel]), wcs, step=step)
        np.testing.assert_array_equal(path.vertices, verts)
        np.testing.assert_array_equal(path.codes, codes)
    assert col.get_array() is None
    np.testing.assert_array_equal(col.get_facecolor()[0], colors.to_rgba(fc))
    np.testing.assert_array_equal(col.get_edgecolor()[0], colors.to_rgba(ec))
    assert ax.get_title() == f"Catalog pixel density map - {small_sky_order1_catalog.catalog_name}"


def test_plot_moc(small_sky_order1_catalog):
    small_sky_order1_catalog.moc.fill = MagicMock()
    _, ax = plot_moc(small_sky_order1_catalog.moc)
    small_sky_order1_catalog.moc.fill.assert_called_once()
    assert small_sky_order1_catalog.moc.fill.call_args[0][0] is ax
    wcs = ax.wcs
    assert small_sky_order1_catalog.moc.fill.call_args[0][1] is wcs


def test_plot_moc_catalog(small_sky_order1_catalog):
    small_sky_order1_catalog.moc.fill = MagicMock()
    _, ax = small_sky_order1_catalog.plot_moc()
    small_sky_order1_catalog.moc.fill.assert_called_once()
    assert small_sky_order1_catalog.moc.fill.call_args[0][0] is ax
    wcs = ax.wcs
    assert small_sky_order1_catalog.moc.fill.call_args[0][1] is wcs
