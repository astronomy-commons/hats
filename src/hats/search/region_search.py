from __future__ import annotations

import nested_pandas as npd
import numpy as np

import hats.pixel_math.healpix_shim as hp
from hats.catalog import TableProperties
from hats.pixel_math.region_to_moc import wrap_ra_angles


def box_filter(
    data_frame: npd.NestedFrame,
    ra: tuple[float, float],
    dec: tuple[float, float],
    metadata: TableProperties,
) -> npd.NestedFrame:
    """Filters a dataframe to only include points within the specified box region.

    Parameters
    ----------
    data_frame : npd.NestedFrame
        DataFrame containing points in the sky
    ra : tuple[float,float]
        Right ascension range, in degrees
    dec : tuple[float,float]
        Declination range, in degrees
    metadata : TableProperties
        hats `Catalog` with catalog_info that matches `data_frame`

    Returns
    -------
    NestedFrame
        A new DataFrame with the rows from `data_frame` filtered to only the points inside the box region.
    """
    ra_values = data_frame[metadata.ra_column].to_numpy()
    dec_values = data_frame[metadata.dec_column].to_numpy()
    wrapped_ra = wrap_ra_angles(ra_values)
    mask_ra = _create_ra_mask(ra, wrapped_ra)
    mask_dec = (dec[0] <= dec_values) & (dec_values <= dec[1])
    data_frame = data_frame.iloc[mask_ra & mask_dec]
    return data_frame


def _create_ra_mask(ra: tuple[float, float], values: np.ndarray) -> np.ndarray:
    """Creates the mask to filter right ascension values. If this range crosses
    the discontinuity line (0 degrees), we have a branched logical operation.

    Parameters
    ----------
    ra: tuple[float, float]
        Right ascension range, in degrees
    values: np.ndarray
        values to mask

    Returns
    -------
    ndarray
        array mask of values within ra range
    """
    if ra[0] == ra[1]:
        return np.ones(len(values), dtype=bool)
    if ra[0] < ra[1]:
        mask = (values >= ra[0]) & (values <= ra[1])
    else:
        mask = ((values >= ra[0]) & (values <= 360)) | ((values >= 0) & (values <= ra[1]))
    return mask


def cone_filter(data_frame: npd.NestedFrame, ra, dec, radius_arcsec, metadata: TableProperties):
    """Filters a dataframe to only include points within the specified cone

    Parameters
    ----------
    data_frame : npd.NestedFrame
        DataFrame containing points in the sky
    ra : float
        Right Ascension of the center of the cone in degrees
    dec : float
        Declination of the center of the cone in degrees
    radius_arcsec : float
        Radius of the cone in arcseconds
    metadata : hc.TableProperties
        hats `TableProperties` with metadata that matches `data_frame`

    Returns
    -------
    NestedFrame
        A new DataFrame with the rows from `data_frame` filtered to only the points inside the cone
    """
    ra_rad = np.radians(data_frame[metadata.ra_column].to_numpy())
    dec_rad = np.radians(data_frame[metadata.dec_column].to_numpy())
    ra0 = np.radians(ra)
    dec0 = np.radians(dec)

    cos_angle = np.sin(dec_rad) * np.sin(dec0) + np.cos(dec_rad) * np.cos(dec0) * np.cos(ra_rad - ra0)

    # Clamp to valid range to avoid numerical issues
    cos_separation = np.clip(cos_angle, -1.0, 1.0)

    cos_radius = np.cos(np.radians(radius_arcsec / 3600))
    data_frame = data_frame[cos_separation >= cos_radius]
    return data_frame


def polygon_filter(data_frame: npd.NestedFrame, polygon, metadata: TableProperties) -> npd.NestedFrame:
    """Filters a dataframe to only include points within the specified polygon.

    Parameters
    ----------
    data_frame : npd.NestedFrame
        DataFrame containing points in the sky
    polygon : ConvexPolygon
        Convex spherical polygon of interest, used to filter points
    metadata : TableProperties
        hats `Catalog` with catalog_info that matches `dataframe`

    Returns
    -------
    NestedFrame
        A new DataFrame with the rows from `dataframe` filtered to only the pixels inside the polygon.
    """
    ra_values = np.radians(data_frame[metadata.ra_column].to_numpy())
    dec_values = np.radians(data_frame[metadata.dec_column].to_numpy())
    inside_polygon = polygon.contains(ra_values, dec_values)
    data_frame = data_frame.iloc[inside_polygon]
    return data_frame


# pylint: disable=import-outside-toplevel
def get_cartesian_polygon(vertices: list[tuple[float, float]]):
    """Creates the convex polygon to filter pixels with. It transforms the
    vertices, provided in sky coordinates of ra and dec, to their respective
    cartesian representation on the unit sphere.

    Parameters
    ----------
    vertices : list[tuple[float, float]]
        The list of vertices of the polygon to filter pixels with,
        as a list of (ra,dec) coordinates, in degrees.

    Returns
    -------
    sphgeom.ConvexPolygon
        The convex polygon object.
    """
    from lsst.sphgeom import ConvexPolygon, UnitVector3d  # pylint: disable=import-error

    vertices_xyz = hp.ang2vec(*np.array(vertices).T)
    edge_vectors = [UnitVector3d(x, y, z) for x, y, z in vertices_xyz]
    return ConvexPolygon(edge_vectors)
