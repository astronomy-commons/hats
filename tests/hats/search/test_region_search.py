import numpy as np

# import nested_pandas as npd
import pandas as pd
import pytest
from astropy.coordinates import SkyCoord

import hats
from hats.search.region_search import get_cartesian_polygon


def test_box_filter_correct_points(small_sky_catalog):
    ra_range = (280, 300)
    dec_range = (-40, -30)

    filtered_cat = small_sky_catalog.filter_by_box(ra=ra_range, dec=dec_range)
    assert len(filtered_cat.get_healpix_pixels()) == 1
    df = pd.read_parquet(
        hats.io.paths.pixel_catalog_file(
            filtered_cat.catalog_base_dir,
            filtered_cat.get_healpix_pixels()[0],
            npix_suffix=filtered_cat.catalog_info.npix_suffix,
        )
    )
    box_search_df = hats.search.region_search.box_filter(df, ra_range, dec_range, filtered_cat.catalog_info)

    assert len(df) != len(box_search_df)

    ra_values = box_search_df[small_sky_catalog.catalog_info.ra_column]
    dec_values = box_search_df[small_sky_catalog.catalog_info.dec_column]

    assert all(ra_range[0] <= ra <= ra_range[1] for ra in ra_values)
    assert all(dec_range[0] <= dec <= dec_range[1] for dec in dec_values)


def test_cone_filter_correct_points(small_sky_catalog):
    ra = 0
    dec = -80
    radius_degrees = 20
    radius = radius_degrees * 3600
    center_coord = SkyCoord(ra, dec, unit="deg")

    filtered_cat = small_sky_catalog.filter_by_cone(ra, dec, radius)
    assert len(filtered_cat.get_healpix_pixels()) == 1
    df = pd.read_parquet(
        hats.io.paths.pixel_catalog_file(
            filtered_cat.catalog_base_dir,
            filtered_cat.get_healpix_pixels()[0],
            npix_suffix=filtered_cat.catalog_info.npix_suffix,
        )
    )
    cone_search_df = hats.search.region_search.cone_filter(df, ra, dec, radius, filtered_cat.catalog_info)

    assert len(df) != len(cone_search_df)

    for _, row in df.iterrows():
        row_ra = row[small_sky_catalog.catalog_info.ra_column]
        row_dec = row[small_sky_catalog.catalog_info.dec_column]
        sep = SkyCoord(row_ra, row_dec, unit="deg").separation(center_coord)
        if sep.degree <= radius_degrees:
            assert len(cone_search_df.loc[cone_search_df["id"] == row["id"]]) == 1
        else:
            assert len(cone_search_df.loc[cone_search_df["id"] == row["id"]]) == 0


@pytest.mark.sphgeom
def test_polygon_search_filters_correct_points(small_sky_catalog):
    vertices = [(300, -50), (300, -55), (272, -55), (272, -50)]
    polygon = get_cartesian_polygon(vertices)

    filtered_cat = small_sky_catalog.filter_by_polygon(vertices)
    assert len(filtered_cat.get_healpix_pixels()) == 1

    df = pd.read_parquet(
        hats.io.paths.pixel_catalog_file(
            filtered_cat.catalog_base_dir,
            filtered_cat.get_healpix_pixels()[0],
            npix_suffix=filtered_cat.catalog_info.npix_suffix,
        )
    )
    polygon_search_df = hats.search.region_search.polygon_filter(df, polygon, filtered_cat.catalog_info)

    assert len(df) != len(polygon_search_df)

    ra_values_radians = np.radians(polygon_search_df[small_sky_catalog.catalog_info.ra_column])
    dec_values_radians = np.radians(polygon_search_df[small_sky_catalog.catalog_info.dec_column])
    assert all(polygon.contains(ra_values_radians, dec_values_radians))
