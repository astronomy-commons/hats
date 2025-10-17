import os.path
from pathlib import Path

import pyarrow as pa
import pytest

from hats.catalog.dataset.table_properties import TableProperties
from hats.loaders import read_hats
from hats.pixel_math import HealpixPixel

DATA_DIR_NAME = "data"
SMALL_SKY_DIR_NAME = "small_sky"
SMALL_SKY_NPIX_ALT_SUFFIX_NAME = "small_sky_npix_alt_suffix"
SMALL_SKY_NPIX_AS_DIR_NAME = "small_sky_npix_as_dir"
SMALL_SKY_SOURCE_OBJECT_INDEX_DIR_NAME = "small_sky_source_object_index"
SMALL_SKY_NESTED_DIR_NAME = "small_sky_nested"

TEST_DIR = os.path.dirname(__file__)

# pylint: disable=missing-function-docstring, redefined-outer-name


@pytest.fixture
def test_data_dir():
    return Path(TEST_DIR) / DATA_DIR_NAME


@pytest.fixture
def small_sky_dir(test_data_dir):
    return test_data_dir / SMALL_SKY_DIR_NAME


@pytest.fixture
def small_sky_npix_alt_suffix_dir(test_data_dir):
    return test_data_dir / SMALL_SKY_NPIX_ALT_SUFFIX_NAME


@pytest.fixture
def small_sky_npix_as_dir_dir(test_data_dir):
    return test_data_dir / SMALL_SKY_NPIX_AS_DIR_NAME


@pytest.fixture
def small_sky_order1_dir(test_data_dir):
    return test_data_dir / "small_sky_o1_collection" / "small_sky_order1"


@pytest.fixture
def small_sky_order1_id_index_dir(test_data_dir):
    return test_data_dir / "small_sky_o1_collection" / "small_sky_order1_id_index"


@pytest.fixture
def small_sky_nested_dir(test_data_dir):
    return test_data_dir / SMALL_SKY_NESTED_DIR_NAME


@pytest.fixture
def small_sky_source_object_index_dir(test_data_dir):
    return test_data_dir / SMALL_SKY_SOURCE_OBJECT_INDEX_DIR_NAME


@pytest.fixture
def catalog_info_data() -> dict:
    return {
        "catalog_name": "test_name",
        "catalog_type": "object",
        "total_rows": 10,
        "ra_column": "ra",
        "dec_column": "dec",
    }


@pytest.fixture
def catalog_info(catalog_info_data) -> TableProperties:
    return TableProperties(**catalog_info_data)


@pytest.fixture
def association_catalog_info_data() -> dict:
    return {
        "catalog_name": "test_name",
        "catalog_type": "association",
        "total_rows": 10,
        "primary_catalog": "small_sky",
        "primary_column": "id",
        "primary_column_association": "id_small_sky",
        "join_catalog": "small_sky_order1",
        "join_column": "id",
        "join_column_association": "id_small_sky_order1",
        "contains_leaf_files": False,
    }


@pytest.fixture
def association_catalog_info(association_catalog_info_data) -> TableProperties:
    return TableProperties(**association_catalog_info_data)


@pytest.fixture
def source_catalog_info() -> dict:
    return {
        "catalog_name": "test_source",
        "catalog_type": "source",
        "total_rows": 100,
        "ra_column": "source_ra",
        "dec_column": "source_dec",
    }


@pytest.fixture
def margin_cache_catalog_info_data() -> dict:
    return {
        "catalog_name": "test_margin",
        "catalog_type": "margin",
        "total_rows": 100,
        "ra_column": "ra",
        "dec_column": "dec",
        "primary_catalog": "test_name",
        "margin_threshold": 0.5,
    }


@pytest.fixture
def margin_catalog_info(margin_cache_catalog_info_data) -> TableProperties:
    return TableProperties(**margin_cache_catalog_info_data)


@pytest.fixture
def small_sky_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("_healpix_29", pa.int64()),
            pa.field("id", pa.int64()),
            pa.field("ra", pa.float64()),
            pa.field("dec", pa.float64()),
            pa.field("ra_error", pa.int64()),
            pa.field("dec_error", pa.int64()),
        ]
    )


@pytest.fixture
def small_sky_source_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("_healpix_29", pa.int64()),
            pa.field("source_id", pa.int64()),
            pa.field("source_ra", pa.float64()),
            pa.field("source_dec", pa.float64()),
            pa.field("mjd", pa.float64()),
            pa.field("mag", pa.float64()),
            pa.field("band", pa.string()),
            pa.field("object_id", pa.int64()),
            pa.field("object_ra", pa.float64()),
            pa.field("object_dec", pa.float64()),
        ]
    )


@pytest.fixture
def margin_catalog_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("_healpix_29", pa.int64()),
            pa.field("id", pa.int64()),
            pa.field("ra", pa.float64()),
            pa.field("dec", pa.float64()),
            pa.field("ra_error", pa.int64()),
            pa.field("dec_error", pa.int64()),
        ]
    )


@pytest.fixture
def dataset_path(test_data_dir) -> str:
    return test_data_dir / "info_only" / "dataset"


@pytest.fixture
def catalog_path(test_data_dir) -> str:
    return test_data_dir / "info_only" / "catalog"


@pytest.fixture
def collection_path(test_data_dir) -> str:
    return test_data_dir / "info_only" / "collection"


@pytest.fixture
def margin_catalog_pixels() -> list[HealpixPixel]:
    return [
        HealpixPixel(0, 4),
        HealpixPixel(0, 7),
        HealpixPixel(0, 8),
        HealpixPixel(1, 44),
        HealpixPixel(1, 45),
        HealpixPixel(1, 46),
        HealpixPixel(1, 47),
    ]


@pytest.fixture
def margin_catalog_path(test_data_dir) -> str:
    return test_data_dir / "small_sky_o1_collection" / "small_sky_order1_margin"


@pytest.fixture
def catalog_pixels() -> list[HealpixPixel]:
    return [HealpixPixel(1, 0), HealpixPixel(1, 1), HealpixPixel(2, 8)]


@pytest.fixture
def association_catalog_path(test_data_dir) -> str:
    return test_data_dir / "small_sky_to_small_sky_order1"


@pytest.fixture
def small_sky_source_dir(test_data_dir) -> str:
    return test_data_dir / "small_sky_source"


@pytest.fixture
def small_sky_collection_dir(test_data_dir) -> str:
    return test_data_dir / "small_sky_o1_collection"


@pytest.fixture
def small_sky_source_pixels():
    """Source catalog pixels"""
    return [
        HealpixPixel(0, 4),
        HealpixPixel(2, 176),
        HealpixPixel(2, 177),
        HealpixPixel(2, 178),
        HealpixPixel(2, 179),
        HealpixPixel(2, 180),
        HealpixPixel(2, 181),
        HealpixPixel(2, 182),
        HealpixPixel(2, 183),
        HealpixPixel(2, 184),
        HealpixPixel(2, 185),
        HealpixPixel(2, 186),
        HealpixPixel(2, 187),
        HealpixPixel(1, 47),
    ]


@pytest.fixture
def small_sky_catalog(small_sky_dir):
    return read_hats(small_sky_dir)


# pylint: disable=import-outside-toplevel
def pytest_collection_modifyitems(items):
    """Modify tests that use the `lsst-sphgeom` package to only run when that
    package has been installed in the development environment.

    If we detect that we can import `lsst-sphgeom`, this method exits early
    and does not modify any test items.
    """
    try:
        # pylint: disable=unused-import
        from lsst.sphgeom import ConvexPolygon

        return
    except ImportError:
        pass

    for item in items:
        if any(item.iter_markers(name="sphgeom")):
            item.add_marker(pytest.mark.skip(reason="lsst-sphgeom is not installed"))
