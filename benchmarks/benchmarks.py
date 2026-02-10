"""Two sample benchmarks to compute runtime and memory usage.

For more information on writing benchmarks:
https://asv.readthedocs.io/en/stable/writing_benchmarks.html."""

from pathlib import Path

import nested_pandas as npd
import numpy as np

import hats.pixel_math as hist
import hats.pixel_math.healpix_shim as hp
from hats import read_hats
from hats.catalog import Catalog, TableProperties
from hats.pixel_math import HealpixPixel
from hats.pixel_tree import align_trees
from hats.pixel_tree.pixel_tree import PixelTree
from hats.search.region_search import cone_filter

BENCH_DATA_DIR = Path(__file__).parent / "data"


def time_test_alignment_even_sky():
    """Create alignment from an even distribution at order 7"""
    initial_histogram = np.full(hp.order2npix(7), 40)
    result = hist.generate_alignment(initial_histogram, highest_order=7, threshold=1_000)
    # everything maps to order 5, given the density
    for mapping in result:
        assert mapping[0] == 5


def time_test_cone_filter_multiple_order():
    """Create a catalog cone filter where we have multiple orders in the catalog"""
    catalog_info = TableProperties(
        **{
            "catalog_name": "test_name",
            "catalog_type": "object",
            "total_rows": 10,
            "ra_column": "ra",
            "dec_column": "dec",
        }
    )
    pixels = [HealpixPixel(6, 30), HealpixPixel(7, 124), HealpixPixel(7, 5000)]
    catalog = Catalog(catalog_info, pixels)
    filtered_catalog = catalog.filter_by_cone(47.1, 6, 30 * 3600)
    assert filtered_catalog.get_healpix_pixels() == [HealpixPixel(6, 30), HealpixPixel(7, 124)]


class Suite:
    def __init__(self) -> None:
        """Just initialize things"""
        self.pixel_list = None
        self.pixel_tree_1 = None
        self.pixel_tree_2 = None

    def setup(self):
        self.pixel_list = [HealpixPixel(8, pixel) for pixel in np.arange(100000)]
        self.pixel_tree_1 = PixelTree.from_healpix(self.pixel_list)
        self.pixel_tree_2 = PixelTree.from_healpix(
            [HealpixPixel(9, pixel) for pixel in np.arange(0, 400000, 4)]
        )

    def time_pixel_tree_creation(self):
        PixelTree.from_healpix(self.pixel_list)

    def time_inner_pixel_alignment(self):
        align_trees(self.pixel_tree_1, self.pixel_tree_2)

    def time_outer_pixel_alignment(self):
        align_trees(self.pixel_tree_1, self.pixel_tree_2, alignment_type="outer")


def time_open_midsize_catalog():
    return read_hats(BENCH_DATA_DIR / "midsize_catalog")


def time_open_large_catalog():
    return read_hats(BENCH_DATA_DIR / "large_catalog")


def time_small_cone_large_catalog():
    original_catalog = read_hats(BENCH_DATA_DIR / "large_catalog")

    original_catalog.filter_by_cone(315, -66.443, 1)


class ConeFilterSuite:
    """Benchmark the cone_filter angular separation computation."""

    def __init__(self):
        self.metadata = None
        self.data_frame = None
        self.ra = None
        self.dec = None
        self.radius_arcsec = None

    def setup(self):
        rng = np.random.default_rng(42)
        n = 100_000
        ra = rng.uniform(0, 360, n)
        dec = np.degrees(np.arcsin(rng.uniform(-1, 1, n)))
        self.metadata = TableProperties(
            catalog_name="bench",
            catalog_type="object",
            total_rows=n,
            ra_column="ra",
            dec_column="dec",
        )
        self.data_frame = npd.NestedFrame({"ra": ra, "dec": dec})
        self.ra = 180.0
        self.dec = 0.0
        self.radius_arcsec = 10.0 * 3600  # 10 degrees

    def time_cone_filter_benchmark(self):
        cone_filter(self.data_frame, self.ra, self.dec, self.radius_arcsec, self.metadata)
