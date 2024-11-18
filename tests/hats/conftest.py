import pytest

from hats.loaders import read_hats
from hats.pixel_math import HealpixPixel

# pylint: disable= redefined-outer-name


@pytest.fixture
def small_sky_catalog(small_sky_dir):
    return read_hats(small_sky_dir)


@pytest.fixture()
def small_sky_pixels():
    return [
        HealpixPixel(0, 11),
    ]


@pytest.fixture
def small_sky_order1_catalog(small_sky_order1_dir):
    return read_hats(small_sky_order1_dir)


@pytest.fixture()
def small_sky_order1_pixels():
    return [
        HealpixPixel(1, 44),
        HealpixPixel(1, 45),
        HealpixPixel(1, 46),
        HealpixPixel(1, 47),
    ]


@pytest.fixture
def pixel_list_depth_first():
    """A list of pixels that are sorted by Norder (depth-first)"""
    # pylint: disable=duplicate-code
    return [
        HealpixPixel(0, 10),
        HealpixPixel(1, 33),
        HealpixPixel(1, 35),
        HealpixPixel(1, 44),
        HealpixPixel(1, 45),
        HealpixPixel(1, 46),
        HealpixPixel(2, 128),
        HealpixPixel(2, 130),
        HealpixPixel(2, 131),
    ]


@pytest.fixture
def pixel_list_breadth_first():
    """The same pixels in the above `pixel_list_depth_first` list, but
    in breadth-first traversal order by the healpix pixel hierarchy.

    See tree for illustration (brackets indicate inner node)::

        .
        ├── <0,8>
        │   ├── <1,32>
        │   │   ├── 2,128
        │   │   ├── 2,130
        │   │   └── 2,131
        │   ├── 1,33
        │   └── 1,35
        ├── 0,10
        └── <0,11>
            ├── 1,44
            ├── 1,45
            └── 1,46
    """
    return [
        HealpixPixel(2, 128),
        HealpixPixel(2, 130),
        HealpixPixel(2, 131),
        HealpixPixel(1, 33),
        HealpixPixel(1, 35),
        HealpixPixel(0, 10),
        HealpixPixel(1, 44),
        HealpixPixel(1, 45),
        HealpixPixel(1, 46),
    ]
