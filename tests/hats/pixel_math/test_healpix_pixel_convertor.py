import pytest

from hats.pixel_math import HealpixPixel
from hats.pixel_math.healpix_pixel_convertor import get_healpix_pixel, get_healpix_tuple


def test_get_healpix_pixel_can_take_either_argument():
    order = 5
    pixel = 10
    for arg in [HealpixPixel(order, pixel), (order, pixel)]:
        returned_pixel = get_healpix_pixel(arg)
        assert isinstance(returned_pixel, HealpixPixel)
        assert returned_pixel.order == order
        assert returned_pixel.pixel == pixel


def test_get_healpix_pixel_raises_error_for_wrong_type():
    with pytest.raises(TypeError):
        get_healpix_pixel("test")


def test_get_healpix_pixel_raises_error_for_wrong_size_tuple():
    with pytest.raises(ValueError):
        get_healpix_pixel((1, 2, 3))


def test_get_healpix_tuple_can_take_either():
    order = 5
    pixel = 10
    for arg in [HealpixPixel(order, pixel), (order, pixel)]:
        returned_pixel = get_healpix_tuple(arg)
        assert isinstance(returned_pixel, tuple)
        assert returned_pixel[0] == order
        assert returned_pixel[1] == pixel


def test_get_healpix_tuple_raises_error_for_wrong_type():
    with pytest.raises(TypeError):
        get_healpix_tuple("test")


def test_get_healpix_tuple_raises_error_for_wrong_size_tuple():
    with pytest.raises(ValueError):
        get_healpix_tuple((1, 2, 3))
