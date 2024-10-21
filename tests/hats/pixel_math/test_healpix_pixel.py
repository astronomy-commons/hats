import pytest

from hats.pixel_math.healpix_pixel import HealpixPixel
from hats.pixel_math.spatial_index import SPATIAL_INDEX_ORDER


def test_pixels_equal():
    order = 3
    pixel = 42
    pix1 = HealpixPixel(order=order, pixel=pixel)
    pix2 = HealpixPixel(order=order, pixel=pixel)
    assert pix1 == pix2
    assert pix1.order == pix2.order
    assert pix1.pixel == pix2.pixel


def test_order_greater_than_max_order_fails():
    with pytest.raises(ValueError):
        HealpixPixel(order=30, pixel=0)


def test_equal_pixel_hash_equal():
    order = 3
    pixel = 42
    test_string = "testing"
    pix1 = HealpixPixel(order=order, pixel=pixel)
    test_dict = {}
    test_dict[pix1] = test_string
    pix2 = HealpixPixel(order=order, pixel=pixel)
    assert pix1 == pix2
    assert pix2 in test_dict
    assert test_dict[pix2] == test_string


def test_pixel_str_and_repr():
    order = 3
    pixel = 42
    test_string = f"Order: {order}, Pixel: {pixel}"
    pix = HealpixPixel(order=order, pixel=pixel)
    assert str(pix) == test_string
    assert repr(pix) == test_string


# pylint: disable=pointless-statement
def test_pixel_get_item():
    order = 3
    pixel = 42
    pix = HealpixPixel(order=order, pixel=pixel)
    assert pix[0] == order
    assert pix[1] == pixel
    with pytest.raises(IndexError):
        pix[-1]
    with pytest.raises(IndexError):
        pix[3]


@pytest.mark.parametrize(
    "order, pixel, final_order, final_pixel",
    [
        (3, 10, 2, 2),
        (6, 1033, 5, 258),
        (4, 0, 3, 0),
        (5, 400, 2, 6),
        (2, 4, 0, 0),
        (3, 10, 3, 10),
    ],
)
def test_convert_lower_order(order, pixel, final_order, final_pixel):
    delta = order - final_order
    pixel = HealpixPixel(order, pixel)
    final_pix = pixel.convert_to_lower_order(delta)
    assert final_pix.order == final_order
    assert final_pix.pixel == final_pixel


def test_convert_lower_order_fails_below_zero():
    order = 4
    pixel = 3
    pixel = HealpixPixel(order, pixel)
    with pytest.raises(ValueError):
        pixel.convert_to_lower_order(order + 1)


def test_convert_lower_order_fails_negative():
    order = 4
    pixel = 3
    pixel = HealpixPixel(order, pixel)
    with pytest.raises(ValueError):
        pixel.convert_to_lower_order(-1)


@pytest.mark.parametrize(
    "order, pixel, delta_order", [(3, 10, 1), (6, 1033, 3), (4, 0, 1), (5, 400, 2), (2, 4, 0), (0, 10, 2)]
)
def test_convert_higher_order(order, pixel, delta_order):
    converted_pixels = HealpixPixel(order, pixel).convert_to_higher_order(delta_order)
    final_order = order + delta_order
    for final_pixel in range(pixel * 4**delta_order, (pixel + 1) * 4**delta_order):
        assert HealpixPixel(final_order, final_pixel) in converted_pixels


def test_convert_higher_order_fails_above_limit():
    order = 4
    pixel = 3
    pixel = HealpixPixel(order, pixel)
    with pytest.raises(ValueError):
        pixel.convert_to_higher_order(SPATIAL_INDEX_ORDER - order + 1)


def test_convert_higher_order_fails_negative():
    order = 4
    pixel = 3
    pixel = HealpixPixel(order, pixel)
    with pytest.raises(ValueError):
        pixel.convert_to_higher_order(-1)
