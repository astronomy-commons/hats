import numpy as np
import numpy.testing as npt

from hats.pixel_math.healpix_pixel_function import get_pixel_argsort, sort_pixels


def test_get_pixel_argsort(pixel_list_depth_first, pixel_list_breadth_first):
    argsort = get_pixel_argsort(pixel_list_depth_first)
    npt.assert_array_equal(argsort, [6, 7, 8, 1, 2, 0, 3, 4, 5])
    sorted_pixel_list = np.array(pixel_list_depth_first)[argsort]
    npt.assert_array_equal(sorted_pixel_list, pixel_list_breadth_first)


def test_get_pixel_argsort_empty():
    argsort = get_pixel_argsort([])
    npt.assert_array_equal(argsort, [])

    argsort = get_pixel_argsort(None)
    npt.assert_array_equal(argsort, [])

    argsort = get_pixel_argsort(np.array([]))
    npt.assert_array_equal(argsort, [])


def test_sort_pixels(pixel_list_depth_first, pixel_list_breadth_first):
    sort_result = sort_pixels(pixel_list_depth_first)
    npt.assert_array_equal(sort_result, pixel_list_breadth_first)

    sort_result = sort_pixels(pixel_list_depth_first)
    npt.assert_array_equal(sort_result, pixel_list_breadth_first)


def test_sort_pixels_empty():
    sort_result = sort_pixels([])
    npt.assert_array_equal(sort_result, [])

    sort_result = sort_pixels(None)
    npt.assert_array_equal(sort_result, [])

    sort_result = sort_pixels(np.array([]))
    npt.assert_array_equal(sort_result, [])
