"""Utilities for generating and manipulating object count histograms"""

import numpy as np
import pandas as pd

import hats.pixel_math.healpix_shim as hp


def empty_histogram(highest_order):
    """Use numpy to create an histogram array with the right shape, filled with zeros.

    Parameters
    ----------
    highest_order : int
        the highest healpix order (e.g. 0-10)

    Returns
    -------
    np.ndarray
        one-dimensional numpy array of long integers, where the length is equal to
        the number of pixels in a healpix map of target order, and all values are set to 0.
    """
    return np.zeros(hp.order2npix(highest_order), dtype=np.int64)


def generate_histogram(
    data: pd.DataFrame,
    highest_order,
    ra_column="ra",
    dec_column="dec",
):
    """Generate a histogram of counts for objects found in `data`

    Parameters
    ----------
    data : pd.DataFrame
        tabular object data
    highest_order : int
        the highest healpix order (e.g. 0-10)
    ra_column : str
        where in the input to find the celestial coordinate right ascension (Default value = "ra")
    dec_column : str
        where in the input to find the celestial coordinate declination (Default value = "dec")

    Returns
    -------
    np.ndarray
        one-dimensional numpy array of long integers where the value at each index corresponds

    Raises
    ------
    ValueError
        if the `ra_column` or `dec_column` cannot be found in the input data.
    """
    histogram_result = empty_histogram(highest_order)

    # Verify that the data frame has columns with desired names.
    required_columns = [ra_column, dec_column]
    if not all(x in data.columns for x in required_columns):
        raise ValueError(f"Invalid column names in input: {ra_column}, {dec_column}")
    mapped_pixels = hp.radec2pix(
        highest_order,
        data[ra_column].values,
        data[dec_column].values,
    )
    mapped_pixel, count_at_pixel = np.unique(mapped_pixels, return_counts=True)
    histogram_result[mapped_pixel] += count_at_pixel.astype(np.int64)
    return histogram_result


def generate_alignment(
    row_count_histogram,
    highest_order=10,
    lowest_order=0,
    threshold=1_000_000,
    drop_empty_siblings=False,
    mem_size_histogram=None,
):
    """Generate alignment from high order pixels to those of equal or lower order

    We may initially find healpix pixels at order 10, but after aggregating up to the pixel
    threshold, some final pixels are order 4 or 7. This method provides a map from pixels
    at order 10 to their destination pixel. This may be used as an input into later partitioning
    map reduce steps.

    Parameters
    ----------
    row_count_histogram : np.array
        one-dimensional numpy array of long integers where the value at each index corresponds to
        the number of objects found at the healpix pixel.
    highest_order : int
        the highest healpix order (e.g. 5-10) (Default value = 10)
    lowest_order : int
        the lowest healpix order (e.g. 1-5). specifying a lowest order
        constrains the partitioning to prevent spatially large pixels. (Default value = 0)
    threshold : int
        the maximum number of objects allowed in a single pixel (Default value = 1_000_000)
    drop_empty_siblings : bool
        if 3 of 4 pixels are empty, keep only the non-empty pixel (Default value = False)
    mem_size_histogram : np.array or None
        one-dimensional numpy array of long integers where the value at each index corresponds to
        the memory size (in bytes) of objects found at the healpix pixel. If provided, this will be
        used to determine the thresholding instead of the param `histogram`. (Default value = None)

    Returns
    -------
    tuple
        one-dimensional numpy array of integer 3-tuples, where the value at each index corresponds
        to the destination pixel at order less than or equal to the `highest_order`.
        The tuple contains three integers:

        - order of the destination pixel
        - pixel number *at the above order*
        - the number of objects in the pixel

    Raises
    ------
    ValueError
        if the histogram is the wrong size, or some initial histogram bins
        exceed threshold.
    """
    # Validate inputs.
    if len(row_count_histogram) != hp.order2npix(highest_order):
        raise ValueError("row_count_histogram is not the right size")
    if mem_size_histogram is not None and len(mem_size_histogram) != hp.order2npix(highest_order):
        raise ValueError("mem_size_histogram is not the right size")
    if lowest_order > highest_order:
        raise ValueError("lowest_order should be less than highest_order")
    if mem_size_histogram is not None:
        max_bin = np.amax(mem_size_histogram)
        if max_bin > threshold:
            raise ValueError(f"single pixel mem_size {max_bin} exceeds threshold {threshold}")
    else:
        max_bin = np.amax(row_count_histogram)
        if max_bin > threshold:
            raise ValueError(f"single pixel row count {max_bin} exceeds threshold {threshold}")

    # Generate nested sums.
    nested_sums_row_count = _count_nested_sums(row_count_histogram, highest_order, lowest_order)
    if mem_size_histogram is not None:
        nested_sums_mem_size = _count_nested_sums(mem_size_histogram, highest_order, lowest_order)
    else:
        nested_sums_mem_size = None

    # Generate alignment.
    if drop_empty_siblings:
        return _get_alignment_dropping_siblings(
            nested_sums_row_count, highest_order, lowest_order, threshold, nested_sums_mem_size
        )
    return _get_alignment(nested_sums_row_count, highest_order, lowest_order, threshold, nested_sums_mem_size)


def _count_nested_sums(histogram, highest_order, lowest_order):
    """Generate nested sums of counts at each healpix order.

    Parameters
    ----------
    histogram : np.array
        one-dimensional numpy array of long integers where the
        value at each index corresponds to the number of objects found at the healpix pixel.

    Returns
    -------
    list
        list of numpy arrays, where each array corresponds to a healpix order.
        Each array contains the counts at each pixel for that order.
    """
    nested_sums = []
    for i in range(0, highest_order):
        nested_sums.append(empty_histogram(i))
    nested_sums.append(histogram)

    # work backward - from highest order, fill in the sums of lower order pixels
    for read_order in range(highest_order, lowest_order, -1):
        parent_order = read_order - 1
        for index in range(0, len(nested_sums[read_order])):
            parent_pixel = index >> 2
            nested_sums[parent_order][parent_pixel] += nested_sums[read_order][index]
    return nested_sums


def _get_alignment(nested_sums_row_count, highest_order, lowest_order, threshold, nested_sums_mem_size):
    """Method to aggregate pixels up to the threshold.

    Checks from low order (large areas), drilling down into higher orders (smaller areas) to
    find the appropriate order for an area of sky."""
    # If nested_sums_mem_size is provided, we're in mem_size mode (and thresholding by memory size).
    # This means we'll want to use the mem_size sums to generate our alignment, but still keep track
    # of the row counts for the output.
    if nested_sums_mem_size is not None:
        nested_sums = nested_sums_mem_size
    else:
        nested_sums = nested_sums_row_count

    # Initialize our alignment structure.
    nested_alignment = []
    for i in range(0, highest_order + 1):
        nested_alignment.append(np.full(hp.order2npix(i), None))

    # Work forward - determine if we should map to a lower order pixel, this pixel, or keep looking.
    for read_order in range(lowest_order, highest_order + 1):
        parent_order = read_order - 1
        for index in range(0, len(nested_sums[read_order])):
            parent_alignment = None
            if parent_order >= 0:
                parent_pixel = index >> 2
                parent_alignment = nested_alignment[parent_order][parent_pixel]

            if parent_alignment:
                nested_alignment[read_order][index] = parent_alignment
            elif nested_sums[read_order][index] == 0:
                continue
            elif nested_sums[read_order][index] <= threshold:
                # For row_count mode, use tuple of (order, pixel, count)
                if not nested_sums_mem_size:
                    nested_alignment[read_order][index] = (
                        read_order,
                        index,
                        nested_sums[read_order][index],
                    )
                # For mem_size mode, use tuple of (order, pixel, mem_size, row_count)
                else:
                    mem_size = nested_sums_mem_size[read_order][index]
                    row_count = nested_sums_row_count[read_order][index]
                    nested_alignment[read_order][index] = (
                        read_order,
                        index,
                        mem_size,
                        row_count,
                    )

    return nested_alignment[highest_order]


def _get_alignment_dropping_siblings(
    nested_sum_row_count, highest_order, lowest_order, threshold, nested_sums_mem_size
):
    """Method to aggregate pixels up to the threshold that collapses completely empty pixels away.

    Checks from higher order (smaller areas) out to lower order (large areas). In this way, we are able to
    keep spatially isolated areas in pixels of higher order.

    This method can be slower than the above `_get_alignment` method, and so should only be used
    when the smaller area pixels are desired.

    This uses a form of hiearchical agglomeration (building a tree bottom-up). For each cell
    at order n, we look at the counts in all 4 subcells at order (n+1). We have two numeric
    values that are easy to compute that we can refer to easily:

    - quad_sum: the total number of counts in this cell
    - quad_max: the largest count within the 4 subcells

    Our agglomeration criteria (the conditions under which we collapse) must BOTH be met:

    - total number in cell is less than the global threshold (quad_sum <= threshold)
    - more than one subcell contains values (quad_sum != quad_max) (if exactly 1
      subcell contains counts, then all of the quad_sum will come from that single quad_max)

    Inversely, we will NOT collapse when EITHER is true:

    - total number in cell is greater than the threshold
    - only one subcell contains values
    """
    # If nested_sums_mem_size is provided, we're in mem_size mode (and thresholding by memory size).
    # This means we'll want to use the mem_size sums to generate our alignment, but still keep track
    # of the row counts for the output.
    if nested_sums_mem_size is not None:
        nested_sums = nested_sums_mem_size
    else:
        nested_sums = nested_sum_row_count

    # Initialize our order map to the highest order.
    order_map = np.array(
        [highest_order if count > 0 else -1 for count in nested_sums[highest_order]], dtype=np.int32
    )
    for pixel_order in range(highest_order - 1, lowest_order - 1, -1):
        for quad_start_index in range(0, hp.order2npix(pixel_order)):
            quad_sum = nested_sums[pixel_order][quad_start_index]
            quad_max = max(nested_sums[pixel_order + 1][quad_start_index * 4 : quad_start_index * 4 + 4])

            if quad_sum != quad_max and quad_sum <= threshold:
                ## Condition where we want to collapse pixels to the lower order (larger area)
                explosion_factor = 4 ** (highest_order - pixel_order)
                exploded_pixels = [
                    *range(
                        quad_start_index * explosion_factor,
                        (quad_start_index + 1) * explosion_factor,
                    )
                ]
                order_map[exploded_pixels] = pixel_order

    # Construct our results.
    nested_alignment = [
        (
            (intended_order, pixel_high_index >> 2 * (highest_order - intended_order))
            if intended_order >= 0
            else None
        )
        for pixel_high_index, intended_order in enumerate(order_map)
    ]
    # For row_count mode, use tuple of (order, pixel, count)
    if nested_sums_mem_size is None:
        nested_alignment = [
            (tup[0], tup[1], nested_sums[tup[0]][tup[1]]) if tup else None for tup in nested_alignment
        ]
    # For mem_size mode, use tuple of (order, pixel, mem_size, row_count)
    else:
        nested_alignment = [
            (
                (tup[0], tup[1], nested_sums[tup[0]][tup[1]], nested_sum_row_count[tup[0]][tup[1]])
                if tup
                else None
            )
            for tup in nested_alignment
        ]

    return np.array(nested_alignment, dtype="object")
