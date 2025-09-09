"""Utilities for generating and manipulating object count histograms"""

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

import hats.pixel_math.healpix_shim as hp


def empty_histogram(highest_order):
    """Use numpy to create an histogram array with the right shape, filled with zeros.

    Args:
        highest_order (int): the highest healpix order (e.g. 0-10)
    Returns:
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

    Args:
        data (:obj:`pd.DataFrame`): tabular object data
        highest_order (int):  the highest healpix order (e.g. 0-10)
        ra_column (str): where in the input to find the celestial coordinate, right ascension
        dec_column (str): where in the input to find the celestial coordinate, declination
    Returns:
        one-dimensional numpy array of long integers where the value at each index corresponds
        to the number of objects found at the healpix pixel.
    Raises:
        ValueError: if the `ra_column` or `dec_column` cannot be found in the input file.
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
    histogram,
    highest_order=10,
    lowest_order=0,
    threshold=1_000_000,
    byte_pixel_threshold=None,
    drop_empty_siblings=False,
):
    """Generate alignment from high order pixels to those of equal or lower order

    We may initially find healpix pixels at order 10, but after aggregating up to the pixel
    threshold, some final pixels are order 4 or 7. This method provides a map from pixels
    at order 10 to their destination pixel. This may be used as an input into later partitioning
    map reduce steps.

    Args:
        histogram (:obj:`np.array`): one-dimensional numpy array of long integers where the
            value at each index corresponds to the number of objects found at the healpix pixel.
        highest_order (int): the highest healpix order (e.g. 5-10)
        lowest_order (int): the lowest healpix order (e.g. 1-5). specifying a lowest order
            constrains the partitioning to prevent spatially large pixels.
        threshold (int): the maximum number of objects allowed in a single pixel
        byte_pixel_threshold (int | None): the maximum number of objects allowed in a single pixel,
            expressed in bytes. if this is set, it will override `threshold`.

        drop_empty_siblings (bool): if 3 of 4 pixels are empty, keep only the non-empty pixel
    Returns:
        one-dimensional numpy array of integer 3-tuples, where the value at each index corresponds
        to the destination pixel at order less than or equal to the `highest_order`.

        The tuple contains three integers:
        - order of the destination pixel
        - pixel number *at the above order*
        - the number of objects in the pixel (if partitioning by row count), or the memory size (if partitioning by memory)

    Note:
        If partitioning is done by memory size, the row count per partition may vary widely and will not match the row count histogram's bins.
    Raises:
        ValueError: if the histogram is the wrong size, or some initial histogram bins
            exceed threshold.
    """
    if len(histogram) != hp.order2npix(highest_order):
        raise ValueError("histogram is not the right size")
    if lowest_order > highest_order:
        raise ValueError("lowest_order should be less than highest_order")

    # Determine aggregation type and threshold
    if byte_pixel_threshold is not None:
        agg_threshold = byte_pixel_threshold
        agg_type = "memory"
    else:
        agg_threshold = threshold
        agg_type = "row_count"

    # Check that none of the high-order pixels already exceed the threshold.
    max_bin = np.amax(histogram)
    if agg_type == "memory" and max_bin > agg_threshold:
        raise ValueError(f"single pixel size {max_bin} bytes exceeds byte_pixel_threshold {agg_threshold}")
    elif agg_type == "row_count" and max_bin > agg_threshold:
        raise ValueError(f"single pixel count {max_bin} exceeds threshold {agg_threshold}")

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

    # Use the aggregation threshold for alignment
    if drop_empty_siblings:
        return _get_alignment_dropping_siblings(nested_sums, highest_order, lowest_order, agg_threshold)
    return _get_alignment(nested_sums, highest_order, lowest_order, agg_threshold)


def _get_alignment(nested_sums, highest_order, lowest_order, threshold):
    """Method to aggregate pixels up to the threshold.

    Checks from low order (large areas), drilling down into higher orders (smaller areas) to
    find the appropriate order for an area of sky."""
    nested_alignment = []
    for i in range(0, highest_order + 1):
        nested_alignment.append(np.full(hp.order2npix(i), None))

    # work forward - determine if we should map to a lower order pixel, this pixel, or keep looking.
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
                nested_alignment[read_order][index] = (
                    read_order,
                    index,
                    nested_sums[read_order][index],
                )

    return nested_alignment[highest_order]


def _get_alignment_dropping_siblings(nested_sums, highest_order, lowest_order, threshold):
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
    nested_alignment = [
        (tup[0], tup[1], nested_sums[tup[0]][tup[1]]) if tup else None for tup in nested_alignment
    ]

    return np.array(nested_alignment, dtype="object")


def generate_row_count_histogram_from_partitions(partition_files, pixel_orders, pixel_indices, highest_order):
    """
    Generate a row count histogram from a list of partition files and their pixel indices/orders.

    Args:
        partition_files (list[str or UPath]): List of paths to partition files.
        pixel_orders (list[int]): List of healpix orders for each partition.
        pixel_indices (list[int]): List of healpix pixel indices for each partition.
        highest_order (int): The highest healpix order (for histogram size).

    Returns:
        np.ndarray: One-dimensional numpy array of long integers, where the value at each index
            corresponds to the number of rows found at the healpix pixel.

    Note:
        If partitioning was done by memory size, this histogram will reflect the actual row counts
        in the output partitions, which may differ significantly from the original row count histogram.
    """
    import logging

    histogram = np.zeros(hp.order2npix(highest_order), dtype=np.int64)
    for file_path, order, pix in zip(partition_files, pixel_orders, pixel_indices):
        try:
            table = pq.read_table(file_path)
            row_count = len(table)
            # Map pixel index to highest_order if needed
            if order == highest_order:
                histogram[pix] += row_count
            else:
                # Map lower order pixel to highest_order pixel indices
                # Each lower order pixel covers 4**(highest_order - order) pixels
                factor = 4 ** (highest_order - order)
                start = pix * factor
                end = (pix + 1) * factor
                histogram[start:end] += row_count // factor
        except Exception as e:
            logging.warning(f"Could not read partition file {file_path}: {e}")
            continue
    return histogram
