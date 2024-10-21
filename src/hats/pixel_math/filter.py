from typing import List

from hats.pixel_math import HealpixPixel
from hats.pixel_tree import PixelAlignment, PixelAlignmentType, align_trees
from hats.pixel_tree.pixel_tree import PixelTree


def get_filtered_pixel_list(pixel_tree: PixelTree, search_tree: PixelTree) -> List[HealpixPixel]:
    """Aligns the catalog pixel tree with another pixel tree of interest, and
    extracts the HEALPix pixels that overlap. This method is useful to obtain
    the pixels intersecting a pre-defined search space.

    Args:
        pixel_tree (PixelTree): The catalog pixel tree
        search_tree (PixelTree): The tree of pixels that defines the search space

    Returns:
        A list of HEALPix pixels that overlap in both trees.
    """
    search_alignment = align_trees(pixel_tree, search_tree, alignment_type=PixelAlignmentType.INNER)
    pixels_df = search_alignment.pixel_mapping[
        [PixelAlignment.PRIMARY_ORDER_COLUMN_NAME, PixelAlignment.PRIMARY_PIXEL_COLUMN_NAME]
    ]
    filtered_pixels_df = pixels_df.drop_duplicates()
    pixel_list = [
        HealpixPixel(order, pixel)
        for order, pixel in zip(
            filtered_pixels_df[PixelAlignment.PRIMARY_ORDER_COLUMN_NAME],
            filtered_pixels_df[PixelAlignment.PRIMARY_PIXEL_COLUMN_NAME],
        )
    ]
    return pixel_list
