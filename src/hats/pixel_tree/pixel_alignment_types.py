from enum import Enum


class PixelAlignmentType(str, Enum):
    """The type of alignment between catalog trees. Like a join, the types describe if nodes should
    be included if they do not appear in the other catalog"""

    INNER = "inner"
    OUTER = "outer"
    LEFT = "left"
    RIGHT = "right"
