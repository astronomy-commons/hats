from enum import Enum


class CatalogType(str, Enum):
    """Enum for possible types of catalog"""

    OBJECT = "object"
    SOURCE = "source"
    ASSOCIATION = "association"
    INDEX = "index"
    MARGIN = "margin"

    @classmethod
    def all_types(cls):
        """Fetch a list of all catalog types"""
        return [t.value for t in cls]
