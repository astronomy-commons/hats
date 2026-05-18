"""High-level namespace, hats"""

from . import catalog, inspection, io, pixel_math, search
from ._version import __version__
from .io.show_versions import show_versions
from .loaders import read_hats
from .pixel_math import HealpixPixel
