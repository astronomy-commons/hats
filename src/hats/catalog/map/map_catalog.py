from __future__ import annotations

from mocpy import MOC
from typing_extensions import Self

import hats.pixel_math.healpix_shim as hp
from hats.catalog.healpix_dataset.healpix_dataset import HealpixDataset
from hats.pixel_tree.moc_utils import copy_moc


class MapCatalog(HealpixDataset): ...
