"""Sparse 1-D histogram of healpix pixel counts."""

import numpy as np
from scipy.sparse import csc_array, load_npz, save_npz, sparray

import hipscat.pixel_math.healpix_shim as hp


class SparseHistogram:
    """Wrapper around scipy's sparse array."""

    def __init__(self, sparse_array):
        if not isinstance(sparse_array, sparray):
            raise ValueError("The sparse array must be a scipy sparse array.")
        if sparse_array.format != "csc":
            raise ValueError("The sparse array must be a Compressed Sparse Column array.")
        self.sparse_array = sparse_array

    def add(self, other):
        """Add in another sparse histogram, updating this wrapper's array.

        Args:
            other (SparseHistogram): the wrapper containing the addend
        """
        if not isinstance(other, SparseHistogram):
            raise ValueError("Both addends should be SparseHistogram.")
        if self.sparse_array.shape != other.sparse_array.shape:
            raise ValueError(
                "The histogram partials have incompatible sizes due to different healpix orders."
            )
        self.sparse_array += other.sparse_array

    def to_array(self):
        """Convert the sparse array to a dense numpy array.

        Returns:
            dense 1-d numpy array.
        """
        return self.sparse_array.toarray()[0]

    def to_file(self, file_name):
        """Persist the sparse array to disk.

        NB: this saves as a sparse array, and so will likely have lower space requirements
        than saving the corresponding dense 1-d numpy array.
        """
        save_npz(file_name, self.sparse_array)

    def to_dense_file(self, file_name):
        """Persist the DENSE array to disk as a numpy array."""
        with open(file_name, "wb+") as file_handle:
            file_handle.write(self.to_array().data)

    @classmethod
    def make_empty(cls, healpix_order=10):
        """Create an empty sparse array for a given healpix order.

        Args:
            healpix_order (int): healpix order

        Returns:
            new sparse histogram
        """
        histo = csc_array((1, hp.order2npix(healpix_order)), dtype=np.int64)
        return cls(histo)

    @classmethod
    def make_from_counts(cls, indexes, counts_at_indexes, healpix_order=10):
        """Create an sparse array for a given healpix order, prefilled with counts at
        the provided indexes.

        e.g. for a dense 1-d numpy histogram of order 0, you might see::

            [0, 4, 0, 0, 0, 0, 0, 0, 9, 0, 0]

        There are only elements at [1, 8], and they have respective values [4, 9]. You
        would create the sparse histogram like::

            make_from_counts([1, 8], [4, 9], 0)

        Args:
            indexes (int[]): index locations of non-zero values
            counts_at_indexes (int[]): values at the ``indexes``
            healpix_order (int): healpix order

        Returns:
            new sparse histogram
        """
        row = np.array(np.zeros(len(indexes), dtype=np.int64))
        histo = csc_array((counts_at_indexes, (row, indexes)), shape=(1, hp.order2npix(healpix_order)))
        return cls(histo)

    @classmethod
    def from_file(cls, file_name):
        """Read sparse histogram from a file.

        Returns:
            new sparse histogram
        """
        histo = load_npz(file_name)
        return cls(histo)
