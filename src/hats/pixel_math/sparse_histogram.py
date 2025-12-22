"""Sparse 1-D histogram of healpix pixel counts."""

import numpy as np

import hats.pixel_math.healpix_shim as hp


class SparseHistogram:
    """Wrapper around a naive sparse array, that is just non-zero indexes and counts.

    e.g. for a dense 1-d numpy histogram of order 0, you might see::

        [0, 4, 0, 0, 0, 0, 0, 0, 9, 0, 0]

    There are only elements at [1, 8], and they have respective values [4, 9]. You
    would create the sparse histogram like::

        SparseHistogram([1, 8], [4, 9], 0)
    """

    def __init__(self, indexes, counts, order):
        if len(indexes) != len(counts):
            raise ValueError("indexes and counts must be same length")
        self.indexes = indexes
        self.counts = counts
        self.order = order

    def to_array(self):
        """Convert the sparse array to a dense numpy array.

        Returns
        -------
        np.ndarray
            dense 1-d numpy array.
        """
        dense = np.zeros(hp.order2npix(self.order), dtype=np.int64)
        dense[self.indexes] = self.counts
        return dense

    def to_file(self, file_name):
        """Persist the sparse array to disk.

        NB: this saves as a sparse array, and so will likely have lower space requirements
        than saving the corresponding dense 1-d numpy array.

        Parameters
        ----------
        file_name : path-like
            intended file to save to
        """
        np.savez(file_name, indexes=self.indexes, counts=self.counts, order=self.order)

    def to_dense_file(self, file_name):
        """Persist the DENSE array to disk as a numpy array.

        Parameters
        ----------
        file_name : path-like
            intended file to save to
        """
        with open(file_name, "wb+") as file_handle:
            file_handle.write(self.to_array().data)

    @classmethod
    def from_file(cls, file_name):
        """Read sparse histogram from a file.

        Parameters
        ----------
        file_name : path-like
            intended file to save read from

        Returns
        -------
        SparseHistogram
            new sparse histogram
        """
        npzfile = np.load(file_name)
        return cls(npzfile["indexes"], npzfile["counts"], npzfile["order"])

    def __eq__(self, value):
        if not isinstance(value, SparseHistogram):
            return False
        return (
            np.array_equal(self.indexes, value.indexes)
            and np.array_equal(self.counts, value.counts)
            and self.order == value.order
        )

    def __str__(self):
        return f"Histogram at order {self.order}\n  - indexes: {self.indexes}\n  - values: {self.counts}"


class HistogramAggregator:
    """Utility for aggregating sparse histograms."""

    def __init__(self, order):
        self.order = order
        self.full_histogram = np.zeros(hp.order2npix(order), dtype=np.int64)

    def add(self, other):
        """Add in another sparse histogram, updating this wrapper's array.

        Parameters
        ----------
        other : SparseHistogram
            the wrapper containing the addend
        """
        if other is None:
            return
        if not isinstance(other, SparseHistogram):
            raise ValueError("Both addends should be SparseHistogram.")
        if self.order != other.order:
            raise ValueError(
                "The histogram partials have incompatible sizes due to different healpix orders."
            )
        if len(other.indexes) == 0:
            return
        self.full_histogram[other.indexes] += other.counts

    def to_sparse(self):
        """Return a SparseHistogram, based on non-zero values in this aggregation."""
        indexes = self.full_histogram.nonzero()[0]
        counts = self.full_histogram[indexes]
        return SparseHistogram(indexes, counts, self.order)


def supplemental_count_histogram(mapped_pixels, supplemental_count, highest_order):
    """Specialized method for getting a histogram of some supplemental count,
    collating according to the pixels in the first argument.

    Typically used during import, when you wish to partition according to some supplemental
    data, such as in-memory size, or length of a nested column.

    Parameters
    ----------
    mapped_pixels : array_like of int
        1-D array of healpix pixel IDs. Values will be
        aggregated by pixel to produce the row-count histogram.
    supplemental_count : None or array_like of int
        Optional 1-D array of supplemental counts (for example per-row memory
        sizes or nested-column lengths). If ``None``, no supplemental histogram
        will be produced and the returned second element will be ``None``.
    highest_order : int
        Healpix order used for the histograms.

    Returns
    -------
    tuple
        ``(row_count_histo, supplemental_count_histo)`` where both elements are
        :class:`SparseHistogram`. ``row_count_histo`` contains counts of rows
        per pixel. ``supplemental_count_histo`` contains the sum of the
        supplemental counts per pixel, or ``None`` if ``supplemental_count`` was
        ``None``.
    """

    mapped_pixel, unique_inverse, count_at_pixel = np.unique(
        mapped_pixels, return_counts=True, sorted=True, return_inverse=True
    )
    row_count_histo = SparseHistogram(mapped_pixel, count_at_pixel, highest_order)

    supplemental_count_histo = None
    if supplemental_count is not None:
        if len(supplemental_count) != len(mapped_pixels):
            raise ValueError("mapped pixels and supplemental counts must be the same length")
        supplemental_sums = np.zeros(len(mapped_pixel), dtype=np.int64)

        for index, supplemental_value in zip(unique_inverse, supplemental_count, strict=True):
            supplemental_sums[index] += supplemental_value

        supplemental_count_histo = SparseHistogram(mapped_pixel, supplemental_sums, highest_order)

    return (row_count_histo, supplemental_count_histo)
