"""Utility functions for handling parquet metadata files"""

from __future__ import annotations

import io
import random
from pathlib import Path

import nested_pandas as npd
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from astropy.io.votable.tree import FieldRef, Group, Param, VOTableFile
from astropy.table import Table
from upath import UPath

from hats.io import file_io, paths
from hats.io.file_io.file_pointer import get_upath
from hats.pixel_math.healpix_pixel import HealpixPixel
from hats.pixel_math.healpix_pixel_function import get_pixel_argsort
from hats.pixel_math.spatial_index import SPATIAL_INDEX_COLUMN


# pylint: disable=too-many-locals
def write_parquet_metadata(
    catalog_path: str | Path | UPath,
    order_by_healpix=True,
    output_path: str | Path | UPath | None = None,
    create_thumbnail: bool = False,
    thumbnail_threshold: int = 1_000_000,
    create_metadata: bool = True,
):
    """Write Parquet dataset-level metadata files (and optional thumbnail) for a catalog.

    Creates files::

        catalog/
        ├── ...
        └── dataset/
            ├── _common_metadata          (always written)
            ├── _metadata                 (only if create_metadata=True)
            ├── data_thumbnail.parquet    (only if create_thumbnail=True)
            └──  ...

    ``dataset/_common_metadata`` contains the full schema of the dataset. This file
    will know all of the columns and their types, as well as any file-level key-value
    metadata associated with the full Parquet dataset.

    ``dataset/_metadata`` contains the combined row group footers from all Parquet files
    in the dataset, which allows readers to read the entire dataset without having
    to open each individual Parquet file. This file can be large for datasets with
    many files, so users may choose to omit it by setting ``create_metadata=False``.

    ``dataset/data_thumbnail.parquet`` gives the user a quick overview of the whole dataset.
    It is a compact file containing one row from each data partition, up to a maximum
    of ``thumbnail_threshold`` rows.

    Notes
    -----
    For more information on the general Parquet metadata files, and why we write them, see
    https://arrow.apache.org/docs/python/parquet.html#writing-metadata-and-common-metadata-files

    For more information on HATS-specific metadata files and conventions, see
    https://www.ivoa.net/documents/Notes/HATS/

    Parameters
    ----------
    catalog_path : str | Path | UPath
        Base path for the catalog root.
    order_by_healpix : bool, default=True
        If True, reorder combined metadata by breadth-first Healpix pixel ordering
        (e.g., secondary indexes). Set False for datasets that should not be reordered.
        Does not modify dataset files on disk.
    output_path : str | Path | UPath | None, default=None
        Base path to write metadata files. If None, uses ``catalog_path``.
    create_thumbnail : bool, default=False
        If True, writes a compact ``data_thumbnail.parquet`` containing one row per
        sampled file.
    thumbnail_threshold : int, default=1_000_000
        Maximum number of rows in the thumbnail (or maximum number of files, if
        thumbnail_threshold exceeds the number of files). One row per partition.
    create_metadata : bool, default=True
        If True, writes ``dataset/_metadata`` combining row group footers.

    Returns
    -------
    int
        Total number of rows across all parquet files in the dataset.
    """
    ignore_prefixes = [
        "_common_metadata",
        "_metadata",
        "data_thumbnail",
    ]

    catalog_path = get_upath(catalog_path)
    dataset_subdir = catalog_path / "dataset"
    (dataset_path, dataset) = file_io.read_parquet_dataset(dataset_subdir, ignore_prefixes=ignore_prefixes)
    metadata_collector = []
    # Collect the healpix pixels so we can sort before writing.
    healpix_pixels = []
    total_rows = 0

    # Collect the first rows for the data thumbnail.
    first_rows = []
    pq_file_list = set()
    if create_thumbnail:
        # The thumbnail_threshold threshold is the maximum number of Parquet rows
        # per pixel, used to prevent memory issues. It doesn't make sense for the
        # thumbnail to have more rows than this. If it does, randomly sample those
        # available.
        row_limit = min(len(dataset.files), thumbnail_threshold)
        # Create set for O(1) lookups in the loop that follows
        pq_file_list = set(random.sample(dataset.files, row_limit))

    # Pass over all files: always count rows; optionally collect metadata & thumbnail rows.
    for single_file in dataset.files:
        relative_path = single_file[len(dataset_path) + 1 :]
        file = file_io.read_parquet_file(dataset_subdir / relative_path)
        single_metadata = file.metadata
        total_rows += single_metadata.num_rows

        if create_metadata:
            # Users must set the file path of each chunk before combining the metadata.
            single_metadata.set_file_path(relative_path)
            if order_by_healpix:
                healpix_pixel = paths.get_healpix_from_path(relative_path)
                healpix_pixels.append(healpix_pixel)
            metadata_collector.append(single_metadata)

        if create_thumbnail and single_file in pq_file_list:
            # Grab a single-row batch for this file for the thumbnail.
            first_rows.append(next(file.iter_batches(batch_size=1)))

    # Set up output path.
    if output_path is None:
        output_path = catalog_path
    if order_by_healpix:
        argsort = get_pixel_argsort(healpix_pixels)
        metadata_collector = np.array(metadata_collector)[argsort]
    catalog_base_dir = get_upath(output_path)
    file_io.make_directory(catalog_base_dir / "dataset", exist_ok=True)

    # Write out the _metadata file.
    if create_metadata:
        metadata_file_pointer = paths.get_parquet_metadata_pointer(catalog_base_dir)
        file_io.write_parquet_metadata(
            dataset.schema,
            metadata_file_pointer,
            metadata_collector=metadata_collector,
            write_statistics=True,
        )

    # Write out the _common_metadata file.
    common_metadata_file_pointer = paths.get_common_metadata_pointer(catalog_base_dir)
    file_io.write_parquet_metadata(dataset.schema, common_metadata_file_pointer)

    # Write out the thumbnail file.
    if create_thumbnail:
        data_thumbnail_pointer = paths.get_data_thumbnail_pointer(catalog_path)
        data_thumbnail = pa.Table.from_batches(first_rows, dataset.schema)
        if SPATIAL_INDEX_COLUMN in data_thumbnail.column_names:
            data_thumbnail = data_thumbnail.sort_by(SPATIAL_INDEX_COLUMN)
        with data_thumbnail_pointer.open("wb") as f_out:
            pq.write_table(data_thumbnail, f_out)

    return total_rows


def _nonemin(value1, value2):
    """Similar to numpy's nanmin, but excludes `None` values.

    NB: If both values are `None`, this will still return `None`
    """
    if value1 is None:
        return value2
    if value2 is None:
        return value1
    return min(value1, value2)


def _nonemax(value1, value2):
    """Similar to numpy's nanmax, but excludes `None` values.

    NB: If both values are `None`, this will still return `None`
    """
    if value1 is None:
        return value2
    if value2 is None:
        return value1
    return max(value1, value2)


def _pick_columns(
    first_row_group,
    exclude_hats_columns: bool = True,
    exclude_columns: list[str] = None,
    include_columns: list[str] = None,
    only_numeric_columns: bool = False,
):
    """Convenience method to find the desired columns and their indexes, given
    some conventional user preferences.
    """

    if include_columns is None:
        include_columns = []

    if exclude_columns is None:
        exclude_columns = []
    if exclude_hats_columns:
        exclude_columns.extend(["Norder", "Dir", "Npix", "_healpix_29"])
    num_columns = first_row_group.num_columns

    column_names = [
        first_row_group.column(col).path_in_schema for col in range(0, first_row_group.num_columns)
    ]
    numeric_columns = [
        first_row_group.column(col).path_in_schema
        for col in range(0, num_columns)
        if first_row_group.column(col).physical_type in ("DOUBLE", "FLOAT", "DECIMAL")
        or "INT" in first_row_group.column(col).physical_type
    ]

    column_names = [name.removesuffix(".list.element") for name in column_names]
    numeric_columns = [name.removesuffix(".list.element") for name in numeric_columns]

    good_column_indexes = []
    for index, name in enumerate(column_names):
        base_name = name.split(".")[0]
        included = len(include_columns) == 0 or name in include_columns or base_name in include_columns
        excluded = len(exclude_columns) > 0 and (name in exclude_columns or base_name in exclude_columns)
        numeric_ok = not only_numeric_columns or name in numeric_columns
        if included and not excluded and numeric_ok:
            good_column_indexes.append(index)
    column_names = [column_names[i] for i in good_column_indexes]

    return good_column_indexes, column_names


def aggregate_column_statistics(
    metadata_file: str | Path | UPath,
    exclude_hats_columns: bool = True,
    exclude_columns: list[str] = None,
    include_columns: list[str] = None,
    only_numeric_columns: bool = False,
    include_pixels: list[HealpixPixel] = None,
):
    """Read footer statistics in parquet metadata, and report on global min/max values.

    Parameters
    ----------
    metadata_file : str | Path | UPath
        path to `_metadata` file
    exclude_hats_columns : bool
        exclude HATS spatial and partitioning fields
        from the statistics. Defaults to True.
    exclude_columns : list[str]
        additional columns to exclude from the statistics.
    include_columns : list[str]
        if specified, only return statistics for the column
        names provided. Defaults to None, and returns all non-hats columns.
    only_numeric_columns : bool
        only include columns that are numeric (integer or floating point) in the
        statistics. If True, the entire frame should be numeric.
        (Default value = False)
    include_pixels : list[HealpixPixel]
        if specified, only return statistics
        for the pixels indicated. Defaults to none, and returns all pixels.

    Returns
    -------
    pd.Dataframe
        Pandas dataframe with global summary statistics
    """
    total_metadata = file_io.read_parquet_metadata(metadata_file)
    num_row_groups = total_metadata.num_row_groups
    if num_row_groups == 0:
        return pd.DataFrame()
    first_row_group = total_metadata.row_group(0)

    good_column_indexes, column_names = _pick_columns(
        first_row_group=first_row_group,
        exclude_hats_columns=exclude_hats_columns,
        exclude_columns=exclude_columns,
        include_columns=include_columns,
        only_numeric_columns=only_numeric_columns,
    )
    if not good_column_indexes:
        return pd.DataFrame()

    extrema = None

    for row_group_index in range(0, num_row_groups):
        row_group = total_metadata.row_group(row_group_index)
        if include_pixels is not None:
            pixel = paths.get_healpix_from_path(row_group.column(0).file_path)
            if pixel not in include_pixels:
                continue
        row_stats = [
            (
                (None, None, 0, 0)
                if row_group.column(col).statistics is None
                else (
                    row_group.column(col).statistics.min,
                    row_group.column(col).statistics.max,
                    row_group.column(col).statistics.null_count,
                    row_group.column(col).num_values,
                )
            )
            for col in good_column_indexes
        ]
        if extrema is None:
            extrema = row_stats
        ## This is annoying, but avoids extra copies, or none comparison.
        else:
            extrema = [
                (
                    (_nonemin(extrema[col][0], row_stats[col][0])),
                    (_nonemax(extrema[col][1], row_stats[col][1])),
                    extrema[col][2] + row_stats[col][2],
                    extrema[col][3] + row_stats[col][3],
                )
                for col in range(0, len(good_column_indexes))
            ]

    if extrema is None:
        return pd.DataFrame()

    stats_lists = np.array(extrema).T

    frame = (
        pd.DataFrame(
            {
                "column_names": column_names,
                "min_value": stats_lists[0],
                "max_value": stats_lists[1],
                "null_count": stats_lists[2],
                "row_count": stats_lists[3],
            }
        )
        .set_index("column_names")
        .astype({"null_count": int, "row_count": int})
    )
    return frame


# pylint: disable=too-many-positional-arguments,too-many-statements
def per_pixel_statistics(
    metadata_file: str | Path | UPath,
    exclude_hats_columns: bool = True,
    exclude_columns: list[str] = None,
    include_columns: list[str] = None,
    only_numeric_columns: bool = False,
    include_stats: list[str] = None,
    multi_index: bool = False,
    include_pixels: list[HealpixPixel] = None,
    per_row_group: bool = False,
):
    """Read footer statistics in parquet metadata, and report on statistics about
    each pixel partition.

    The statistics gathered are a subset of the available attributes in the
    ``pyarrow.parquet.ColumnChunkMetaData``:

    - ``min_value`` - minimum value seen in a single data partition
    - ``max_value`` - maximum value seen in a single data partition
    - ``null_count`` - number of null values
    - ``row_count`` - total number of values. note that this will only vary by column
      if you have some nested columns in your dataset
    - ``disk_bytes`` - Compressed size of the data in the parquet file, in bytes
    - ``memory_bytes`` - Uncompressed size, in bytes

    Parameters
    ----------
    metadata_file : str | Path | UPath
        path to `_metadata` file
    exclude_hats_columns : bool
        exclude HATS spatial and partitioning fields
        from the statistics. Defaults to True.
    exclude_columns : list[str]
        additional columns to exclude from the statistics.
    include_columns : list[str]
        if specified, only return statistics for the column
        names provided. Defaults to None, and returns all non-hats columns.
    only_numeric_columns : bool
        only include columns that are numeric (integer or
        floating point) in the statistics. If True, the entire frame should be numeric.
        (Default value = False)
    include_stats : list[str]
        if specified, only return the kinds of values from list
        (min_value, max_value, null_count, row_count, disk_bytes, memory_bytes).
        Defaults to None, and returns all values.
    multi_index : bool
        should the returned frame be created with a multi-index, first on
        pixel, then on column name? Default is False, and instead indexes on pixel, with
        separate columns per-data-column and stat value combination.
        (Default value = False)
    include_pixels : list[HealpixPixel]
        if specified, only return statistics
        for the pixels indicated. Defaults to none, and returns all pixels.
    per_row_group : bool
        should the returned data be even more fine-grained and provide
        per row group (within each pixel) level statistics? Default is currently False.

    Returns
    -------
    pd.Dataframe
        Pandas dataframe with granular per-pixel statistics
    """
    total_metadata = file_io.read_parquet_metadata(metadata_file)
    num_row_groups = total_metadata.num_row_groups
    if num_row_groups == 0:
        return pd.DataFrame()
    first_row_group = total_metadata.row_group(0)

    good_column_indexes, column_names = _pick_columns(
        first_row_group=first_row_group,
        exclude_hats_columns=exclude_hats_columns,
        exclude_columns=exclude_columns,
        include_columns=include_columns,
        only_numeric_columns=only_numeric_columns,
    )
    if not good_column_indexes:
        return pd.DataFrame()

    all_stats = ["min_value", "max_value", "null_count", "row_count", "disk_bytes", "memory_bytes"]
    int_stats = ["null_count", "row_count"]

    if include_stats is None or len(include_stats) == 0:
        include_stats = all_stats
    else:
        for stat in include_stats:
            if stat not in all_stats:
                raise ValueError(f"include_stats must be from list {all_stats} (found {stat})")
        include_stats = [stat for stat in all_stats if stat in include_stats]
        int_stats = [stat for stat in int_stats if stat in include_stats]

    stat_mask = np.array([ind for ind, stat in enumerate(all_stats) if stat in include_stats])
    combined_stats = {}
    pixels = []
    leaf_stats = []

    for row_group_index in range(0, num_row_groups):
        row_group = total_metadata.row_group(row_group_index)
        pixel = paths.get_healpix_from_path(row_group.column(0).file_path)
        if include_pixels is not None and pixel not in include_pixels:
            continue
        row_stats = [
            (
                [None, None, 0, 0]
                if row_group.column(col).statistics is None
                else [
                    row_group.column(col).statistics.min,
                    row_group.column(col).statistics.max,
                    row_group.column(col).statistics.null_count,
                    row_group.column(col).num_values,
                    row_group.column(col).total_compressed_size,
                    row_group.column(col).total_uncompressed_size,
                ]
            )
            for col in good_column_indexes
        ]
        if per_row_group:
            row_stats = np.take(row_stats, stat_mask, axis=1)
            pixels.append(pixel)
            leaf_stats.append(row_stats)
        else:
            if pixel not in combined_stats:
                combined_stats[pixel] = row_stats
            else:
                current_stats = combined_stats[pixel]
                combined_stats[pixel] = [
                    (
                        _nonemin(current_stats[i][0], row_stats[i][0]),
                        _nonemax(current_stats[i][1], row_stats[i][1]),
                        current_stats[i][2] + row_stats[i][2],
                        current_stats[i][3] + row_stats[i][3],
                        current_stats[i][4] + row_stats[i][4],
                        current_stats[i][5] + row_stats[i][5],
                    )
                    for i in range(0, len(good_column_indexes))
                ]

    if per_row_group:
        stats_lists = np.array(leaf_stats)
    else:
        pixels = list(combined_stats.keys())
        stats_lists = np.array(
            [np.take(row_stats, stat_mask, axis=1) for row_stats in combined_stats.values()]
        )
    original_shape = stats_lists.shape

    if len(stats_lists) == 0:
        return pd.DataFrame()

    if multi_index:
        stats_lists = stats_lists.reshape((original_shape[0] * original_shape[1], original_shape[2]))
        frame = pd.DataFrame(
            stats_lists,
            index=pd.MultiIndex.from_product([pixels, column_names], names=["pixel", "column"]),
            columns=include_stats,
        ).astype({stat_name: int for stat_name in int_stats})
    else:
        stats_lists = stats_lists.reshape((original_shape[0], original_shape[1] * original_shape[2]))
        mod_col_names = [[f"{col_name}: {stat}" for stat in include_stats] for col_name in column_names]
        mod_col_names = np.array(mod_col_names).flatten()
        int_col_names = [[f"{col_name}: {stat}" for stat in int_stats] for col_name in column_names]
        int_col_names = np.array(int_col_names).flatten()
        frame = pd.DataFrame(stats_lists, index=pixels, columns=mod_col_names).astype(
            {stat_name: int for stat_name in int_col_names}
        )
    return frame


def pick_metadata_schema_file(catalog_base_dir: str | Path | UPath) -> UPath | None:
    """Determines the appropriate file to read for parquet metadata
    stored in the _common_metadata or _metadata files.

    Parameters
    ----------
    catalog_base_dir : str | Path | UPath
        base path for the catalog

    Returns
    -------
    UPath | None
        path to a parquet file containing metadata schema.
    """
    common_metadata_file = paths.get_common_metadata_pointer(catalog_base_dir)
    common_metadata_exists = file_io.does_file_or_directory_exist(common_metadata_file)
    metadata_file = paths.get_parquet_metadata_pointer(catalog_base_dir)
    metadata_exists = file_io.does_file_or_directory_exist(metadata_file)
    if not (common_metadata_exists or metadata_exists):
        return None
    return common_metadata_file if common_metadata_exists else metadata_file


# pylint: disable=protected-access
def nested_frame_to_vo_schema(
    nested_frame: npd.NestedFrame,
    *,
    verbose: bool = False,
    field_units: dict | None = None,
    field_ucds: dict | None = None,
    field_descriptions: dict | None = None,
    field_utypes: dict | None = None,
):
    """Create VOTableFile metadata, based on the names and types of fields in the NestedFrame.
    Add ancillary attributes to fields where they are provided in the optional dictionaries.
    Note on field names with nested columns: to include ancillary attributes (units, ucds, etc)
    for a nested sub-column, use dot notation (e.g. ``"lightcurve.band"``). You can add ancillary
    attributes for the entire nested column group using the nested column name (e.g. ``"lightcurve"``).

    Parameters
    ----------
    nested_frame : npd.NestedFrame
        nested frame representing catalog data. this can be empty, as we only need to
        know about the column names and types.
    verbose: bool
        Should we print out additional debugging statements about the vo metadata?
    field_units: dict | None
        dictionary mapping column names to astropy units (or string representation of units)
    field_ucds: dict | None
        dictionary mapping column names to UCDs (Uniform Content Descriptors)
    field_descriptions: dict | None
        dictionary mapping column names to free-text descriptions
    field_utypes: dict | None
        dictionary mapping column names to utypes

    Returns
    -------
    VOTableFile
        VO object containing all relevant metadata (but no data)
    """
    field_units = field_units or {}
    field_ucds = field_ucds or {}
    field_descriptions = field_descriptions or {}
    field_utypes = field_utypes or {}

    # Collate and tidy up the column names and data types.
    df_types = nested_frame.to_pandas().dtypes
    names = []
    data_types = []
    for col in nested_frame.base_columns:
        names.append(col)
        data_types.append(str(df_types[col]))

    for col in nested_frame.nested_columns:
        for key, val in nested_frame[col].dtype.column_dtypes.items():
            names.append(f"{col}.{key}")
            data_types.append(str(val))
    # astropy.Table uses numpy-style dtypes, and this cleans up type strings.
    data_types = ["U" if "string" in t else t.removesuffix("[pyarrow]") for t in data_types]

    # Might have extra content for nested columns.
    named_descriptions = {key: field_descriptions[key] for key in field_descriptions if key in names}
    named_units = {key: field_units[key] for key in field_units if key in names}
    if verbose:
        dropped_keys_units = set(field_units.keys()) - set(named_units.keys())
        dropped_keys_desc = set(field_descriptions.keys()) - set(named_descriptions.keys())
        if dropped_keys_units or dropped_keys_desc:
            print("================== Extra Fields ==================")
        if dropped_keys_units:
            print(f"warning - dropping some units ({len(dropped_keys_units)}):")
            print(dropped_keys_units)
        if dropped_keys_desc:
            print(f"warning - dropping some descriptions ({len(dropped_keys_desc)}):")
            print(dropped_keys_desc)

    t = Table(names=names, dtype=data_types, units=named_units, descriptions=named_descriptions)

    votablefile = VOTableFile()
    votablefile = votablefile.from_table(t)

    ## TODO - add info to root resource, e.g. obsregime.

    ## Add groups for nested columns
    vo_table = votablefile.get_first_table()
    for col in nested_frame.nested_columns:
        new_group = Group(vo_table, name=col, config=vo_table._config, pos=vo_table._pos)
        if col in field_descriptions:
            new_group.description = field_descriptions[col]
        else:
            new_group.description = "multi-column nested format"
        vo_table.groups.append(new_group)

        new_param = Param(vo_table, name="is_nested_column", datatype="boolean", value="t")
        new_group.entries.append(new_param)

        for key in nested_frame[col].columns:
            new_field = FieldRef(vo_table, ref=f"{col}.{key}")
            new_group.entries.append(new_field)

    ## Go back and add UCD/utypes to fields
    for field in vo_table.iter_fields_and_params():
        field_name = field.name
        if field_name in field_ucds:
            field.ucd = field_ucds[field_name]
        if field_name in field_utypes:
            field.utype = field_utypes[field_name]
    return votablefile


def write_voparquet_in_common_metadata(
    catalog_base_dir: str | Path | UPath,
    *,
    verbose: bool = False,
    field_units: dict | None = None,
    field_ucds: dict | None = None,
    field_descriptions: dict | None = None,
    field_utypes: dict | None = None,
):
    """Create VOTableFile metadata, based on the names and types of fields in the parquet files,
    and write to a ``catalog_base_dir/dataset/_common_metadata`` parquet file.
    Add ancillary attributes to fields where they are provided in the optional dictionaries.
    Note on field names with nested columns: to include ancillary attributes (units, ucds, etc)
    for a nested sub-column, use dot notation (e.g. ``"lightcurve.band"``). You can add ancillary
    attributes for the entire nested column group using the nested column name (e.g. ``"lightcurve"``).

    Parameters
    ----------
    catalog_base_dir : str | Path | UPath
        base path for the catalog
    verbose: bool
        Should we print out additional debugging statements about the vo metadata?
    field_units: dict | None
        dictionary mapping column names to astropy units (or string representation of units)
    field_ucds: dict | None
        dictionary mapping column names to UCDs (Uniform Content Descriptors)
    field_descriptions: dict | None
        dictionary mapping column names to free-text descriptions
    field_utypes: dict | None
        dictionary mapping column names to utypes
    """
    schema_file = pick_metadata_schema_file(catalog_base_dir=catalog_base_dir)
    if not schema_file:
        return
    nested_frame = npd.read_parquet(schema_file)
    votablefile = nested_frame_to_vo_schema(
        nested_frame=nested_frame,
        verbose=verbose,
        field_units=field_units,
        field_ucds=field_ucds,
        field_descriptions=field_descriptions,
        field_utypes=field_utypes,
    )

    xml_bstr = io.BytesIO()
    votablefile.to_xml(xml_bstr)
    xml_str = xml_bstr.getvalue().decode("utf-8")
    if verbose:
        print("================== Table XML ==================")
        print(xml_str)

    pa_schema = file_io.read_parquet_metadata(schema_file).schema.to_arrow_schema()

    original_metadata = pa_schema.metadata or {}
    updated_metadata = original_metadata | {
        b"IVOA.VOTable-Parquet.version": b"1.0",
        b"IVOA.VOTable-Parquet.content": xml_str,
    }

    pa_schema = pa_schema.with_metadata(updated_metadata)

    file_io.write_parquet_metadata(pa_schema, schema_file)
