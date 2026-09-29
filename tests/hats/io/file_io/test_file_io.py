import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from hats.io import paths
from hats.io.file_io import (
    delete_file,
    get_upath_for_protocol,
    load_csv_to_pandas,
    load_csv_to_pandas_generator,
    make_directory,
    read_fits_image,
    read_parquet_dataset,
    read_parquet_file_to_pandas,
    remove_directory,
    write_dataframe_to_csv,
    write_fits_image,
    write_string_to_file,
)
from hats.io.file_io.file_io import _leaf_columns, _parquet_precache_all_bytes, get_parquet_write_table_kwargs
from hats.pixel_math.spatial_index import SPATIAL_INDEX_COLUMN


def test_make_directory(tmp_path):
    test_dir_path = tmp_path / "test_path"
    assert not test_dir_path.exists()
    make_directory(test_dir_path)
    assert test_dir_path.exists()


def test_make_existing_directory_raises(tmp_path):
    test_dir_path = tmp_path / "test_path"
    make_directory(test_dir_path)
    assert test_dir_path.exists()
    with pytest.raises(OSError):
        make_directory(test_dir_path)


def test_make_existing_directory_existok(tmp_path):
    test_dir_path = tmp_path / "test_path"
    make_directory(test_dir_path)
    test_inner_dir_path = test_dir_path / "test_inner"
    make_directory(test_inner_dir_path)
    assert test_dir_path.exists()
    assert test_inner_dir_path.exists()
    make_directory(test_dir_path, exist_ok=True)
    assert test_dir_path.exists()
    assert test_inner_dir_path.exists()


def test_make_and_remove_directory(tmp_path):
    test_dir_path = tmp_path / "test_path"
    assert not test_dir_path.exists()
    make_directory(test_dir_path)
    make_directory(test_dir_path / "subdirectory")
    (test_dir_path / "subdirectory" / "file").touch()
    assert test_dir_path.exists()
    remove_directory(test_dir_path)
    assert not test_dir_path.exists()

    ## Directory no longer exists to be deleted.
    with pytest.raises(FileNotFoundError):
        remove_directory(test_dir_path)

    ## Directory doesn't exist, but shouldn't throw an error.
    remove_directory(test_dir_path, ignore_errors=True)
    assert not test_dir_path.exists()


def test_write_string_to_file(tmp_path):
    test_file_path = tmp_path / "text_file.txt"
    test_string = "this is a test"
    write_string_to_file(test_file_path, test_string, encoding="utf-8")
    with open(test_file_path, "r", encoding="utf-8") as file:
        data = file.read()
        assert data == test_string
    delete_file(test_file_path)
    assert not test_file_path.exists()


def test_load_csv_to_pandas(small_sky_source_dir):
    partition_info_path = small_sky_source_dir / "partition_info.csv"
    frame = load_csv_to_pandas(partition_info_path)
    assert len(frame) == 14


def test_load_csv_to_pandas_generator(small_sky_source_dir):
    partition_info_path = small_sky_source_dir / "partition_info.csv"
    num_reads = 0
    for frame in load_csv_to_pandas_generator(partition_info_path, chunksize=7, compression=None):
        assert len(frame) == 7
        num_reads += 1
    assert num_reads == 2


def test_load_csv_to_pandas_generator_encoding(tmp_path):
    path = tmp_path / "koi8-r.csv"
    with path.open(encoding="koi8-r", mode="w") as fh:
        fh.write("col1,col2\nыыы,яяя\n")
    num_reads = 0
    for frame in load_csv_to_pandas_generator(path, chunksize=7, encoding="koi8-r"):
        assert len(frame) == 1
        num_reads += 1
    assert num_reads == 1


def test_write_df_to_csv(tmp_path):
    random_df = pd.DataFrame(np.random.randint(0, 100, size=(100, 4)), columns=list("ABCD")).astype(int)
    test_file_path = tmp_path / "test.csv"
    write_dataframe_to_csv(random_df, test_file_path, index=False)
    loaded_df = pd.read_csv(test_file_path).astype(int)
    pd.testing.assert_frame_equal(loaded_df, random_df)


def test_read_parquet_data(tmp_path):
    random_df = pd.DataFrame(np.random.randint(0, 100, size=(100, 4)), columns=list("ABCD"))
    random_df = random_df.convert_dtypes(dtype_backend="pyarrow")
    test_file_path = tmp_path / "test.parquet"
    random_df.to_parquet(test_file_path)
    dataframe = read_parquet_file_to_pandas(test_file_path)
    pd.testing.assert_frame_equal(dataframe, random_df)

    # Show that it also works given a directory.
    test_dir = tmp_path / "test_dir"
    test_dir.mkdir()
    random_df.to_parquet(test_dir / "test.parquet")
    # Add a second file to show that they'll both be read.
    random_df2 = pd.DataFrame(np.random.randint(0, 100, size=(100, 4)), columns=list("ABCD"))
    random_df2.to_parquet(test_dir / "test2.parquet")
    random_dfs = pd.concat([random_df, random_df2]).sort_values(list("ABCD")).reset_index(drop=True)
    dataframe_from_dir = read_parquet_file_to_pandas(test_dir)
    dataframe_from_dir = dataframe_from_dir.sort_values(list("ABCD")).reset_index(drop=True)
    pd.testing.assert_frame_equal(dataframe_from_dir, random_dfs)


def test_read_parquet_dataset(small_sky_dir, small_sky_order1_dir):
    paths, ds = read_parquet_dataset(small_sky_dir / "dataset" / "Norder=0")

    assert ds.count_rows() == 131

    paths, ds = read_parquet_dataset([small_sky_dir / "dataset" / "Norder=0" / "Dir=0" / "Npix=11.parquet"])

    assert ds.count_rows() == 131
    assert len(paths) == 1

    paths, ds = read_parquet_dataset(
        [
            small_sky_order1_dir / "dataset" / "Norder=1" / "Dir=0" / "Npix=44.parquet",
            small_sky_order1_dir / "dataset" / "Norder=1" / "Dir=0" / "Npix=45.parquet",
            small_sky_order1_dir / "dataset" / "Norder=1" / "Dir=0" / "Npix=46.parquet",
            small_sky_order1_dir / "dataset" / "Norder=1" / "Dir=0" / "Npix=47.parquet",
        ]
    )
    assert ds.count_rows() == 131
    assert len(paths) == 4


def test_write_point_map_roundtrip(small_sky_order1_dir, tmp_path):
    """Test the reading/writing of a catalog point map"""
    expected_counts_skymap = read_fits_image(paths.get_point_map_file_pointer(small_sky_order1_dir))
    output_map_pointer = paths.get_point_map_file_pointer(tmp_path)
    write_fits_image(expected_counts_skymap, output_map_pointer)
    counts_skymap = read_fits_image(output_map_pointer)
    np.testing.assert_array_equal(counts_skymap, expected_counts_skymap)


def test_read_hats_with_s3():
    upath = get_upath_for_protocol("s3://bucket/catalog")
    assert upath.storage_options.get("anon")
    assert upath.storage_options["default_block_size"] == 32 * 1024


def test_read_hats_with_http():
    """Confirm that we provide additional options to the default HTTP fsspec object."""
    upath_http = get_upath_for_protocol("http://catalog")
    assert upath_http.fs.block_size == 32 * 1024
    assert upath_http.fs.client_kwargs["headers"]["User-Agent"].startswith("hats")
    assert not _parquet_precache_all_bytes(upath_http.fs)

    # Make sure we're not overriding any user-supplied User-Agent.
    upath_https = get_upath_for_protocol("https://catalog", client_kwargs={"headers": {"User-Agent": "007"}})
    assert upath_https.fs.block_size == 32 * 1024
    assert upath_https.fs.client_kwargs["headers"]["User-Agent"] == "007"
    assert not _parquet_precache_all_bytes(upath_https.fs)

    # Make sure we're not overriding any other user-supplied connection parameters.
    upath_https = get_upath_for_protocol(
        "https://catalog", client_kwargs={"headers": {"other_header": "header_value"}, "method": "POST"}
    )
    assert upath_https.fs.block_size == 32 * 1024
    assert upath_https.fs.client_kwargs["headers"]["User-Agent"].startswith("hats")
    assert upath_https.fs.client_kwargs["headers"]["other_header"] == "header_value"
    assert upath_https.fs.client_kwargs["method"] == "POST"
    assert not _parquet_precache_all_bytes(upath_https.fs)

    upath_http_full = get_upath_for_protocol("http://catalog.lsdb.org/hats/catalogs/gaia_dr3")
    assert upath_http_full.fs.block_size == 32 * 1024
    assert upath_http_full.fs.client_kwargs["headers"]["User-Agent"].startswith("hats")
    assert not _parquet_precache_all_bytes(upath_http_full.fs)

    upath_http_vizier = get_upath_for_protocol("https://vizcat.cds.unistra.fr/hats:n=1000000/gaia_dr3/")
    assert upath_http_vizier.fs.block_size == 32 * 1024
    assert upath_http_vizier.fs.client_kwargs["headers"]["User-Agent"].startswith("hats")
    assert _parquet_precache_all_bytes(upath_http_vizier)


@pytest.fixture
def nested_schema():
    return pa.schema(
        [
            pa.field(SPATIAL_INDEX_COLUMN, pa.int64()),
            pa.field("ra", pa.float64()),
            pa.field("id", pa.int64()),
            pa.field(
                "lc",
                pa.struct(
                    [
                        pa.field("flux", pa.float32()),
                        pa.field("band", pa.string()),
                    ]
                ),
            ),
            pa.field("tags", pa.list_(pa.string())),
            pa.field("props", pa.map_(pa.string(), pa.float64())),
        ]
    )


@pytest.fixture
def plain_schema():
    """Schema with no spatial index column."""
    return pa.schema([pa.field("ra", pa.float64()), pa.field("id", pa.int64())])


def test_leaf_columns_walks_struct_list_map(nested_schema):
    leaves = dict(_leaf_columns(nested_schema))
    assert leaves.keys() == {
        SPATIAL_INDEX_COLUMN,
        "ra",
        "id",
        "lc.flux",
        "lc.band",
        "tags.list.element",
        "props.key_value.key",
        "props.key_value.value",
    }


def test_get_parquet_write_table_kwargs_defaults(nested_schema):
    kwargs = get_parquet_write_table_kwargs(nested_schema)
    assert kwargs["compression"] == "zstd"
    assert kwargs["column_encoding"] == {
        SPATIAL_INDEX_COLUMN: "DELTA_BINARY_PACKED",
        "ra": "BYTE_STREAM_SPLIT",
        "lc.flux": "BYTE_STREAM_SPLIT",
        "props.key_value.value": "BYTE_STREAM_SPLIT",
    }
    assert set(kwargs["use_dictionary"]) == {"id", "lc.band", "tags.list.element", "props.key_value.key"}


def test_get_parquet_write_table_kwargs_no_spatial_index(plain_schema):
    kwargs = get_parquet_write_table_kwargs(plain_schema)
    assert kwargs["column_encoding"] == {"ra": "BYTE_STREAM_SPLIT"}
    assert kwargs["use_dictionary"] == ["id"]


def test_get_parquet_write_table_kwargs_column_encoding_override_merges(nested_schema):
    kwargs = get_parquet_write_table_kwargs(
        nested_schema, write_table_kwargs={"column_encoding": {"id": "DELTA_BINARY_PACKED"}}
    )
    assert kwargs["column_encoding"][SPATIAL_INDEX_COLUMN] == "DELTA_BINARY_PACKED"
    assert kwargs["column_encoding"]["id"] == "DELTA_BINARY_PACKED"
    assert "id" not in kwargs["use_dictionary"]


def test_get_parquet_write_table_kwargs_use_dictionary_false(nested_schema):
    kwargs = get_parquet_write_table_kwargs(nested_schema, write_table_kwargs={"use_dictionary": False})
    assert kwargs["use_dictionary"] is False
    assert kwargs["column_encoding"][SPATIAL_INDEX_COLUMN] == "DELTA_BINARY_PACKED"


def test_get_parquet_write_table_kwargs_use_dictionary_list_wins_over_encoding(nested_schema):
    # pyarrow rejects a column with both dictionary encoding and a column_encoding, so the
    # explicit use_dictionary list wins and any conflicting encoding (default or override) is dropped.
    kwargs = get_parquet_write_table_kwargs(
        nested_schema,
        write_table_kwargs={"use_dictionary": ["ra", "id"], "column_encoding": {"id": "DELTA_BINARY_PACKED"}},
    )
    assert kwargs["use_dictionary"] == ["ra", "id"]
    assert "ra" not in kwargs["column_encoding"]
    assert "id" not in kwargs["column_encoding"]


def test_get_parquet_write_table_kwargs_does_not_mutate_input(nested_schema):
    write_table_kwargs = {"column_encoding": {"id": "DELTA_BINARY_PACKED"}, "use_dictionary": ["lc.band"]}
    original = {"column_encoding": {"id": "DELTA_BINARY_PACKED"}, "use_dictionary": ["lc.band"]}
    get_parquet_write_table_kwargs(nested_schema, write_table_kwargs=write_table_kwargs)
    assert write_table_kwargs == original


@pytest.mark.parametrize(
    "write_table_kwargs",
    [
        None,
        {"use_dictionary": ["ra", "id"]},
        {"use_dictionary": ["id"], "column_encoding": {"id": "DELTA_BINARY_PACKED"}},
    ],
)
def test_get_parquet_write_table_kwargs_are_valid_for_pyarrow(tmp_path, nested_schema, write_table_kwargs):
    """The computed kwargs should never make pyarrow itself raise a conflict error."""
    table = pa.table(
        {
            SPATIAL_INDEX_COLUMN: pa.array([1, 2], type=pa.int64()),
            "ra": pa.array([1.0, 2.0], type=pa.float64()),
            "id": pa.array([1, 2], type=pa.int64()),
            "lc": pa.array(
                [{"flux": 1.0, "band": "g"}, {"flux": 2.0, "band": "r"}],
                type=nested_schema.field("lc").type,
            ),
            "tags": pa.array([["a"], ["b", "c"]], type=nested_schema.field("tags").type),
            "props": pa.array([[("a", 1.0)], [("b", 2.0)]], type=nested_schema.field("props").type),
        },
        schema=nested_schema,
    )
    kwargs = get_parquet_write_table_kwargs(nested_schema, write_table_kwargs=write_table_kwargs)
    pq.write_table(table, tmp_path / "test.parquet", **kwargs)
