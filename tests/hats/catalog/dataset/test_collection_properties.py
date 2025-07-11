import pytest

from hats.catalog.dataset.collection_properties import CollectionProperties
from hats.io.file_io import write_string_to_file
from hats.io.file_io.file_io import load_text_file


def test_read_collection_from_file(small_sky_collection_dir):
    properties_from_file = CollectionProperties.read_from_dir(small_sky_collection_dir).model_dump(
        by_alias=False, exclude_none=True
    )

    expected_attributes = {
        "name": "small_sky_o1_collection",
        "hats_primary_table_url": "small_sky_order1",
        "all_margins": "small_sky_order1_margin",
        "default_margin": "small_sky_order1_margin",
        "all_indexes": "id small_sky_order1_id_index",
        "default_index": "id",
        "obs_regime": "Optical",
    }

    for key, val in expected_attributes.items():
        assert properties_from_file[key] == val


def test_collection_properties_string():
    expected_properties = CollectionProperties(
        name="small_sky_01",
        hats_primary_table_url="small_sky_order1",
        obs_regime="Optical",
    )

    ## str representation should not include additional properties.
    assert (
        str(expected_properties)
        == """  name small_sky_01
  hats_primary_table_url small_sky_order1
"""
    )


def test_read_collection_list_parse(tmp_path):
    test_file_path = tmp_path / "collection.properties"

    file_content = """  name= foo
  hats_primary_table_url=small_sky_order1
  all_margins=small_sky_order1_margin small_sky_order1_margin_10arcs
  all_indexes=
"""
    write_string_to_file(test_file_path, file_content, encoding="utf-8")

    parsed_properties = CollectionProperties.read_from_dir(tmp_path)
    assert parsed_properties.all_margins == ["small_sky_order1_margin", "small_sky_order1_margin_10arcs"]
    assert parsed_properties.all_indexes is None

    file_content = """  name= foo
  hats_primary_table_url=small_sky_order1
  all_margins=
  all_indexes=id small_sky_order1_id_index
"""
    write_string_to_file(test_file_path, file_content, encoding="utf-8")

    parsed_properties = CollectionProperties.read_from_dir(tmp_path)
    assert parsed_properties.all_margins is None
    assert parsed_properties.all_indexes == {"id": "small_sky_order1_id_index"}

    file_content = """  name= foo
  hats_primary_table_url=small_sky_order1
  all_margins=
  all_indexes=
"""
    write_string_to_file(test_file_path, file_content, encoding="utf-8")

    parsed_properties = CollectionProperties.read_from_dir(tmp_path)

    parsed_properties.to_properties_file(tmp_path)

    contents = load_text_file(test_file_path)
    assert contents == [
        "#HATS Collection\n",
        "obs_collection=foo\n",
        "hats_primary_table_url=small_sky_order1\n",
    ]


def test_write_properties_colon_not_escaped(tmp_path):
    test_timestamp_value = "2025-06-30T17:10UTC"

    collection_props = CollectionProperties(
        name="test_collection_with_colon",
        hats_primary_table_url="test_url_with_colon",
        hats_creation_date=test_timestamp_value,
    )

    collection_props.to_properties_file(tmp_path)

    output_file_path = tmp_path / "collection.properties"

    file_contents = load_text_file(output_file_path)

    expected_line_unescaped = f"hats_creation_date={test_timestamp_value}\n"
    assert expected_line_unescaped in file_contents

    escaped_timestamp_value = test_timestamp_value.replace(":", r"\:")
    unexpected_line_escaped = f"hats_creation_date={escaped_timestamp_value}\n"
    assert unexpected_line_escaped not in file_contents


def test_read_collection_read_errors(tmp_path):
    test_file_path = tmp_path / "collection.properties"

    with pytest.raises(FileNotFoundError):
        CollectionProperties.read_from_dir(tmp_path)

    ## Very basic content - should work.
    file_content = """  name= foo
  hats_primary_table_url=small_sky_order1
"""
    write_string_to_file(test_file_path, file_content, encoding="utf-8")

    _ = CollectionProperties.read_from_dir(tmp_path)

    ## Unsupported extra property.
    file_content = """  name= foo
  hats_primary_table_url=small_sky_order1
  hats_col_ra=ra
"""
    write_string_to_file(test_file_path, file_content, encoding="utf-8")

    with pytest.raises(ValueError, match="hats_col_ra"):
        CollectionProperties.read_from_dir(tmp_path)

    ## Unmatched index map.
    file_content = """  name= foo
  hats_primary_table_url=small_sky_order1
  all_indexes=ra
"""
    write_string_to_file(test_file_path, file_content, encoding="utf-8")

    with pytest.raises(ValueError, match="all_indexes"):
        CollectionProperties.read_from_dir(tmp_path)


def test_read_collection_from_file_round_trip(small_sky_collection_dir, tmp_path):
    table_properties = CollectionProperties.read_from_dir(small_sky_collection_dir)
    table_properties.to_properties_file(tmp_path)
    round_trip_properties = CollectionProperties.read_from_dir(tmp_path)

    assert table_properties == round_trip_properties

    kwarg_properties = CollectionProperties(
        **round_trip_properties.model_dump(by_alias=False, exclude_none=True)
    )
    assert table_properties == kwarg_properties


def test_read_collection_default_margin_not_found(small_sky_collection_dir, tmp_path):
    table_properties = CollectionProperties.read_from_dir(small_sky_collection_dir)
    assert table_properties.all_margins == ["small_sky_order1_margin"]
    table_properties.default_margin = "small_sky_order1_margin_1deg"
    table_properties.to_properties_file(tmp_path)
    with pytest.raises(ValueError, match="not found in all_margins"):
        CollectionProperties.read_from_dir(tmp_path)


def test_read_collection_all_margins_not_specified(small_sky_collection_dir, tmp_path):
    table_properties = CollectionProperties.read_from_dir(small_sky_collection_dir)
    assert table_properties.default_margin == "small_sky_order1_margin"
    table_properties.all_margins = []
    table_properties.to_properties_file(tmp_path)
    with pytest.raises(ValueError, match="all_margins needs to be set"):
        CollectionProperties.read_from_dir(tmp_path)


def test_read_collection_default_index_not_found(small_sky_collection_dir, tmp_path):
    table_properties = CollectionProperties.read_from_dir(small_sky_collection_dir)
    assert table_properties.all_indexes == {"id": "small_sky_order1_id_index"}
    table_properties.default_index = "obj_name"
    table_properties.to_properties_file(tmp_path)
    with pytest.raises(ValueError, match="not found in all_indexes"):
        CollectionProperties.read_from_dir(tmp_path)


def test_read_collection_all_indexes_not_specified(small_sky_collection_dir, tmp_path):
    table_properties = CollectionProperties.read_from_dir(small_sky_collection_dir)
    assert table_properties.default_index == "id"
    table_properties.all_indexes = []
    table_properties.to_properties_file(tmp_path)
    with pytest.raises(ValueError, match="all_indexes needs to be set"):
        CollectionProperties.read_from_dir(tmp_path)


def test_collection_parsing():
    ## Confirm we can pass in already dict- or list-like objects, and get the expected values.
    ## Space-only delimited
    results = CollectionProperties.space_delimited_list(
        "small_sky_order1_margin small_sky_order1_50arcs_margin"
    )
    assert results == ["small_sky_order1_margin", "small_sky_order1_50arcs_margin"]

    ## Comma-delimited
    results = CollectionProperties.space_delimited_list(
        "small_sky_order1_margin, small_sky_order1_50arcs_margin"
    )
    assert results == ["small_sky_order1_margin", "small_sky_order1_50arcs_margin"]

    ## Already a list of strings
    results = CollectionProperties.space_delimited_list(
        ["small_sky_order1_margin", "small_sky_order1_50arcs_margin"]
    )
    assert results == ["small_sky_order1_margin", "small_sky_order1_50arcs_margin"]

    ## None or empty
    results = CollectionProperties.space_delimited_list(None)
    assert results is None

    results = CollectionProperties.space_delimited_list("")
    assert results is None

    results = CollectionProperties.index_tuples("id small_sky_order1_id_index")
    assert results == {"id": "small_sky_order1_id_index"}

    ## Comma-delimited
    results = CollectionProperties.index_tuples("id small_sky_order1_id_index, ra small_sky_ra_index")
    assert results == {"id": "small_sky_order1_id_index", "ra": "small_sky_ra_index"}

    ## Already a dict of strings
    results = CollectionProperties.index_tuples(
        {"id": "small_sky_order1_id_index", "ra": "small_sky_ra_index"}
    )
    assert results == {"id": "small_sky_order1_id_index", "ra": "small_sky_ra_index"}

    ## None or empty
    results = CollectionProperties.index_tuples(None)
    assert results is None

    results = CollectionProperties.index_tuples("")
    assert results is None

    simple_properties = CollectionProperties(
        name="small_sky_01",
        hats_primary_table_url="small_sky_order1",
    )

    simple_properties_with_none = CollectionProperties(
        name="small_sky_01",
        hats_primary_table_url="small_sky_order1",
        all_margins=None,
        default_margin=None,
        all_indexes=None,
        default_index=None,
    )

    assert simple_properties == simple_properties_with_none
