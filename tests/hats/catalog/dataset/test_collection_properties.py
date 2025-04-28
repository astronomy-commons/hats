import pytest

from hats.catalog.dataset.collection_properties import CollectionProperties
from hats.io.file_io import write_string_to_file
from hats.io.file_io.file_io import load_text_file


def test_read_collection_from_file(small_sky_collection_dir):
    properties_from_file = CollectionProperties.read_from_dir(small_sky_collection_dir)

    expected_properties = CollectionProperties(
        name="small_sky_01",
        hats_primary_table_url="small_sky_order1",
        all_margins="small_sky_order1_margin",
        default_margin="small_sky_order1_margin",
        all_indexes="id small_sky_order1_id_index",
        default_index="id",
        obs_regime="Optical",
    )

    assert properties_from_file == expected_properties


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


def test_collection_parsing(small_sky_collection_dir):
    ## Confirm we can pass in already dict- or list-like objects, and get the expected values.
    properties_from_file = CollectionProperties.read_from_dir(small_sky_collection_dir)

    expected_properties = CollectionProperties(
        name="small_sky_01",
        hats_primary_table_url="small_sky_order1",
        all_margins=["small_sky_order1_margin"],
        default_margin="small_sky_order1_margin",
        all_indexes={"id": "small_sky_order1_id_index"},
        default_index="id",
        obs_regime="Optical",
    )

    assert properties_from_file == expected_properties

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
