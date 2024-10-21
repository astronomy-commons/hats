import pytest

from hats.catalog.dataset.table_properties import TableProperties


@pytest.mark.parametrize("data_dir", ["catalog", "dataset", "index_catalog", "margin_cache"])
def test_read_from_file_round_trip(test_data_dir, data_dir, tmp_path):
    dataset_path = test_data_dir / "info_only" / data_dir

    table_properties = TableProperties.read_from_dir(dataset_path)
    table_properties.to_properties_file(tmp_path)
    round_trip_properties = TableProperties.read_from_dir(tmp_path)

    assert table_properties == round_trip_properties

    kwarg_properties = TableProperties(**round_trip_properties.model_dump(by_alias=False, exclude_none=True))
    assert table_properties == kwarg_properties


def test_properties_parsing():
    table_properties = TableProperties(
        catalog_name="foo",
        catalog_type="index",
        total_rows=15,
        extra_columns="a , b",
        indexing_column="a",
        primary_catalog="bar",
        hats_copyright="LINCC Frameworks 2024",
    )
    assert table_properties.extra_columns == ["a", "b"]

    # hats_copyright is not part of the named args, so it shouldn't show up in the debug string
    assert (
        str(table_properties)
        == """  catalog_name foo
  catalog_type index
  total_rows 15
  primary_catalog bar
  indexing_column a
  extra_columns a b
"""
    )
    table_properties_using_list = TableProperties(
        catalog_name="foo",
        catalog_type="index",
        total_rows=15,
        extra_columns=["a", "b"],
        indexing_column="a",
        primary_catalog="bar",
        hats_copyright="LINCC Frameworks 2024",
    )
    assert table_properties_using_list == table_properties


def test_properties_allowed_required():
    # Missing required field indexing_column
    with pytest.raises(ValueError, match="indexing_column"):
        TableProperties(
            catalog_name="foo",
            catalog_type="index",
            total_rows=15,
            primary_catalog="bar",
        )

    # join_column is only allowed on association catalogs
    with pytest.raises(ValueError, match="join_column"):
        TableProperties(
            catalog_name="foo",
            catalog_type="index",
            total_rows=15,
            indexing_column="a",
            primary_catalog="bar",
            join_column="b",
        )

    # extra_columnsss is a typo
    with pytest.raises(ValueError, match="extra_columnsss"):
        TableProperties(
            catalog_name="foo",
            catalog_type="index",
            total_rows=15,
            indexing_column="a",
            primary_catalog="bar",
            extra_columnsss=["beep"],
        )


def test_copy_and_update():
    initital_properties = TableProperties(
        catalog_name="foo",
        catalog_type="index",
        total_rows=15,
        indexing_column="a",
        primary_catalog="bar",
    )
    prop_a = initital_properties.copy_and_update()
    assert initital_properties == prop_a

    prop_b = initital_properties.copy_and_update(catalog_name="bar")
    assert initital_properties != prop_b
    assert prop_b.catalog_name == "bar"

    prop_d = initital_properties.copy_and_update(**{"catalog_name": "bar"})
    assert initital_properties != prop_d
    assert prop_d.catalog_name == "bar"
    assert prop_b == prop_d

    prop_c = initital_properties.copy_and_update(moc_sky_fraction=0.54)
    assert initital_properties != prop_c
    assert prop_c.__pydantic_extra__["moc_sky_fraction"] == pytest.approx(0.54)

    # extra_columnsss is a typo
    with pytest.raises(ValueError, match="extra_columnsss"):
        initital_properties.copy_and_update(extra_columnsss=0.54)

    with pytest.raises(ValueError, match="extra_columnsss"):
        initital_properties.copy_and_update(**{"extra_columnsss": 0.54})


def test_read_from_dir_branches(
    small_sky_dir,
    small_sky_order1_dir,
    association_catalog_path,
    small_sky_source_object_index_dir,
    margin_catalog_path,
    small_sky_source_dir,
):
    TableProperties.read_from_dir(small_sky_dir)
    TableProperties.read_from_dir(small_sky_order1_dir)
    TableProperties.read_from_dir(association_catalog_path)
    TableProperties.read_from_dir(small_sky_source_object_index_dir)
    TableProperties.read_from_dir(margin_catalog_path)
    TableProperties.read_from_dir(small_sky_source_dir)
