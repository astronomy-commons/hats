from importlib.metadata import version

import pytest

from hats.catalog.dataset.table_properties import TableProperties
from hats.io.file_io.file_io import load_text_file


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
        skymap_order=7,
        skymap_alt_orders="2 3 4",
        indexing_column="a",
        primary_catalog="bar",
        hats_copyright="LINCC Frameworks 2024",
    )
    assert table_properties.extra_columns == ["a", "b"]

    # hats_copyright is not part of the named args, so it shouldn't show up in the debug string
    expected_str = """catalog_name      foo
catalog_type      index
total_rows        15
primary_catalog   bar
indexing_column   a
extra_columns     a b
npix_suffix       .parquet
skymap_order      7
skymap_alt_orders 2 3 4
"""
    assert str(table_properties) == expected_str
    assert repr(table_properties) == expected_str

    table_properties_using_list = TableProperties(
        catalog_name="foo",
        catalog_type="index",
        total_rows=15,
        extra_columns=["a", "b"],
        skymap_order=7,
        skymap_alt_orders=[2, 3, 4],
        indexing_column="a",
        primary_catalog="bar",
        hats_copyright="LINCC Frameworks 2024",
    )
    assert table_properties_using_list == table_properties


def test_properties_int_list_parsing():
    results = TableProperties.space_delimited_int_list("0 2 5")
    assert results == [0, 2, 5]

    results = TableProperties.space_delimited_int_list("0 5 2 5")
    assert results == [0, 2, 5]

    results = TableProperties.space_delimited_int_list([0, 2, 5])
    assert results == [0, 2, 5]

    results = TableProperties.space_delimited_int_list([0, 5, 2, 5])
    assert results == [0, 2, 5]

    results = TableProperties.space_delimited_int_list(" 2 ")
    assert results == [2]

    results = TableProperties.space_delimited_int_list(2)
    assert results == [2]

    results = TableProperties.space_delimited_int_list("      ")
    assert results is None

    results = TableProperties.space_delimited_int_list("")
    assert results is None

    results = TableProperties.space_delimited_int_list(None)
    assert results is None

    with pytest.raises(ValueError, match="invalid literal"):
        TableProperties.space_delimited_int_list("one two five")

    with pytest.raises(ValueError, match="Unsupported type"):
        TableProperties.space_delimited_int_list(["0 2 5"])


def test_properties_allowed_required():
    # Missing required field indexing_column
    with pytest.raises(ValueError, match="indexing_column"):
        TableProperties(
            catalog_name="foo",
            catalog_type="index",
            total_rows=15,
            primary_catalog="bar",
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


def test_read_from_dir_branches(
    small_sky_dir,
    small_sky_order1_dir,
    small_sky_npix_alt_suffix_dir,
    small_sky_npix_as_dir_dir,
    association_catalog_path,
    small_sky_source_object_index_dir,
    margin_catalog_path,
    small_sky_source_dir,
):
    TableProperties.read_from_dir(small_sky_dir)
    TableProperties.read_from_dir(small_sky_npix_alt_suffix_dir)
    TableProperties.read_from_dir(small_sky_npix_as_dir_dir)
    TableProperties.read_from_dir(small_sky_order1_dir)
    TableProperties.read_from_dir(association_catalog_path)
    TableProperties.read_from_dir(small_sky_source_object_index_dir)
    TableProperties.read_from_dir(margin_catalog_path)
    TableProperties.read_from_dir(small_sky_source_dir)


def test_extra_dict():
    extra_properties = {
        "hats_copyright": "LINCC Frameworks 2024",
        "hats_estsize": 10000000,
        "hats_max_rows": 1000000,
    }
    table_properties = TableProperties(
        catalog_name="foo",
        catalog_type="index",
        total_rows=15,
        extra_columns="a , b",
        indexing_column="a",
        primary_catalog="bar",
        **extra_properties,
    )

    assert table_properties.extra_dict() == extra_properties


def test_provenance_dict(small_sky_dir, tmp_path):
    properties = TableProperties.new_provenance_dict(small_sky_dir)
    assert list(properties.keys()) == [
        "hats_builder",
        "hats_creation_date",
        "hats_estsize",
        "hats_release_date",
        "hats_version",
    ]
    # Most values are dynamic, but these are some safe assumptions.
    assert properties["hats_builder"] == f"hats v{version('hats')}"
    assert properties["hats_creation_date"].startswith("20")
    assert properties["hats_estsize"] >= 0
    assert properties["hats_release_date"].startswith("20")
    assert properties["hats_version"].startswith("v")

    properties_object = TableProperties(
        catalog_name="foo",
        catalog_type="index",
        total_rows=15,
        indexing_column="a",
        primary_catalog="bar",
        **properties,
    )

    properties_object.to_properties_file(tmp_path)

    contents = ",".join(load_text_file(tmp_path / "hats.properties"))
    assert "\\" not in contents

    # Test that we can override a property and add another.
    properties = TableProperties.new_provenance_dict(
        small_sky_dir, builder="lsdb v0.1", hats_estsize=1000, foo="bar"
    )
    assert properties["hats_builder"] == f"lsdb v0.1, hats v{version('hats')}"
    assert properties["hats_creation_date"].startswith("20")
    assert properties["hats_estsize"] == 1000
    assert properties["hats_release_date"].startswith("20")
    assert properties["hats_version"].startswith("v")
    assert properties["foo"] == "bar"
