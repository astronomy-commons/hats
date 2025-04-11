from hats.catalog.dataset.collection_properties import CollectionProperties


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


def test_read_collection_from_file_round_trip(small_sky_collection_dir, tmp_path):
    table_properties = CollectionProperties.read_from_dir(small_sky_collection_dir)
    table_properties.to_properties_file(tmp_path)
    round_trip_properties = CollectionProperties.read_from_dir(tmp_path)

    assert table_properties == round_trip_properties

    kwarg_properties = CollectionProperties(
        **round_trip_properties.model_dump(by_alias=False, exclude_none=True)
    )
    assert table_properties == kwarg_properties
