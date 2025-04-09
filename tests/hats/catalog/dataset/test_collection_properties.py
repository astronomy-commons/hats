from hats.catalog.dataset.collection_properties import CollectionProperties


def test_read_collection_from_file_round_trip(small_sky_collection_dir, tmp_path):
    table_properties = CollectionProperties.read_from_dir(small_sky_collection_dir)
    table_properties.to_properties_file(tmp_path)
    print("table_properties", table_properties)
    round_trip_properties = CollectionProperties.read_from_dir(tmp_path)

    assert table_properties == round_trip_properties

    kwarg_properties = CollectionProperties(
        **round_trip_properties.model_dump(by_alias=False, exclude_none=True)
    )
    assert table_properties == kwarg_properties
