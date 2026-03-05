def assert_catalog_info_is_correct(
    expected_catalog_info,
    catalog_info,
    *,
    do_not_compare: list[str] | None = None,
    check_extra_properties: bool = True,
    **properties_to_update,
):
    """Check that the catalog properties are similar to the expected ones."""
    if do_not_compare is None:
        do_not_compare = []
    do_not_compare.extend(["hats_creation_date", "hats_estsize"])
    do_not_compare_dict = {prop: None for prop in do_not_compare}
    expected_catalog_info = expected_catalog_info.copy_and_update(**do_not_compare_dict)
    catalog_info = catalog_info.copy_and_update(**(properties_to_update | do_not_compare_dict))
    assert expected_catalog_info.explicit_dict() == catalog_info.explicit_dict()
    if check_extra_properties:
        assert expected_catalog_info.extra_dict() == catalog_info.extra_dict()
