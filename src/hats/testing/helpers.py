# pylint: disable=import-outside-toplevel,import-error
def _dict_almost_equal(dict_expected, dict_test):  # pragma: no cover
    try:
        import pytest
    except ImportError as exc:
        raise ImportError("pytest is required to use this method. Install with pip or conda.") from exc

    assert dict_expected.keys() == dict_test.keys()
    for key in dict_expected:
        assert dict_expected[key] == pytest.approx(
            dict_test[key], rel=1e-4
        ), f"Failed approx comparison for {key} (expected: {dict_expected[key]}, test: {dict_test[key]})"


def assert_catalog_info_is_correct(
    expected_catalog_info,
    catalog_info,
    *,
    do_not_compare: list[str] | None = None,
    check_extra_properties: bool = True,
    **properties_to_update,
):  # pragma: no cover
    """Check that the catalog properties are similar to the expected ones."""
    if do_not_compare is None:
        do_not_compare = []
    do_not_compare.extend(["hats_creation_date", "hats_estsize"])
    do_not_compare_dict = {prop: None for prop in do_not_compare}
    expected_catalog_info = expected_catalog_info.copy_and_update(**do_not_compare_dict)
    catalog_info = catalog_info.copy_and_update(**(properties_to_update | do_not_compare_dict))
    _dict_almost_equal(expected_catalog_info.explicit_dict(), catalog_info.explicit_dict())
    if check_extra_properties:
        _dict_almost_equal(expected_catalog_info.extra_dict(), catalog_info.extra_dict())
