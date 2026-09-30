import pytest

from hats.catalog import CatalogType, ExtensionProperties


def test_read_from_file(small_sky_extension_file):
    properties = ExtensionProperties.read_from_file(small_sky_extension_file)
    assert properties.name == "small_sky_order1_errors"
    assert properties.catalog_type == CatalogType.EXTENSION
    assert properties.primary_catalog == "small_sky_o1_with_extension"
    assert properties.primary_column == "id"
    assert properties.join_catalog == "small_sky_order1_errors"
    assert properties.join_column == "object_id"
    assert properties.extension_columns == ["ra_error", "dec_error"]
    assert properties.extension_join_style == "left"
    assert properties.extension_product_type == "astrometry"


def test_round_trip(tmp_path, extension_info_data):
    properties = ExtensionProperties(**(extension_info_data | {"extension_product_type": "spectra"}))
    assert properties.catalog_type == CatalogType.EXTENSION
    properties.to_properties_file(tmp_path)

    ## The file is named after the extension
    file_path = tmp_path / "small_sky_order1_errors.properties"
    contents = file_path.read_text()
    assert contents.startswith("#HATS Extension\n")
    assert "dataproduct_type=extension" in contents
    assert "hats_ext_cols=ra_error dec_error" in contents
    assert "hats_product_type_served=spectra" in contents
    assert "hats_primary_table_url=small_sky_o1_with_extension" in contents

    assert ExtensionProperties.read_from_file(file_path) == properties


def test_list_of_columns(extension_info_data):
    properties = ExtensionProperties(
        **(extension_info_data | {"extension_columns": ["ra_error", "dec_error"]})
    )
    assert properties.extension_columns == ["ra_error", "dec_error"]
    assert ExtensionProperties(**(extension_info_data | {"extension_columns": ""})).extension_columns is None


def test_missing_required_fields(extension_info_data):
    ## Missing fields are reported under the key they have in the file
    missing_keys = {
        "name": "obs_collection",
        "primary_catalog": "hats_primary_table_url",
        "primary_column": "hats_col_assn_primary",
        "join_catalog": "hats_assn_join_table_url",
        "join_column": "hats_col_assn_join",
    }
    for missing_field, missing_key in missing_keys.items():
        props = extension_info_data.copy()
        props.pop(missing_field)
        with pytest.raises(ValueError, match=missing_key):
            ExtensionProperties(**props)


def test_optional_fields(extension_info_data):
    extension_info_data.pop("extension_columns")
    properties = ExtensionProperties(**extension_info_data)
    assert properties.extension_columns is None
    assert properties.extension_join_style is None
    assert properties.extension_product_type is None
    props = extension_info_data | {"extension_join_style": "inner"}
    assert ExtensionProperties(**props).extension_join_style == "inner"


def test_bad_join_style(extension_info_data):
    with pytest.raises(ValueError, match="extension_join_style"):
        ExtensionProperties(**(extension_info_data | {"extension_join_style": "outer"}))


def test_bad_catalog_type(extension_info_data, small_sky_dir):
    """An extension file must be of type extension, so a table's properties are not one."""
    with pytest.raises(ValueError, match="catalog_type"):
        ExtensionProperties(**(extension_info_data | {"catalog_type": "object"}))
    with pytest.raises(ValueError, match="dataproduct_type"):
        ExtensionProperties.read_from_file(small_sky_dir / "hats.properties")
