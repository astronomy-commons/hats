from hats import read_hats
from hats.catalog import CatalogCollection, CatalogType, ExtensionCatalog, TableProperties


def test_read_extension(small_sky_order1_catalog, small_sky_extension_dir):
    ## The extension has margins, so it is a collection holding the extension catalog.
    extension = read_hats(small_sky_extension_dir).main_catalog
    assert isinstance(extension, ExtensionCatalog)
    assert isinstance(extension.catalog_info, TableProperties)

    assert extension.catalog_info.catalog_type == CatalogType.EXTENSION
    assert extension.catalog_info.total_rows == 131
    assert extension.catalog_info.primary_catalog == "small_sky_o1_with_extension/small_sky_order1"
    assert extension.catalog_info.primary_column == "id"
    assert extension.catalog_info.join_column == "object_id"
    assert extension.catalog_info.extension_columns == ["ra_error", "dec_error"]
    assert extension.catalog_info.extension_join_style == "left"
    assert extension.schema.names == ["_healpix_29", "object_id", "ra", "dec", "ra_error", "dec_error"]

    # Extensions carry coordinates, so they can be used on their own
    assert extension.catalog_info.ra_column == "ra"
    assert extension.catalog_info.dec_column == "dec"

    # The extension shares the partitioning with the core
    assert extension.get_healpix_pixels() == small_sky_order1_catalog.get_healpix_pixels()


def test_read_extension_collection(small_sky_o1_with_extension_dir):
    """The collection lists its extension, which is a collection of its own when it has margins."""
    collection = read_hats(small_sky_o1_with_extension_dir)
    assert isinstance(collection, CatalogCollection)
    assert collection.all_extensions == ["small_sky_order1_errors"]
    assert collection.collection_properties.hats_primary_table_url == "small_sky_order1"

    extension_collection = read_hats(small_sky_o1_with_extension_dir / "small_sky_order1_errors")
    assert isinstance(extension_collection.main_catalog, ExtensionCatalog)
    assert extension_collection.all_margins == [
        "small_sky_order1_errors_margin",
        "small_sky_order1_errors_margin_10arcs",
    ]
