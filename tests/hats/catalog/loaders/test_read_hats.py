from hats.loaders import read_hats


def test_read_hats_branches(
    small_sky_dir,
    small_sky_order1_dir,
    association_catalog_path,
    small_sky_source_object_index_dir,
    margin_catalog_path,
    small_sky_source_dir,
    test_data_dir,
):
    read_hats(small_sky_dir)
    read_hats(small_sky_order1_dir)
    read_hats(association_catalog_path)
    read_hats(small_sky_source_object_index_dir)
    read_hats(margin_catalog_path)
    read_hats(small_sky_source_dir)
    read_hats(test_data_dir / "square_map")
