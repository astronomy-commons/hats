from hats.io.file_io import get_upath_for_protocol
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


def test_read_hats_initializes_upath_once(small_sky_dir, mocker):
    mock_method = "hats.io.file_io.file_pointer.get_upath_for_protocol"
    # Setting the side effect allows us to run the mocked function's code
    mocked_upath_call = mocker.patch(mock_method, side_effect=get_upath_for_protocol)
    read_hats(small_sky_dir)
    # The construction of the UPath is called once, at the start of `read_hats`
    mocked_upath_call.assert_called_once_with(small_sky_dir)


def test_read_hats_with_s3_anonymous_access():
    upath = get_upath_for_protocol("s3://bucket/catalog")
    assert upath.storage_options.get("anon")


def test_read_hats_nonstandard_npix_suffix(
    small_sky_npix_alt_suffix_dir,
    small_sky_npix_as_dir_dir,
):
    read_hats(small_sky_npix_alt_suffix_dir)
    read_hats(small_sky_npix_as_dir_dir)
