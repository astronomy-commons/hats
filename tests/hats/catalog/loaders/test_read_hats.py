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
    mocked_upath_call = mocker.patch(mock_method, side_effect=get_upath_for_protocol)
    read_hats(small_sky_dir)
    mocked_upath_call.assert_called_once_with(small_sky_dir)
