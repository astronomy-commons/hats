from pathlib import Path

from hats.io.file_io import (
    append_paths_to_pointer,
    directory_has_contents,
    does_file_or_directory_exist,
    find_files_matching_path,
    get_directory_contents,
    is_regular_file,
)


def test_file_or_dir_exist(small_sky_dir):
    assert does_file_or_directory_exist(small_sky_dir)

    catalog_info_string = small_sky_dir / "properties"
    assert does_file_or_directory_exist(catalog_info_string)


def test_file_or_dir_exist_false(small_sky_dir):
    assert not does_file_or_directory_exist(str(small_sky_dir) + "incorrect file")


def test_append_paths_to_pointer(tmp_path):
    test_paths = ["folder", "file.txt"]
    test_path = tmp_path / "folder" / "file.txt"
    assert str(append_paths_to_pointer(tmp_path, *test_paths)) == str(test_path)


def test_is_regular_file(small_sky_dir):
    properties_file = small_sky_dir / "properties"
    assert is_regular_file(properties_file)

    assert not is_regular_file(small_sky_dir)

    partition_dir = small_sky_dir / "Norder=0"
    assert not is_regular_file(partition_dir)


def test_find_files_matching_path(small_sky_dir):
    ## no_wildcard
    matching_files = find_files_matching_path(small_sky_dir, "properties")
    assert len(matching_files) == 1

    ## wilcard in the name (matches _metadata and _common_metadata)
    assert len(find_files_matching_path(small_sky_dir, "*metadata*")) == 2


def test_find_files_matching_path_directory(small_sky_order1_dir):
    assert len(find_files_matching_path(small_sky_order1_dir)) == 1

    ## wildcard in directory - will match all files at indicated depth
    assert len(find_files_matching_path(small_sky_order1_dir, "*", "*", "*", "*")) == 4


def test_directory_has_contents(small_sky_order1_dir, tmp_path):
    assert directory_has_contents(small_sky_order1_dir)
    assert not directory_has_contents(tmp_path)


def test_get_directory_contents(small_sky_order1_dir, tmp_path):
    small_sky_contents = get_directory_contents(small_sky_order1_dir)

    small_sky_paths = [Path(p) for p in small_sky_contents]

    expected = [
        "dataset",
        "partition_info.csv",
        "point_map.fits",
        "properties",
    ]

    expected = [small_sky_order1_dir / file_name for file_name in expected]

    assert small_sky_paths == expected

    assert len(get_directory_contents(tmp_path)) == 0
