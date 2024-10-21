import dataclasses
import os

import pytest

from hats.inspection.almanac import Almanac
from hats.inspection.almanac_info import AlmanacInfo


def test_from_catalog_dir(small_sky_dir):
    """Load from a directory."""
    almanac_info = AlmanacInfo.from_catalog_dir(small_sky_dir)
    assert almanac_info.catalog_name == "small_sky"


def test_from_bad_file(association_catalog_partition_join_file):
    """Test failures when loading almanac from invalid files."""
    with pytest.raises(ValueError, match="csv"):
        AlmanacInfo.from_file(association_catalog_partition_join_file)


def test_write_to_file(tmp_path, association_catalog_path):
    """Write out the almanac to file and make sure we can read it again."""
    almanac_info = AlmanacInfo.from_catalog_dir(association_catalog_path)
    assert almanac_info.catalog_name == "small_sky_to_small_sky_order1"
    almanac_info.version = "v0.0.1"
    almanac_info.deprecated = "yes - use something else"
    almanac_info.creators.append("me")

    almanac_info.write_to_file(tmp_path, default_dir=False)

    new_info = AlmanacInfo.from_file(tmp_path / "small_sky_to_small_sky_order1.yml")

    assert new_info.catalog_name == almanac_info.catalog_name

    almanac_info_dict = dataclasses.asdict(almanac_info)
    new_info_dict = dataclasses.asdict(new_info)
    for key, value in new_info_dict.items():
        assert almanac_info_dict[key] == value


def test_write_to_file_load_almanac(tmp_path, small_sky_dir):
    """Write out the almanac to file and make sure we can read it again."""
    almanac_info = AlmanacInfo.from_catalog_dir(small_sky_dir)
    assert almanac_info.catalog_name == "small_sky"

    almanac_info.write_to_file(tmp_path, default_dir=False)

    alms = Almanac(include_default_dir=False, dirs=tmp_path)
    assert len(alms.catalogs()) == 1


def test_write_to_bad_file(tmp_path, small_sky_dir):
    """Test failure conditions when writing almanac."""
    os.environ["HATS_ALMANAC_DIR"] = str(tmp_path)
    almanac_info = AlmanacInfo.from_catalog_dir(small_sky_dir)

    with pytest.raises(ValueError, match="only one"):
        almanac_info.write_to_file(tmp_path, default_dir=True)

    with pytest.raises(ValueError, match="foo"):
        almanac_info.write_to_file(fmt="foo")

    almanac_info.write_to_file(default_dir=True)
    with pytest.raises(ValueError, match="already exists"):
        almanac_info.write_to_file(default_dir=True)


def test_association_fields(association_catalog_path, test_data_dir, small_sky_dir):
    """Test additional text fields tables with primary/join relationships."""
    almanac_info = AlmanacInfo.from_catalog_dir(association_catalog_path)
    assert almanac_info.catalog_name == "small_sky_to_small_sky_order1"
    assert almanac_info.primary == "small_sky"
    assert almanac_info.join == "small_sky_order1"

    almanac_info = AlmanacInfo.from_catalog_dir(test_data_dir / "info_only" / "index_catalog")
    assert almanac_info.primary == "catalog"
    assert almanac_info.join is None

    almanac_info = AlmanacInfo.from_catalog_dir(small_sky_dir)
    assert almanac_info.primary is None
    assert almanac_info.join is None


## Commented out -
## As new test catalogs are added, you can use this method to quickly add
## their almanac to the default almanac directory
# def test_add_almanac(source_catalog_info_file):
#     """Write out the almanac to file and make sure we can read it again."""
#     almanac_info = AlmanacInfo.from_catalog_dir(source_catalog_info_file)
#     almanac_info.write_to_file()
