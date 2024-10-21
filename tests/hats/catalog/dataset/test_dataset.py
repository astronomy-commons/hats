import os

import pytest

from hats.catalog.dataset.dataset import Dataset


def test_read_hats(dataset_path):
    dataset = Dataset.read_hats(dataset_path)
    assert dataset.on_disk
    assert str(dataset.catalog_path) == str(dataset_path)
    assert str(dataset.catalog_base_dir) == str(dataset_path)


def test_read_from_missing_folder(tmp_path):
    wrong_path = tmp_path / "wrong"
    with pytest.raises(FileNotFoundError, match="directory"):
        Dataset.read_hats(wrong_path)


def test_read_from_empty_folder(tmp_path):
    dataset_path = tmp_path / "dat"
    os.makedirs(dataset_path)
    with pytest.raises(FileNotFoundError, match="properties file"):
        Dataset.read_hats(dataset_path)
