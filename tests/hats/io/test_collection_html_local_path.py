import shutil

from hats.io.summary_file import write_catalog_summary_file


def test_collection_html_uses_local_relative_path(tmp_path, small_sky_collection_dir):
    collection_dir = tmp_path / "collection"
    shutil.copytree(small_sky_collection_dir, collection_dir)

    output_path = write_catalog_summary_file(collection_dir, fmt="html")

    content = output_path.read_text()
    assert 'lsdb.open_catalog(".")' in content
    assert 'href="./collection.properties"' in content
