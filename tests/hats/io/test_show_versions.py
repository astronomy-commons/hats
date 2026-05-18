import hats


def test_show_versions(capsys):
    hats.show_versions()
    captured = capsys.readouterr().out
    assert captured.startswith("\n--------      SYSTEM INFO      --------")
    assert "hats" in captured
    assert "nested-pandas" in captured
    assert "pyarrow" in captured
    assert "fsspec" in captured
