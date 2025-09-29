import hats


def test_version():
    """Check to see that we can get the package version"""
    assert hats.__version__ is not None
