import hats


def test_hats_version():
    """Check to see that we can get the HATS version"""
    assert hats.__version__ is not None
