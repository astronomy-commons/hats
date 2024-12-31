from hipscat.pixel_math_ext import order2npix, generate_alignment
import numpy as np

def test_order2npix():
    assert order2npix(0) == 12
    assert order2npix(4) == 3072

def test_generate_alignment():
    assert len(generate_alignment(np.arange(12), 0, 0, 4)) == 12