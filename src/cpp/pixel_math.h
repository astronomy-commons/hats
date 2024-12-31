#ifndef PIXEL_MATH_H_
#define  PIXEL_MATH_H_

#include <pybind11/pybind11.h>

#include <vector>

namespace py = pybind11;


float square(float x) { return x * x; }

unsigned long order2npix(unsigned short pix) {
    return 12 * 1 << (2 * pix);
}

std::vector<std::vector<unsigned long long int> > generate_alignment(
    const std::vector<unsigned long long int> &histogram,
    unsigned short highest_order,
    unsigned short lowest_order,
    unsigned long long int threshold
);

PYBIND11_MODULE(pixel_math_ext, m) {
    m.def("square", &square);
    m.def("order2npix", &order2npix);
    m.def("generate_alignment", &generate_alignment);
}

#endif