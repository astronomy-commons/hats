#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <vector>
#include "pixel_math.h"

std::vector<std::vector<unsigned long long int> > generate_alignment(
    const std::vector<unsigned long long int> &histogram,
    unsigned short highest_order,
    unsigned short lowest_order,
    unsigned long long int threshold
){
    std::vector<std::vector<unsigned long long int> > result (order2npix(highest_order));
    return result;
}
