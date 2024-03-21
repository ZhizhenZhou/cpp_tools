#ifndef RADIX_CONVERT_HPP
#define RADIX_CONVERT_HPP

#include <iostream>
#include <sstream>

using namespace std;

class RadixConvert {
    public:
        RadixConvert();
        static int binaryToDecimal(const string &bin_str);
};

#endif