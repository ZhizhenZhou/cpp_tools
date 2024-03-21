#include <iostream>
#include <sstream>
#include <cmath>
#include "radix_convert.hpp"

using namespace std;

RadixConvert::RadixConvert() {}

int RadixConvert::binaryToDecimal(const string &binaryStr) {
    int decimalNum = 0;
    int len = binaryStr.length();

    for (int i = 0; i < len; ++i) {
        if (binaryStr[i] == '1') {
            decimalNum += pow(2, len - 1 - i);
        } else if (binaryStr[i] == '0') {

        } else {
            return -1;
        }
    }

    return decimalNum;
}