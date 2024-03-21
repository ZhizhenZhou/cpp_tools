#include <iostream>
#include <sstream>
#include "linktable.hpp"
#include "radix_convert.hpp"

using namespace std;

Model::Model() {}

int Model::getID() const {
    return id;
}

void Model::setIndex(const string &i) {
    index = i;
    decimal_index = RadixConvert::binaryToDecimal(index);
}

string Model::getIndex() const {
    return index;
}

// void Model::setDecIndex() {
//     decimal_index = RadixConvert::binaryToDecimal(index);
// }

int Model::getDecIndex() const {
    return decimal_index;
}

int Model::getNum() const {
    return num;
}

int Model::getPreID() const {
    return pre_id;
}

int Model::getNextID() const {
    return next_id;
}

void Model::setAddress(const string &a) {
    address = a;
}

string Model::getAddress() const {
    return address;
}