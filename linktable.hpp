#ifndef LINK_TABLE_HPP
#define LINK_TABLE_HPP

#include <iostream>
#include <sstream>
#include "radix_convert.hpp"

using namespace std;


class Model {
    public:
        Model();
        int getID() const;
        void setIndex(const string &i);
        string getIndex() const;
        // void setDecIndex();
        int getDecIndex() const;
        int getNum() const;
        int getPreID() const;
        int getNextID() const;
        void setAddress(const string &a);
        string getAddress() const;
    
    private:
        int id;
        string index;
        int decimal_index;
        int num;
        int pre_id;
        int next_id;
        string address;
};

#endif