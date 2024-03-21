#ifndef TXTHELPER_HPP
#define TXTHELPER_HPP

#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <sstream>
#include <filesystem>
#include <unordered_map>

using namespace std;

class TxtHelper {
    private:
        std::unordered_map<std::string, std::ostringstream> buffer;
    public:
        bool isTxtFile(const string& filePath);
        void openTxtFile(const string& filePath);
        void createPath(const string& filePath);
        vector<pair<string, string> > parseTxtFile(const string& filePath);
        void writeASCLine(const string& filePath, const pair<string, string>& data);
        void flushDataToFile();
};

#endif
