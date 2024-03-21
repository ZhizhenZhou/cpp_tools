#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <sstream>
#include <filesystem>
#include "txtHelper.hpp"
#include <unordered_map>

using namespace std;


bool TxtHelper::isTxtFile(const string& filePath) {
    // 获取文件扩展名
    size_t dotPos = filePath.rfind(".");
    if (dotPos == string::npos)
        return false;

    string extension = filePath.substr(dotPos + 1);

    // 检查文件扩展名是否为txt
    return (extension == "txt");
}

void TxtHelper::openTxtFile(const string& filePath) {
    if (!isTxtFile(filePath)) {
        return;
    }

    // 打开txt文件
    ifstream file(filePath);
    if (file.is_open()) {
        string line;
        while (getline(file, line)) {
            cout << line << endl;
        }
        file.close();
    } else {
        cout << "无法打开文件：" << filePath << endl;
    }
}

void TxtHelper::createPath(const string& filePath) {
    if (!std::filesystem::exists(filePath)) {
        // 文件不存在，创建路径
        std::filesystem::path directory = std::filesystem::path(filePath).parent_path();

        if (!std::filesystem::exists(directory)) {
            bool created = std::filesystem::create_directories(directory);

            if (created) {
                // cout << "路径已创建！" << endl;
            } else {
                cout << "无法创建路径。" << endl;
            }
        }

        // 创建文件（这里只是模拟）
        ofstream file(filePath);
        file.close();

        // cout << "文件已创建！" << endl;
    } else {
        cout << "文件已存在。" << endl;
    }
}

vector<pair<string, string> > TxtHelper::parseTxtFile(const string& filePath) {
    vector<pair<string, string> > data;

    if (!isTxtFile(filePath)) {
        cout << "不是txt文件！" << endl;
        return data;
    }

    ifstream file(filePath);
    if (file.is_open()) {
        string line;
        while (getline(file, line)) {
            istringstream iss(line);
            string str;
            // int num;
            string numStr;

            // if (iss >> str >> num) {
            //     data.push_back(make_pair(str, num));
            // }
            if (getline(iss, str, ',') && getline(iss, numStr)) {
                data.push_back(make_pair(str, numStr));
            }
        }
        file.close();
    } else {
        cout << "无法打开文件：" << filePath << endl;
    }

    return data;
}

// void TxtHelper::writeTxtFile(const string& filePath, const pair<string, string>& data) {
//     ofstream file(filePath, ios::app);
//     if (file.is_open()) {
//         file << data.first << "," << data.second << endl;
//         file.close();
//         // cout << "数据已写入文件：" << filePath << endl;
//     } else {
//         cout << "无法打开文件：" << filePath << endl;
//     }
// }

void TxtHelper::writeASCLine(const std::string& filePath, const std::pair<std::string, std::string>& data) {
    buffer[filePath] << data.first << "," << data.second << std::endl;
}

void TxtHelper::flushDataToFile() {
    for (const auto& item : buffer) {
        const std::string& filePath = item.first;
        const std::ostringstream& contentStream = item.second;

        std::ofstream file(filePath, std::ios::app);
        if (file.is_open()) {
            file << contentStream.str();
            file.close();
        } else {
            std::cout << "无法打开文件：" << filePath << std::endl;
        }
    }

    buffer.clear();
}
