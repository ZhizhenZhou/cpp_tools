#include <iostream>
#include <sstream>
#include <fstream>
#include <filesystem>
#include <cstdlib>
#include <string>
#include <vector>
// #include <ctime>
#include <time.h>
#include <chrono>
#include <thread>
#include <mutex>
#include <queue>
#include <functional>
#include <condition_variable>
#include <tuple>
#include "rapidcsv.h"
#include "dbHelper.hpp"
#include "SimpleIni.h"
#include "utils.hpp"

int INDEX_LENGTH_SC_CREATETABLE = 0;

template<typename T1, typename T2>
std::pair<T1, std::vector<T2>> parseCSVLine(const std::string& line) {
    std::pair<T1, std::vector<T2>> result;
    std::vector<T2> values;
    std::stringstream ss(line);
    std::string token;
    if (std::getline(ss, token, ',')) {
        std::stringstream converter(token);
        converter >> result.first;
    }
    while (std::getline(ss, token, ',')) {
        std::stringstream converter(token);
        T2 value;
        converter >> value;
        values.push_back(value);
    }
    result.second = values;
    return result;
}

std::vector<int> findIndices(const std::vector<std::string>& a, const std::vector<std::string>& b, int indexLength) {
    std::vector<int> indices;
    int nonExistentIndexNum = 0;
    for (int i = 0; i < indexLength; i++) {
        if (i < a.size()) {
            auto it = std::find(b.begin(), b.end(), a[i]);
            if (it != b.end()) {
                int index = std::distance(b.begin(), it);
                indices.push_back(index);
            } else {
                int index = -1;
                indices.push_back(index);
                nonExistentIndexNum++;
            }
        } else {
            int index = -1;
            indices.push_back(index);
            nonExistentIndexNum++;
        }
    }
    if (nonExistentIndexNum == indexLength) {
        return {};
    } else {
        return indices;
    }
}

// 用于处理单个细胞的操作
void processCell(const std::string& line, const std::map<std::string, std::vector<std::string> >& indexDict, const std::vector<std::string>& headerGenes, std::unordered_map<std::string, std::unordered_map<std::string, std::vector<std::string> > >& ModelDict) {
    try {
        std::pair<std::string, std::vector<float> > csvline = parseCSVLine<std::string, float>(line);

        // 读取细胞信息
        std::string cellName = csvline.first;
        std::vector<float> row = csvline.second;

        // cout << "开始处理" << cellName << row.size() << endl;

        for (const auto& indexPair : indexDict) {
            const std::string indexName = indexPair.first;
            const std::vector<std::string>& indexGenes = indexPair.second;

            std::vector<int> indexGeneLocate = findIndices(indexGenes, headerGenes, INDEX_LENGTH_SC_CREATETABLE);

            if (!indexGeneLocate.empty()) {
                // 创建索引字符串
                std::stringstream indexstream;
                int unexpressedGeneNum = 0;
                for (const auto& i : indexGeneLocate) {
                    int x;
                    if (i != -1) {
                        if (row[i] > 0) {
                            x = 1;
                        } else {
                            x = 0;
                            unexpressedGeneNum++;
                        }
                        // x = (row[i] > 0) ? 1 : 0;
                    } else {
                        x = 0;
                        unexpressedGeneNum++;
                    }
                    indexstream << x;
                }
                
                // if (unexpressedGeneNum != std::stoi(index_marker_length)) {
                std::string index = indexstream.str();

                // 添加到 ModelDict 中
                // std::lock_guard<std::mutex> lock(mtx);  // 加锁以防止多个线程同时写入 ModelDict
                ModelDict[index][cellName].push_back(indexName);
                // }
            }
        }
    // cout << "结束处理" << cellName << endl;
    } catch (const std::exception &e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }
}

void process_csv_file(const std::string& csvFile, const std::map<std::string, std::vector<std::string>>& indexDict, DBHelper& dbHelper) {
    std::ifstream file(csvFile);

    if (file.is_open()) {
        std::string line;
        // bool isHeader = true;
        std::vector<std::string> headerGenes;
        // string cellName;
        // vector<float> row;

        // unordered_map<string, vector<pair<string, Model> > > ModelDict;
        std::unordered_map<std::string, std::unordered_map<std::string, std::vector<std::string> > > indextables;

        std::cout << "开始处理csv文件. " << getCurrentTime() << std::endl;

        // 处理第一行数据
        getline(file, line); 
        std::pair<std::string, std::vector<std::string> > firstRow = parseCSVLine<std::string, std::string>(line);
        headerGenes = firstRow.second;

        // ThreadPool pool(500);

        try {
            while (getline(file, line)) {
                processCell(line, indexDict, headerGenes, indextables);
                // pool.enqueue(processCell, line, std::ref(indexDict), std::ref(headerGenes), std::ref(indextables));
	        }

            // pool.wait();

            std::cout << "处理结束csv文件.part1 " << getCurrentTime() << std::endl;
        } catch (const std::exception &e) {
            std::cerr << "Error: " << e.what() << std::endl;
        }

        file.close();

        dbHelper.insert_table_items(indextables, csvFile);

        std::cout << "处理结束csv文件.part2 " << getCurrentTime() << std::endl;
    }
}

extern "C" {
    void create_table(const char* f, const char* iniPath) {
        std::string filename(f);
        std::string configPath(iniPath);

        std::string gene_marker_path;
        std::string index_marker_length;

        std::string python_command;
        std::string python_script_path;
        std::string sql_script_path;
        std::string dbhost;
        std::string dbport;
        std::string dbname;
        std::string dbuser;
        std::string dbpassword;

        DBHelper dbHelper;

        CSimpleIni ini;
        ini.LoadFile(iniPath);

        dbhost = ini.GetValue("database", "host", "");
        dbport = ini.GetValue("database", "port", "");
        dbname = ini.GetValue("database", "name", "");
        dbuser = ini.GetValue("database", "user", "");
        dbpassword = ini.GetValue("database", "password", "");

        // folderPath = ini.GetValue("filepath", "./gene", "");
        gene_marker_path = ini.GetValue("filepath", "marker_gene_path", "");
        index_marker_length = ini.GetValue("filepath", "index_marker_length", "0");

        python_command = ini.GetValue("pythonscript", "command", "");
        python_script_path = ini.GetValue("pythonscript", "script_path", "");
        sql_script_path = ini.GetValue("sqlscript", "script_path", "");

        dbHelper.setHost(dbhost);
        dbHelper.setPort(dbport);
        dbHelper.setName(dbname);
        dbHelper.setUser(dbuser);
        dbHelper.setPassword(dbpassword);
        dbHelper.setSQLScriptPath(sql_script_path);

        dbHelper.connectionDB();

        INDEX_LENGTH_SC_CREATETABLE = std::stoi(index_marker_length);

        std::vector<std::string> csvFiles;

        std::ifstream file(filename);

        //file里面每一行是需要添加的数据集的文件名
        if (file.is_open()) {
            std::string line;
            while (getline(file, line)) {
                //每一行是一个文件名，将这些文件更新到表里
                std::string filepath = line;
                // cout << "filepath " << filepath << endl;
                if (std::filesystem::exists(filepath)) {
                    csvFiles.push_back(filepath);
                } else {
                    std::cout << line << " does not exist in file system." << std::endl;
                }
            }
            file.close();
        } else {
            std::cout << "Failed to open file." << std::endl;
        }

        for (const std::string& csvFile : csvFiles) {
            std::string arg1 = gene_marker_path;
            std::string arg2 = csvFile;
            std::string arg3 = index_marker_length;
            std::string fullCommand = python_command + " " + python_script_path + " -i " + arg1 + " -d " + arg2 + " -n " + arg3;
            int result = std::system(fullCommand.c_str());
            if (result == 0) {
                std::cout << "Update index executed successfully." << std::endl;
            } else {
                std::cout << "Update index execution failed." << std::endl;
            }
            rapidcsv::Document gene_marker_csv = rapidcsv::Document(gene_marker_path, rapidcsv::LabelParams(0, -1));

            std::map<std::string, std::vector<std::string>> indexDict;

            for (const auto& indexName : gene_marker_csv.GetColumnNames()) {
                std::vector<std::string> gene_marker = gene_marker_csv.GetColumn<std::string>(indexName);
                indexDict[indexName] = gene_marker;
            }

            std::cout << "File: " << csvFile << " is processing at " << getCurrentTime() << std::endl;
            process_csv_file(csvFile, indexDict, dbHelper);
            std::cout << "File: " << csvFile << " is processed at " << getCurrentTime() << std::endl;
        }

        dbHelper.closeConnectionDB();
    }
}

// int main() {
//     create_table("/mnt/c/Users/research/Downloads/sc_search/test1/create_table.txt", "/mnt/c/Users/research/Downloads/sc_search/test1/sc_config.ini");
//     // sc_search("/mnt/c/Users/research/Downloads/sc_search/test_3_brca.csv", 3, 0, "/mnt/c/Users/research/Downloads/sc_search/output1.json", "/mnt/c/Users/research/Downloads/sc_search/test1/sc_config.ini", 0.7);
//     // create_table("/Users/zhizhenzhou/zzz/Postgraduate/Guided Study/demo/create_table_sc_st.txt", "/Users/zhizhenzhou/zzz/Postgraduate/Guided Study/demo/djangoDemo/config/sc_st_config.ini");
//     // rapidcsv::Document gene_marker_csv = rapidcsv::Document(gene_marker_path, rapidcsv::LabelParams(0, -1));
//     // for (const auto &indexName : gene_marker_csv.GetColumnNames()) {
//     //     cout << indexName << endl;
//     // }
// }
