#include <iostream>
#include <sstream>
#include <fstream>
#include <filesystem>
#include <cstdlib>
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <queue>
#include <functional>
#include <condition_variable>
#include <tuple>
#include "json.hpp"
#include "rapidcsv.h"
#include "dbHelper.hpp"
#include "SimpleIni.h"
#include "utils.hpp"

int INDEX_LENGTH_SC_SEARCH = 0;


// vector<string> find_k_MAX(int k, const unordered_map<string, int>& linecount, int indexCount) {
//     vector<string> results;
//     vector<pair<string, int> > countVector(linecount.begin(), linecount.end());

//     sort(countVector.begin(), countVector.end(),
//         [](const pair<string, int>& a, const pair<string, int>& b) {
//             return a.second > b.second;
//         });

//     if (k <= 0) {
//         for (int i = 0; i < countVector.size(); i++) {
//             stringstream result;
//             result << countVector[i].first << "," << countVector[i].second << "/" << indexCount;
//             results.push_back(result.str());
//         }
//     } else {
//         for (int i = 0; i < k && i < countVector.size(); i++) {
//             stringstream result;
//             result << countVector[i].first << "," << countVector[i].second << "/" << indexCount;
//             results.push_back(result.str());
//         }
//     }

//     return results;

//     // for (json::iterator it = j.begin(); it != j.end(); ++it) {
//     //     vector<string> value = it.value();
//     //     if (value.size() > k) {
//     //         value.resize(k);
//     //         j[it.key()] = value;
//     //     }
//     // }
// }

std::vector<std::string> find_k_MAX(int k, const std::unordered_map<std::string, int>& expressedLinecount, int expressedIndexCount, const std::unordered_map<std::string, int>& unexpressedLinecount, int unexpressedIndexCount, const float alpha) {
    // json jsonMatches;

    // for (const auto& entry : expressedLinecount) {
    //     const string& str = entry.first;
    //     int expressedCount = entry.second;
    //     int unexpressedCount = unexpressedLinecount.count(str) ? unexpressedLinecount.at(str) : 0;
    //     std::stringstream match_degree_expressed;
    //     match_degree_expressed << expressedCount << "/" << expressedIndexCount;
    //     std::stringstream match_degree_unexpressed;
    //     match_degree_unexpressed << unexpressedCount << "/" << unexpressedIndexCount;

    //     double match_expressed = static_cast<double>(expressedCount) / expressedIndexCount;
    //     double match_unexpressed = static_cast<double>(unexpressedCount) / unexpressedIndexCount;
    //     double match = match_expressed * alpha + match_unexpressed * (1 - alpha);

    //     jsonMatches[str] = {
    //         {"match_degree_expressed", match_degree_expressed.str()},
    //         {"match_degree_unexpressed", match_degree_unexpressed.str()},
    //         {"match_degree", match}
    //     };
    // }

    // // 创建一个存储字符串和匹配值的向量，并根据匹配值进行排序
    // vector<pair<string, double>> sortedPairs;
    // for (const auto& entry : jsonMatches.items()) {
    //     sortedPairs.push_back(make_pair(entry.key(), entry.value()["match_degree"].get<double>()));
    // }
    // sort(sortedPairs.begin(), sortedPairs.end(), [](const pair<string, double>& a, const pair<string, double>& b) {
    //     return a.second > b.second; // 降序排序
    // });

    // // 判断k的值，如果小于等于0，则返回所有结果
    // if (k <= 0) {
    //     vector<string> allResults;
    //     for (const auto& pair : sortedPairs) {
    //         const string& str = pair.first;
    //         const json& matchInfo = jsonMatches[str];
    //         string result = str + ", match_degree_expressed: " + matchInfo["match_degree_expressed"].get<string>() +
    //                         ", match_degree_unexpressed: " + matchInfo["match_degree_unexpressed"].get<string>() +
    //                         ", match: " + to_string(matchInfo["match_degree"].get<double>());
    //         allResults.push_back(result);
    //     }
    //     return allResults;
    // } else {
    //     // 取出前 k 个字符串，并将字符串和匹配值连接起来
    //     vector<string> topK;
    //     for (int i = 0; i < k && i < sortedPairs.size(); ++i) {
    //         const string& str = sortedPairs[i].first;
    //         const json& matchInfo = jsonMatches[str];
    //         string result = str + ", match_degree_expressed: " + matchInfo["match_degree_expressed"].get<string>() +
    //                         ", match_degree_unexpressed: " + matchInfo["match_degree_unexpressed"].get<string>() +
    //                         ", match: " + to_string(matchInfo["match_degree"].get<double>());
    //         topK.push_back(result);
    //     }
    //     return topK;
    // }
    std::vector<std::pair<std::string, int> > sortedExpressedPairs; 
    for (const auto& entry : expressedLinecount) {
        sortedExpressedPairs.push_back(entry);
    }

    sort(sortedExpressedPairs.begin(), sortedExpressedPairs.end(), [](const std::pair<std::string, int>& a, const std::pair<std::string, int>& b) {
        return a.second > b.second;
    });

    // int i = 0;
    // int topKCount = expressedIndexCount;
    std::vector<std::string> result;
    for (int i = 0; i < sortedExpressedPairs.size(); ++i) {
        const std::string& str = sortedExpressedPairs[i].first;
        int expressedCount = sortedExpressedPairs[i].second;

        // if (expressedCount < topKCount && k > 0) {
        //     topKCount = expressedCount;
        //     i++;
        // }

        if (expressedCount == 0) {
            int unexpressedCount = unexpressedLinecount.count(str) ? unexpressedLinecount.at(str) : 0;
            double match_unexpressed = static_cast<double>(unexpressedCount) / static_cast<double>(unexpressedIndexCount);
            if (match_unexpressed > alpha) {
                std::stringstream match_degree_expressed;
                match_degree_expressed << expressedCount << "/" << expressedIndexCount;
                std::stringstream match_degree_unexpressed;
                match_degree_unexpressed << unexpressedCount << "/" << unexpressedIndexCount;

                std::string result_str = str + ", match_degree_expressed: " + match_degree_expressed.str() +
                                ", match_degree_unexpressed: " + match_degree_unexpressed.str();
                result.push_back(result_str);
            }
        } else {
            int unexpressedCount = unexpressedLinecount.count(str) ? unexpressedLinecount.at(str) : 0;
            double match_unexpressed = static_cast<double>(unexpressedCount) / static_cast<double>(unexpressedIndexCount);
            std::stringstream match_degree_expressed;
            match_degree_expressed << expressedCount << "/" << expressedIndexCount;
            std::stringstream match_degree_unexpressed;
            match_degree_unexpressed << unexpressedCount << "/" << unexpressedIndexCount;

            std::string result_str = str + ", match_degree_expressed: " + match_degree_expressed.str() +
                            ", match_degree_unexpressed: " + match_degree_unexpressed.str();
            result.push_back(result_str);
        }
    }
    return result;
}

std::string sc_index_encode(const rapidcsv::Document& query_csv, const rapidcsv::Document& gene_marker_csv, const std::string& cellName, const std::string& indexName) {
    std::vector<std::string> gene_marker = gene_marker_csv.GetColumn<std::string>(indexName);
    std::stringstream indexstream;
    int unexpressedGeneNum = 0;
    int nonExistentGeneNum = 0;
    for (int i = 0; i < INDEX_LENGTH_SC_SEARCH; i++) {
        try {
            std::string geneName = gene_marker[i];
            float cell = query_csv.GetCell<float>(geneName, cellName);
            int j = 0;
            if (cell > 0) {
                j = 1;
            } else {
                unexpressedGeneNum++;
            }
            // int j = (cell > 0) ? 1 : 0;
            indexstream << j;
        } catch (const std::exception &e) {
            indexstream << 0;
            unexpressedGeneNum++;
            nonExistentGeneNum++;
        }
    }
    if (nonExistentGeneNum == INDEX_LENGTH_SC_SEARCH) {
        return "invalid index";
    } else if (unexpressedGeneNum == INDEX_LENGTH_SC_SEARCH) {
        return "all zero";
    } else {
        std::string index = indexstream.str();
        return index;
    }
}

void search_single_cell_a_index(const rapidcsv::Document& query_csv, const rapidcsv::Document& gene_marker_csv, std::unordered_map<std::string, std::vector<std::string> >& expressedIndexcode, std::unordered_map<std::string, std::vector<std::string> >& unexpressedIndexcode, const std::string& cellName, int& expressedIndexNum, int& unexpressedIndexNum, const std::string& indexName) {
    try {
        std::string index = sc_index_encode(query_csv, gene_marker_csv, cellName, indexName);
        // std::string table_name;
        if (index == "invalid index") {
            // 所有index无效，放弃
        } else if (index == "all zero") {
            // 这里单独处理全0的情况
            std::string all_zero;
            all_zero.resize(INDEX_LENGTH_SC_SEARCH, '0');

            if (unexpressedIndexcode.count(all_zero) != 0) {
                unexpressedIndexcode[all_zero].push_back(indexName);
            } else {
                unexpressedIndexcode[all_zero] = std::vector<std::string>({indexName});
            }
            unexpressedIndexNum++;
        } else {
            if (expressedIndexcode.count(index) !=0) {
                expressedIndexcode[index].push_back(indexName);
            } else {
                expressedIndexcode[index] = std::vector<std::string>({indexName});
            }
            expressedIndexNum++;

        }
    } catch (const std::exception &e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }
}

std::pair<int, int> search_single_cell_all_index(const rapidcsv::Document& query_csv, const rapidcsv::Document& gene_marker_csv, std::unordered_map<std::string, std::vector<std::string> >& expressedIndexcode, std::unordered_map<std::string, std::vector<std::string> >& unexpressedIndexcode, const std::string& cellName) {
    try {
        // 这里indexNum是有用的index数量，也就是用于查询的cell，有多少个能够正确编码的index
        int expressedIndexNum = 0;
        int unexpressedIndexNum = 0;

//        cout << "开始搜索细胞" << cellName << getCurrentTime() << endl;
        for (const auto &indexName : gene_marker_csv.GetColumnNames()) {
            search_single_cell_a_index(query_csv, gene_marker_csv, expressedIndexcode, unexpressedIndexcode, cellName, expressedIndexNum, unexpressedIndexNum, indexName);
        }

//        cout << cellName << "全部提交" << endl;

//        cout << "结束搜索" << cellName << getCurrentTime() << endl;
        return std::make_pair(expressedIndexNum, unexpressedIndexNum);
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return std::make_pair(0, 0);
    }
}

void search_single_cell(const rapidcsv::Document& query_csv, const rapidcsv::Document& gene_marker_csv, nlohmann::json& j, const std::string& cellName, int k, const float alpha, DBHelper& dbHelper) {
    // auto start_time = std::chrono::steady_clock::now();

    try {
        // std::cout << "*********************** " << cellName << " start ***********************" << std::endl;
        std::unordered_map<std::string, std::vector<std::string> > expressedIndexcode;
        std::unordered_map<std::string, std::vector<std::string> > unexpressedIndexcode;
        std::unordered_map<std::string, int> searchedCellExpressedIndexCount;
        std::unordered_map<std::string, int> searchedCellUnexpressedIndexCount;
        int expressedIndexCount = 0;
        int unexpressedIndexCount = 0;
        std::tie(expressedIndexCount, unexpressedIndexCount) = search_single_cell_all_index(query_csv, gene_marker_csv, expressedIndexcode, unexpressedIndexcode, cellName);

        // // 输出 expressedIndexcode
        // std::cout << "expressedIndexcode:\n";
        // for (const auto& entry : expressedIndexcode) {
        //     const std::string& key = entry.first;
        //     const std::vector<std::string>& values = entry.second;

        //     std::cout << "Key: " << key << "\n";
        //     std::cout << "Values: ";
        //     for (const std::string& value : values) {
        //         std::cout << value << " ";
        //     }
        //     std::cout << "\n";
        // }

        // // 输出 unexpressedIndexcode
        // std::cout << "unexpressedIndexcode:\n";
        // for (const auto& entry : unexpressedIndexcode) {
        //     const std::string& key = entry.first;
        //     const std::vector<std::string>& values = entry.second;

        //     std::cout << "Key: " << key << "\n";
        //     std::cout << "Values: ";
        //     for (const std::string& value : values) {
        //         std::cout << value << " ";
        //     }
        //     std::cout << "\n";
        // }

        dbHelper.query(expressedIndexcode, searchedCellExpressedIndexCount);
        dbHelper.query(unexpressedIndexcode, searchedCellUnexpressedIndexCount);

        // // 输出 searchedCellExpressedIndexCount
        // std::cout << "searchedCellExpressedIndexCount:\n";
        // for (const auto& entry : searchedCellExpressedIndexCount) {
        //     const std::string& key = entry.first;
        //     const int value = entry.second;

        //     std::cout << "Key: " << key << "\n";
        //     std::cout << "Value: " << value << "\n";
        // }

        // // 输出 searchedCellUnexpressedIndexCount
        // std::cout << "searchedCellUnexpressedIndexCount:\n";
        // for (const auto& entry : searchedCellUnexpressedIndexCount) {
        //     const std::string& key = entry.first;
        //     const int value = entry.second;

        //     std::cout << "Key: " << key << "\n";
        //     std::cout << "Value: " << value << "\n";
        // }

        std::vector<std::string> cell_list = find_k_MAX(k, searchedCellExpressedIndexCount, expressedIndexCount, searchedCellUnexpressedIndexCount, unexpressedIndexCount, alpha);

        // auto end_time = std::chrono::steady_clock::now();
        // auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        // std::cout << cellName << "搜索用时" << duration.count() / 1000.0 << "秒" << std::endl;

        // 使用互斥锁保护对 JSON 对象的访问
        // lock_guard<mutex> lock(jsonMutex);
        j[cellName] = cell_list;
        // std::cout << "*********************** " << cellName << " end ***********************" << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }
}

extern "C" {
    void sc_search(const char* query_csv_path, int k, float cutoff, const char* output_result, const char* iniPath, const float alpha) {
        try {
            std::string query(query_csv_path);
            std::string output(output_result);

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

            INDEX_LENGTH_SC_SEARCH = std::stoi(index_marker_length);

            nlohmann::json j;

            rapidcsv::Document gene_marker_csv = rapidcsv::Document(gene_marker_path, rapidcsv::LabelParams(0, -1));
            //用来query的csv，行名是cellname，列名是genename
            rapidcsv::Document query_csv = rapidcsv::Document(query, rapidcsv::LabelParams(0, 0));

            // ThreadPool pool(100);

            auto start_time = std::chrono::steady_clock::now();
            std::cout << "总搜索开始" << getCurrentTime() << std::endl;

            for (const auto &cellName : query_csv.GetRowNames()) {
                search_single_cell(query_csv, gene_marker_csv, j, cellName, k, alpha, dbHelper);
            }

            std::cout << "所有提交结束" << getCurrentTime() << std::endl;

            std::cout << "总搜索结束" << getCurrentTime() << std::endl;

            auto end_time = std::chrono::steady_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

            std::cout << "总搜索用时" << duration.count() / 1000.0 << "秒" << std::endl;

            std::ofstream file(output);
            file << j.dump();
            file.close();

            dbHelper.closeConnectionDB();
        }  catch (const std::exception& e) {
            std::cerr << "Error: " << e.what() << std::endl;
        }
    }
}

// int main() {
//     // create_table("/mnt/c/Users/research/Downloads/sc_search/test1/create_table.txt", "/mnt/c/Users/research/Downloads/sc_search/test1/sc_config.ini");
//     sc_search("/mnt/c/Users/research/Downloads/sc_search/test_3_brca.csv", 3, 0, "/mnt/c/Users/research/Downloads/sc_search/output1.json", "/mnt/c/Users/research/Downloads/sc_search/test1/sc_config.ini", 0.7);
//     // create_table("/Users/zhizhenzhou/zzz/Postgraduate/Guided Study/demo/create_table_sc_st.txt", "/Users/zhizhenzhou/zzz/Postgraduate/Guided Study/demo/djangoDemo/config/sc_st_config.ini");
//     // rapidcsv::Document gene_marker_csv = rapidcsv::Document(gene_marker_path, rapidcsv::LabelParams(0, -1));
//     // for (const auto &indexName : gene_marker_csv.GetColumnNames()) {
//     //     cout << indexName << endl;
//     // }
// }
