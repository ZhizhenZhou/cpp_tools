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
#include "json.hpp"
#include "rapidcsv.h"
#include "dbHelper.hpp"
#include "linktable.hpp"
#include "txtHelper.hpp"
#include "SimpleIni.h"

using namespace std;
using json = nlohmann::json;

string gene_marker_path;
string index_folder;
string index_marker_length;
// string folderPath;

string python_command;
string python_script_path;
string dbhost;
string dbport;
string dbname;
string dbuser;
string dbpassword;

DBHelper dbHelper;

CSimpleIni ini;

mutex jsonMutex;
mutex mtx;

class ThreadPool {
public:
    ThreadPool(size_t numThreads) : totalTasks(0), completedTasks(0), stop(false) {
        for (size_t i = 0; i < numThreads; ++i) {
            threads.emplace_back([this] {
                while (true) {
                    std::function<void()> task;
                    {
                        std::unique_lock<std::mutex> lock(this->queueMutex);
                        this->condition.wait(lock, [this] { return this->stop || !this->tasks.empty(); });
                        if (this->stop && this->tasks.empty()) {
                            return;
                        }
                        task = std::move(this->tasks.front());
                        this->tasks.pop();
                    }
                    task();
                    {
                        std::unique_lock<std::mutex> lock(completedTasksMutex);
                        ++completedTasks;
                        if (completedTasks == totalTasks) {
                            completionCondition.notify_all();
                        }
                    }
                }
            });
        }
    }

    template <typename Function, typename... Args>
    void enqueue(Function&& func, Args&&... args) {
        {
            std::unique_lock<std::mutex> lock(queueMutex);
            tasks.emplace([func, args...] { func(args...); });
        }
        {
            std::unique_lock<std::mutex> lock(completedTasksMutex);
            ++totalTasks;
        }
        condition.notify_one();
    }

    void wait() {
        std::unique_lock<std::mutex> lock(completedTasksMutex);
        completionCondition.wait(lock, [this] { return completedTasks == totalTasks; });
    }

    ~ThreadPool() {
        {
            std::unique_lock<std::mutex> lock(queueMutex);
            stop = true;
        }
        condition.notify_all();
        for (std::thread& thread : threads) {
            thread.join();
        }
    }

private:
    std::vector<std::thread> threads;
    std::queue<std::function<void()>> tasks;
    std::mutex queueMutex;
    std::condition_variable condition;

    std::mutex completedTasksMutex;
    std::condition_variable completionCondition;
    size_t totalTasks;
    size_t completedTasks;

    bool stop;
};

void init(const char* configPath) {
    ini.LoadFile(configPath);

    dbhost = ini.GetValue("database", "host", "");
    dbport = ini.GetValue("database", "port", "");
    dbname = ini.GetValue("database", "name", "");
    dbuser = ini.GetValue("database", "user", "");
    dbpassword = ini.GetValue("database", "password", "");

    // folderPath = ini.GetValue("filepath", "./gene", "");
    gene_marker_path = ini.GetValue("filepath", "marker_gene_path", "");
    index_folder = ini.GetValue("filepath", "index_folder", "");
    index_marker_length = ini.GetValue("filepath", "index_marker_length", "0");

    python_command = ini.GetValue("pythonscript", "command", "");
    python_script_path = ini.GetValue("pythonscript", "script_path", "");

    dbHelper.setHost(dbhost);
    dbHelper.setPort(dbport);
    dbHelper.setName(dbname);
    dbHelper.setUser(dbuser);
    dbHelper.setPassword(dbpassword);

    dbHelper.connectionDB();
}

void end() {
    dbHelper.closeConnectionDB();
}

std::string getCurrentTime() {
    // // 获取当前时间
    // std::time_t currentTime = std::time(nullptr);

    // // 转换为本地时间
    // std::tm* localTime = std::localtime(&currentTime);

    // // 格式化时间字符串
    // char buffer[80];
    // std::strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", localTime);

    // // 返回时间字符串
    // return std::string(buffer);

    // 获取当前时间点

    time_t curtime;
 
    time(&curtime);

    return std::string(ctime(&curtime));
}

void lineCount(const string& filepath, unordered_map<string, int>& linecount) {
    ifstream file(filepath); // 打开输入文件
    if (file.is_open()) {
        string line;
        while (getline(file, line)) {
            // 统计每一行的出现次数
            linecount[line]++;
        }

        file.close();
    } else {
        cout << "没有对应的index编码结果" << filepath << endl;
    }
}

void cutoff_map(float cutoff, unordered_map<string, int>& linecount) {
    auto it = linecount.begin();
    while (it != linecount.end())
    {
        if (it->second < ceil(cutoff)) {
            it = linecount.erase(it);
        } else {
            it++;
        }
    }
    
}

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

vector<string> find_k_MAX(int k, const unordered_map<string, int>& expressedLinecount, int expressedIndexCount, const unordered_map<string, int>& unexpressedLinecount, int unexpressedIndexCount, const float alpha) {
    int k2 = 2;
    float alpha2 = 0.8;
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
    vector<pair<string, int> > sortedExpressedPairs; 
    for (const auto& entry : expressedLinecount) {
        sortedExpressedPairs.push_back(entry);
    }

    sort(sortedExpressedPairs.begin(), sortedExpressedPairs.end(), [](const pair<string, int>& a, const pair<string, int>& b) {
        return a.second > b.second;
    });

    int k1 = 0;
    int topKCount = expressedIndexCount;
    std::vector<std::string> result;
    for (int i = 0; i < sortedExpressedPairs.size(); ++i) {
        const string& str = sortedExpressedPairs[i].first;
        int expressedCount = sortedExpressedPairs[i].second;

        if (expressedCount < topKCount && k > 0) {
            topKCount = expressedCount;
            k1++;
        }

        if (k1 <= k2) {
            int unexpressedCount = unexpressedLinecount.count(str) ? unexpressedLinecount.at(str) : 0;
            double match_unexpressed = static_cast<double>(unexpressedCount) / static_cast<double>(unexpressedIndexCount);
            if (match_unexpressed > alpha) {
                std::stringstream match_degree_expressed;
                match_degree_expressed << expressedCount << "/" << expressedIndexCount;
                std::stringstream match_degree_unexpressed;
                match_degree_unexpressed << unexpressedCount << "/" << unexpressedIndexCount;

                string result_str = str + ", match_degree_expressed: " + match_degree_expressed.str() +
                                ", match_degree_unexpressed: " + match_degree_unexpressed.str();
                result.push_back(result_str);
            }
        } else if (k1 <= k) {
            int unexpressedCount = unexpressedLinecount.count(str) ? unexpressedLinecount.at(str) : 0;
            double match_unexpressed = static_cast<double>(unexpressedCount) / static_cast<double>(unexpressedIndexCount);
            if (match_unexpressed > alpha2) {
                std::stringstream match_degree_expressed;
                match_degree_expressed << expressedCount << "/" << expressedIndexCount;
                std::stringstream match_degree_unexpressed;
                match_degree_unexpressed << unexpressedCount << "/" << unexpressedIndexCount;

                string result_str = str + ", match_degree_expressed: " + match_degree_expressed.str() +
                                ", match_degree_unexpressed: " + match_degree_unexpressed.str();
                result.push_back(result_str);
            }
        } else {
            break;
        }
    }
    return result;
}

vector<string> extract_all_string(const unordered_map<string, int>& linecount) {
    vector<string> results;

    for (const auto& pair : linecount) {
        results.push_back(pair.first);
    }

    return results;
}

vector<string> find_k_count_line(int k, const unordered_map<string, int>& linecount) {
    vector<string> results;

    for (const auto& pair : linecount) {
        if (pair.second == k) {
            results.push_back(pair.first);
        }
    }

    return results;
}

vector<string> sort_line_count(const unordered_map<string, int>& linecount) {
    vector<string> results;
    vector<pair<string, int>> countVector(linecount.begin(), linecount.end());

    sort(countVector.begin(), countVector.end(),
        [](const pair<string, int>& a, const pair<string, int>& b) {
            return a.second > b.second;
        });

    for (const auto& pair : countVector) {
        results.push_back(pair.first);
    }

    return results;
}

// bool fileExists(const std::string& filepath) {
//     ifstream file(filepath);
//     return file.good();
// }

// void createFile(const std::string& filepath) {
//     std::ofstream file(filepath);
//     if (!file) {
//         std::cerr << "Failed to create file: " << filepath << std::endl;
//         return;
//     }
//     file.close();
//     std::cout << "File created: " << filepath << std::endl;
// }

// void insert_data_from_csv_to_db(const string &filePath, const string &table_name, const vector<string> &gene_marker) {
//     if (!dbHelper.check_table_exist(table_name)) {
//         dbHelper.create_table(table_name);
//     }

//     rapidcsv::Document csv = rapidcsv::Document(filePath, rapidcsv::LabelParams(0, 0));

//     vector<string> cellNames = csv.GetRowNames();

//     for (const string &cellName : cellNames) {
//         stringstream indexstream;
//         bool iscellmatchindex = true;
        
//         for (const string &geneName : gene_marker) {
//             try {
//                 float cell = csv.GetCell<float>(geneName, cellName);
//                 int i = (cell > 0) ? 1 : 0;
//                 indexstream << i;
//             } catch (const exception &e) {
//                 // cerr << "指定的列名 " << geneName << " 在CSV文件中不存在" << endl;
//                 iscellmatchindex = false;
//                 break;
//             }
//         }

//         if (iscellmatchindex) {
//             string index = indexstream.str();
//             // cout << cellName << " index is " << index << "." << endl;
//             //将这个index插入表中
//             Model data;
//             data.setIndex(index);
//             dbHelper.insert_item(table_name, data, filePath, cellName, index_folder);
//         }
//     }
// }

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

// std::vector<size_t> findIndices(const std::vector<std::string>& a, const std::vector<std::string>& b) {
//     std::vector<size_t> indices;
//     for (size_t i = 0; i < a.size(); i++) {
//         auto it = std::find(b.begin(), b.end(), a[i]);
//         if (it != b.end()) {
//             size_t index = std::distance(b.begin(), it);
//             indices.push_back(index);
//         } else {
//             return {};
//         }
//     }
//     return indices;
// }

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
void processCell(const std::string& line, const std::map<std::string, std::vector<std::string> >& indexDict, const std::vector<std::string>& headerGenes, std::unordered_map<std::string, std::vector<std::pair<std::string, Model> > >& ModelDict) {
    try {
        std::pair<std::string, std::vector<float> > csvline = parseCSVLine<std::string, float>(line);

        // 读取细胞信息
        std::string cellName = csvline.first;
        std::vector<float> row = csvline.second;

        // cout << "开始处理" << cellName << row.size() << endl;

        for (const auto& indexPair : indexDict) {
            const std::string indexName = indexPair.first;
            const std::vector<std::string>& indexGenes = indexPair.second;

            std::vector<int> indexGeneLocate = findIndices(indexGenes, headerGenes, std::stoi(index_marker_length));

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

                // 创建 Model 对象
                Model data;
                data.setIndex(index);

                // 添加到 ModelDict 中
                std::lock_guard<std::mutex> lock(mtx);  // 加锁以防止多个线程同时写入 ModelDict
                ModelDict[indexName].push_back(std::make_pair(cellName, data));
                // }
            }
        }
    // cout << "结束处理" << cellName << endl;
    } catch (const std::exception &e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }
}

void process_csv_file(const string& csvFile, const map<string, vector<string>>& indexDict) {
    ifstream file(csvFile);

    if (file.is_open()) {
        string line;
        // bool isHeader = true;
        vector<string> headerGenes;
        // string cellName;
        // vector<float> row;

        unordered_map<string, vector<pair<string, Model> > > ModelDict;

        cout << "开始处理csv文件. " << getCurrentTime() << endl;

        // 处理第一行数据
        getline(file, line); 
        std::pair<std::string, std::vector<string> > firstRow = parseCSVLine<std::string, string>(line);
        headerGenes = firstRow.second;

        // // 定义一个向量来存储启动的线程对象
        // std::vector<std::thread> threads;
        ThreadPool pool(500);
        // int i = 1;

        try {
            while (getline(file, line)) {
                pool.enqueue(processCell, line, std::ref(indexDict), std::ref(headerGenes), std::ref(ModelDict));
	        }

            pool.wait();

            cout << "处理结束csv文件.part1 " << getCurrentTime() << endl;
        } catch (const std::exception &e) {
            std::cerr << "Error: " << e.what() << std::endl;
        }

        file.close();

        // for (auto& record : ModelDict) {
        //    string indexName = record.first;
        //    vector<pair<string, Model> >& dataList = record.second;

        //    // cout << indexName << endl;

        //    if (!dataList.empty()) {
        //        if (!dbHelper.check_table_exist(indexName)) {
        //            dbHelper.create_table(indexName);
        //        }

        //        for (auto& data : dataList) {
        //            string& cellname = data.first;
        //            Model& node = data.second;

        //            dbHelper.insert_item(indexName, node, csvFile, cellname, index_folder);
        //        }
        //    }
        // }
        dbHelper.insert_items(ModelDict, csvFile, index_folder);

        cout << "处理结束csv文件.part2 " << getCurrentTime() << endl;
        // dbHelper.workCommit();
    }
}

// void get_csv_list_from_folder(vector<string> &csvFiles, const string &folderpath) {    
//     for (const auto &file : filesystem::directory_iterator(folderpath)) {
//         if (file.path().extension() == ".csv") {
//             csvFiles.push_back(file.path().string());
//         }
//     }
// }

// void create_table(const string &folderpath) {
//     init();
//     vector<string> csvFiles;
//     get_csv_list_from_folder(csvFiles, folderpath);
//     rapidcsv::Document gene_marker_csv = rapidcsv::Document(gene_marker_path, rapidcsv::LabelParams(0, -1));
//     for (const auto &indexName : gene_marker_csv.GetColumnNames()) {
//         vector<string> gene_marker = gene_marker_csv.GetColumn<string>(indexName);
//         for (const string &csvFile : csvFiles) {
//             insert_data_from_csv_to_db(csvFile, indexName, gene_marker);
//         }
//     }
//     end();
// }

string search_address_with_index(const string &index, const string &table_name) {

    string res = dbHelper.search_address(table_name, RadixConvert::binaryToDecimal(index));

    return res;
}

string sc_index_encode(const rapidcsv::Document& query_csv, const rapidcsv::Document& gene_marker_csv, const string& cellName, const string& indexName) {
    std::vector<string> gene_marker = gene_marker_csv.GetColumn<string>(indexName);
    std::stringstream indexstream;
    int unexpressedGeneNum = 0;
    int nonExistentGeneNum = 0;
    for (int i = 0; i < std::stoi(index_marker_length); i++) {
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
        } catch (const exception &e) {
            indexstream << 0;
            unexpressedGeneNum++;
            nonExistentGeneNum++;
        }
    }
    if (nonExistentGeneNum == std::stoi(index_marker_length)) {
        return "invalid index";
    } else if (unexpressedGeneNum == std::stoi(index_marker_length)) {
        return "all zero";
        // throw std::logic_error("");
        // throw std::logic_error("All marker gene in this index " + indexName + " not exist.");
        // return;
    } else {
        string index = indexstream.str();
        return index;
    }
    // for (const string &geneName : gene_marker) {
    //     try {
    //         float cell = query_csv.GetCell<float>(geneName, cellName);
    //         int i = (cell > 0) ? 1 : 0;
    //         indexstream << i;
    //     } catch (const exception &e) {
    //         //这里要抛弃这个index
    //         // cerr << indexName << "中的" << geneName << "基因 在这个query的细胞" << cellName << "中 不存在" << endl;
    //         throw std::out_of_range("row not found: " + geneName);
    //     }
    // }
}

string find_index_path(const string& indexName, const string& index) {
    std::string result_path;
    if (index == "all zero") {
        std::string all_zero;
        all_zero.resize(std::stoi(index_marker_length), '0');
        result_path = index_folder + indexName + "/" + all_zero + ".txt";
    } else {
        result_path = index_folder + indexName + "/" + index + ".txt";
    }
    return result_path;
}

void search_linecount_for_a_cell_thread(const rapidcsv::Document& query_csv, const rapidcsv::Document& gene_marker_csv, unordered_map<string, int>& expressedLinecount, unordered_map<string, int>& unexpressedLinecount, const string& cellName, int& expressedIndexNum, int& unexpressedIndexNum, const std::string& indexName) {
//    auto start_time = std::chrono::steady_clock::now();
    try {
        std::string index = sc_index_encode(query_csv, gene_marker_csv, cellName, indexName);
        if (index == "invalid index") {
            // 所有index无效，放弃
        } else if (index == "all zero") {
            // 这里单独处理全0的情况
            std::string index_file_path = find_index_path(indexName, index);
            if (std::filesystem::exists(index_file_path)) {
                lineCount(index_file_path, unexpressedLinecount);
                unexpressedIndexNum++;
            }
        } else {
            string index_file_path = find_index_path(indexName, index);
            if (std::filesystem::exists(index_file_path)) {
    //            std::lock_guard<std::mutex> lock(mtx); // 上锁，以确保unordered_map的线程安全性
                lineCount(index_file_path, expressedLinecount);
                expressedIndexNum++;
            }
        }
    } catch (const std::out_of_range &e) {
        return;
    } catch (const std::exception &e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }

//    auto end_time = std::chrono::steady_clock::now();
//    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

//    std::cout << cellName << " " << indexName << "搜索用时" << duration.count() / 1000.0 << "秒" << std::endl;
}

std::pair<int, int> search_linecount_for_a_cell(const rapidcsv::Document& query_csv, const rapidcsv::Document& gene_marker_csv, unordered_map<string, int>& expressedLinecount, unordered_map<string, int>& unexpressedLinecount, const string& cellName) {
    try {
        // 这里indexNum是有用的index数量，也就是用于查询的cell，有多少个能够正确编码的index
        int expressedIndexNum = 0;
        int unexpressedIndexNum = 0;
//        std::vector<std::thread> threads;
//        ThreadPool pool(615);

//        cout << "开始搜索细胞" << cellName << getCurrentTime() << endl;
        for (const auto &indexName : gene_marker_csv.GetColumnNames()) {
//            threads.emplace_back(search_linecount_for_a_cell_thread, std::ref(query_csv), std::ref(gene_marker_csv), std::ref(linecount), std::cref(cellName), std::ref(indexNum), std::cref(indexName));
            search_linecount_for_a_cell_thread(query_csv, gene_marker_csv, expressedLinecount, unexpressedLinecount, cellName, expressedIndexNum, unexpressedIndexNum, indexName);
//            pool.enqueue(search_linecount_for_a_cell_thread, std::ref(query_csv), std::ref(gene_marker_csv), std::ref(linecount), std::cref(cellName), std::ref(indexNum), std::cref(indexName));
        }

//        cout << cellName << "全部提交" << endl;

//        pool.wait();
//        for (auto& thread : threads) {
//            thread.join();
//        }

//        cout << "结束搜索" << cellName << getCurrentTime() << endl;
        return std::make_pair(expressedIndexNum, unexpressedIndexNum);
    } catch (const std::exception& e) {
        cerr << "Error: " << e.what() << endl;
        return std::make_pair(0, 0);
    }
}

void process_cell(const rapidcsv::Document& query_csv, const rapidcsv::Document& gene_marker_csv, json& j, const string& cellName, int k, float cutoff, const float alpha) {
    auto start_time = std::chrono::steady_clock::now();

    try {
        unordered_map<string, int> expressedLinecount;
        unordered_map<string, int> unexpressedLinecount;
        int expressedIndexCount = 0;
        int unexpressedIndexCount = 0;
        std::tie(expressedIndexCount, unexpressedIndexCount) = search_linecount_for_a_cell(query_csv, gene_marker_csv, expressedLinecount, unexpressedLinecount, cellName);

        if (cutoff > 0) {
            float cutoff_num = expressedIndexCount * cutoff;
            //将linecount中int小于cutoff设定值的项删去
            cutoff_map(cutoff_num, expressedLinecount);
        }

        //取出linecount里前k个出现最多次数的line
        vector<string> cell_list = find_k_MAX(k, expressedLinecount, expressedIndexCount, unexpressedLinecount, unexpressedIndexCount, alpha);

        auto end_time = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

        // std::cout << cellName << "搜索用时" << duration.count() / 1000.0 << "秒" << std::endl;

        // 使用互斥锁保护对 JSON 对象的访问
        lock_guard<mutex> lock(jsonMutex);
        j[cellName] = cell_list;
    } catch (const std::exception& e) {
        cerr << "Error: " << e.what() << endl;
    }

//    auto end_time = std::chrono::steady_clock::now();
//    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
//
//    std::cout << cellName << "搜索用时" << duration.count() / 1000.0 << "秒" << std::endl;
}

extern "C" {
    void create_table(const char* f, const char* iniPath) {
        string filename(f);

        init(iniPath);

        vector<string> csvFiles;

        ifstream file(filename);

        //file里面每一行是需要添加的数据集的文件名
        if (file.is_open()) {
            string line;
            while (getline(file, line)) {
                //每一行是一个文件名，将这些文件更新到表里
                string filepath = line;
                // cout << "filepath " << filepath << endl;
                if (filesystem::exists(filepath)) {
                    csvFiles.push_back(filepath);
                } else {
                    cout << line << " does not exist in file system." << endl;
                }
            }
            file.close();
        } else {
            cout << "Failed to open file." << endl;
        }

        for (const string& csvFile : csvFiles) {
            std::string arg1 = gene_marker_path;
            std::string arg2 = csvFile;
            // std::string arg3 = std::to_string(8);
            std::string arg3 = index_marker_length;
            std::string fullCommand = python_command + " " + python_script_path + " -i " + arg1 + " -d " + arg2 + " -n " + arg3;
            int result = std::system(fullCommand.c_str());
            if (result == 0) {
                std::cout << "Update index executed successfully." << std::endl;
            } else {
                std::cout << "Update index execution failed." << std::endl;
            }
            rapidcsv::Document gene_marker_csv = rapidcsv::Document(gene_marker_path, rapidcsv::LabelParams(0, -1));

            map<string, vector<string>> indexDict;

            for (const auto &indexName : gene_marker_csv.GetColumnNames()) {
                vector<string> gene_marker = gene_marker_csv.GetColumn<string>(indexName);
                indexDict[indexName] = gene_marker;
            }

            cout << "File: " << csvFile << " is processing at " << getCurrentTime() << endl;
            process_csv_file(csvFile, indexDict);
            cout << "File: " << csvFile << " is processed at " << getCurrentTime() << endl;
        }

        end();
    }

    void sc_search(const char* query_csv_path, int k, float cutoff, const char* output_result, const char* iniPath, const float alpha) {
        string query(query_csv_path);
        string output(output_result);
        init(iniPath);

        json j;

        rapidcsv::Document gene_marker_csv = rapidcsv::Document(gene_marker_path, rapidcsv::LabelParams(0, -1));
        //用来query的csv，行名是cellname，列名是genename
        rapidcsv::Document query_csv = rapidcsv::Document(query, rapidcsv::LabelParams(0, 0));

//        vector<thread> threads;
        ThreadPool pool(100);

        auto start_time = std::chrono::steady_clock::now();
        cout << "总搜索开始" << getCurrentTime() << endl;

        for (const auto &cellName : query_csv.GetRowNames()) {
//            threads.emplace_back(process_cell, ref(query_csv), ref(gene_marker_csv), ref(j), cellName, k, cutoff);
            pool.enqueue(process_cell, ref(query_csv), ref(gene_marker_csv), ref(j), cellName, k, cutoff, alpha);
//            process_cell(query_csv, gene_marker_csv, j, cellName, k, cutoff);
        }

        cout << "所有提交结束" << getCurrentTime() << endl;

        // 等待所有线程执行完毕
//        for (auto& thread : threads) {
//            thread.join();
//        }
        pool.wait();

        cout << "总搜索结束" << getCurrentTime() << endl;

        auto end_time = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

        std::cout << "总搜索用时" << duration.count() / 1000.0 << "秒" << std::endl;

        ofstream file(output);
        file << j.dump();
        file.close();

        end();
    }

    // void sc_st_search(const char* query_csv_path, int k, const char* output_result, const char* iniPath) {
    //     string query(query_csv_path);
    //     string output(output_result);
    //     init(iniPath);

    //     json j;

    //     rapidcsv::Document gene_marker_csv = rapidcsv::Document(gene_marker_path, rapidcsv::LabelParams(0, -1));
    //     //用来query的csv，行名是cellname，列名是genename
    //     rapidcsv::Document query_csv = rapidcsv::Document(query, rapidcsv::LabelParams(0, 0));

    //     int indexNum = gene_marker_csv.GetColumnCount();

    //     //TODO 可以多线程(每个cell一个线程)
    //     for (const auto &cellName : query_csv.GetRowNames()) {
    //         unordered_map<string, int> linecount;
    //         int indexCount = 0;
    //         indexCount = search_linecount_for_a_cell(query_csv, gene_marker_csv, linecount, cellName);
    //         cout << indexCount << endl;

    //         //取出每个index都与之匹配的cell list(即出现次数等于index个数的line)
    //         vector<string> cell_list = find_k_count_line(indexNum, linecount);
    //         j[cellName] = cell_list;
    //     }

    //     ofstream file(output);
    //     file << j.dump();
    //     file.close();

    //     end();
    // }

    // void st_search(const char* query_csv_path, const char* output_result, const char* iniPath) {
    //     string query(query_csv_path);
    //     string output(output_result);
    //     init(iniPath);

    //     json j;

    //     rapidcsv::Document gene_marker_csv = rapidcsv::Document(gene_marker_path, rapidcsv::LabelParams(0, -1));
    //     //用来query的csv，行名是cellname，列名是genename
    //     rapidcsv::Document query_csv = rapidcsv::Document(query, rapidcsv::LabelParams(0, 0));

    //     //TODO 可以多线程(每个cell一个线程)
    //     for (const auto &cellName : query_csv.GetRowNames()) {
    //         unordered_map<string, int> linecount;
    //         int indexCount = 0;
    //         indexCount = search_linecount_for_a_cell(query_csv, gene_marker_csv, linecount, cellName);
    //         cout << indexCount << endl;

    //         //取出每个index都与之匹配的cell list(即出现次数等于index个数的line)
    //         vector<string> cell_list = extract_all_string(linecount);
    //         j[cellName] = cell_list;
    //     }

    //     ofstream file(output);
    //     file << j.dump();
    //     file.close();

    //     end();
    // }
}

int main() {
    create_table("/mnt/c/Users/research/Downloads/sc_search/test1/create_table.txt", "/mnt/c/Users/research/Downloads/sc_search/test1/sc_config.ini");
    // sc_search("/mnt/c/Users/research/Downloads/sc_search/test_1_cll.csv", 500, 0, "/mnt/c/Users/research/Downloads/sc_search/output1.json", "/mnt/c/Users/research/Downloads/sc_search/sc_config.ini");
    // create_table("/Users/zhizhenzhou/zzz/Postgraduate/Guided Study/demo/create_table_sc_st.txt", "/Users/zhizhenzhou/zzz/Postgraduate/Guided Study/demo/djangoDemo/config/sc_st_config.ini");
    // rapidcsv::Document gene_marker_csv = rapidcsv::Document(gene_marker_path, rapidcsv::LabelParams(0, -1));
    // for (const auto &indexName : gene_marker_csv.GetColumnNames()) {
    //     cout << indexName << endl;
    // }
}
