#include <iostream>
#include <fstream>
#include <sstream>
#include <pqxx/pqxx>
#include "dbHelper.hpp"
#include <unordered_map>

DBHelper::DBHelper() : conn(nullptr), txn(nullptr) {};

DBHelper::~DBHelper() {
    if (txn != nullptr) {
        delete txn;
    }
    if (conn != nullptr) {
        delete conn;
    }
}

void DBHelper::setHost(const std::string& host) {
    dbhost = host;
}

void DBHelper::setPort(const std::string& port) {
    dbport = port;
}

void DBHelper::setName(const std::string& name) {
    dbname = name;
}

void DBHelper::setUser(const std::string& user) {
    dbuser = user;
}

void DBHelper::setPassword(const std::string& password) {
    dbpassword = password;
}

void DBHelper::setSQLScriptPath(const std::string& sqlPath) {
    dbSQLScriptPath = sqlPath;
}

std::string DBHelper::getHost() {
    return dbhost;
}

std::string DBHelper::getPort() {
    return dbport;
}

std::string DBHelper::getName() {
    return dbname;
}

std::string DBHelper::getUser() {
    return dbuser;
}

std::string DBHelper::getPassword() {
    return dbpassword;
}

std::string DBHelper::getSQLScriptPath() {
    return dbSQLScriptPath;
}

bool DBHelper::isConnected() {
    if (conn != nullptr && conn->is_open()) {
        return true;
    } else {
        return false;
    }
}

void DBHelper::connectionDB() {
    if (isConnected()) {
        std::cout << "A connection has been established. Please do not start a connection again." << std::endl;
    } else {
        if (!dbname.empty() && !dbuser.empty() && !dbpassword.empty() && !dbhost.empty() && !dbport.empty() && !dbSQLScriptPath.empty()) {
            std::stringstream conn_str;
            conn_str << "dbname=" << DBHelper::dbname << " user=" << DBHelper::dbuser << " password=" << DBHelper::dbpassword << " hostaddr=" << DBHelper::dbhost << " port=" << DBHelper::dbport;
            conn = new pqxx::connection(conn_str.str());

            if (isConnected()) {
                injectSqlFunctions(dbSQLScriptPath);
                // cout << "Opened database successfully: " << conn->dbname() << endl;
            } else {
                std::cout << "Can't open database" << std::endl;
            }
        } else {
            std::cout << "Please set up all configurations for the connection!" << std::endl;
        }
    }
}

void DBHelper::closeConnectionDB() {
    if (isConnected()) {
        conn->disconnect();
        // conn->close();
        // cout << "Connection is closed." << endl;
    } else {
        std::cout << "Please connect to a DB before close it!" << std::endl;
    }
}

bool DBHelper::isWorkStarted() {
    if (txn && txn->conn().is_open()) {
        return true;
    } else {
        return false;
    }
}

void DBHelper::workStart() {
    if (isConnected()) {
        if (isWorkStarted()) {
            std::cout << "A work has been started, please not start a work again." << std::endl;
            return;
        }
        if (txn) {
            delete txn;
        }
        txn = new pqxx::work(*conn);
        // cout << "A work is started." << endl;
        return;
    } else {
        std::cout << "Please connect to DB before start a work!" << std::endl;
    }
}

void DBHelper::workCommit() {
    if (isWorkStarted()) {
        txn->commit();
        txn = nullptr;
        // cout << "A work is commited." << endl;
        return;
    } else {
        std::cout << "Please start a work first!" << std::endl;
        return;
    }
}

std::string DBHelper::construct_sql_query(const std::unordered_map<std::string, std::vector<std::string> >& queryindexs) {
    std::stringstream sql_query;
    sql_query << "SELECT cellname, filepath, total_count FROM process_json_data('{";
    std::string separator_indexcode = "";
    for (const auto& table : queryindexs) {
        const std::string& tablename = "indexcode_" + table.first;
        sql_query << separator_indexcode << "\"" << tablename << "\": [";
        separator_indexcode = ", ";
        std::string separator_indexname = "";
        const std::vector<std::string>& indexnames = table.second;
        for (const auto& indexname : indexnames) {
            sql_query << separator_indexname << "\"" << indexname << "\"";
            separator_indexname = ", ";
        }
        sql_query << "]";
    }
    sql_query << "}');";

    // std::cout << sql_query.str() << std::endl;
    return sql_query.str();
}

void DBHelper::query(const std::unordered_map<std::string, std::vector<std::string> >& queryindexs, std::unordered_map<std::string, int>& CellIndexCount) {
    try {
        if (isConnected()) {
            std::string sql_query = construct_sql_query(queryindexs);
            workStart();
            pqxx::result res = txn->exec(sql_query);

            for (const auto& row : res) {
                std::string cellname = row["cellname"].as<std::string>();
                std::string filepath = row["filepath"].as<std::string>();
                int count = row["total_count"].as<int>();
                std::string cellnameWithFilepath = filepath + "," + cellname;

                CellIndexCount[cellnameWithFilepath] = count;
            }
            workCommit();
        } else {
            std::cout << "You have not connected to a DB yet!" << std::endl;
            return;
        }
    } catch (const std::exception &e) {
        std::cerr << e.what() << std::endl;
        throw e;
    }
}

void DBHelper::insert_table_item(const std::string& table_name, const std::string& cellname, const std::string& filepath, const std::vector<std::string>& column_list) {
    try {
        if (isConnected()) {
            workStart();
            std::stringstream insert;
            std::string separator = "";
            insert << "SELECT update_table_columns('" << table_name << "', ARRAY[";

            for (auto& indexname : column_list) {
                insert << separator << "'" << indexname << "'";
                separator = ", ";
            }
            insert << "], '" << cellname << "', '" << filepath << "')";
            // std::cout << insert.str() << std::endl;
            pqxx::result res = txn->exec(insert.str());
            workCommit();
        } else {
            std::cout << "You have not connected to a DB yet!" << std::endl;
            return;
        }
    } catch (const std::exception &e) {
        std::cerr << e.what() << std::endl;
        throw e;
    }
}

void DBHelper::insert_table_items(const std::unordered_map<std::string, std::unordered_map<std::string, std::vector<std::string> > >& indextables, const std::string& filepath) {
    for (const auto& table : indextables) {
        const std::string& tablename = "indexcode_" + table.first;
        const std::unordered_map<std::string, std::vector<std::string> >& rows_data = table.second;

        for (const auto& row_data : rows_data) {
            const std::string& cellname = row_data.first;
            const std::vector<std::string>& index_cols = row_data.second;

            insert_table_item(tablename, cellname, filepath, index_cols);
        }
        // std::cout << tablename << "finished" << std::endl;
    }
}

void DBHelper::injectSqlFunctions(const std::string& sqlFile) {
    try {
        if (isConnected()) {
            std::ifstream file(sqlFile);
            if (!file.is_open()) {
                throw std::runtime_error("Failed to open SQL file: " + sqlFile + ".");
            }

            std::stringstream buffer;
            buffer << file.rdbuf();

            workStart();
            txn->exec(buffer.str());
            // txn.exec(buffer.str());
            workCommit();
            std::cout << "SQL is injected." << std::endl;
        }
    } catch (const std::exception &e) {
        std::cerr << e.what() << std::endl;
        throw e;
    }
}
