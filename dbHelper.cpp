#include <iostream>
#include <sstream>
#include <filesystem>
#include <pqxx/pqxx>
#include "dbHelper.hpp"
#include "linktable.hpp"
// #include "SimpleIni.h"
#include "txtHelper.hpp"
#include <unordered_map>

using namespace std;
using namespace pqxx;

DBHelper::DBHelper() : conn(nullptr), txn(nullptr) {};

DBHelper::~DBHelper() {
    if (txn != nullptr) {
        delete txn;
    }
    if (conn != nullptr) {
        delete conn;
    }
}

void DBHelper::setHost(const string &host) {
    dbhost = host;
}

void DBHelper::setPort(const string &port) {
    dbport = port;
}

void DBHelper::setName(const string &name) {
    dbname = name;
}

void DBHelper::setUser(const string &user) {
    dbuser = user;
}

void DBHelper::setPassword(const string &password) {
    dbpassword = password;
}

string DBHelper::getHost() {
    return dbhost;
}

string DBHelper::getPort() {
    return dbport;
}

string DBHelper::getName() {
    return dbname;
}

string DBHelper::getUser() {
    return dbuser;
}

string DBHelper::getPassword() {
    return dbpassword;
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
        cout << "A connection has been established. Please do not start a connection again." << endl;
    } else {
        if (!dbname.empty() && !dbuser.empty() && !dbpassword.empty() && !dbhost.empty() && !dbport.empty()) {
            stringstream conn_str;
            conn_str << "dbname=" << DBHelper::dbname << " user=" << DBHelper::dbuser << " password=" << DBHelper::dbpassword << " hostaddr=" << DBHelper::dbhost << " port=" << DBHelper::dbport;
            conn = new connection(conn_str.str());

            if (isConnected()) {
                // cout << "Opened database successfully: " << conn->dbname() << endl;
            } else {
                cout << "Can't open database" << endl;
            }
        } else {
            cout << "Please set up all configurations for the connection!" << endl;
        }
    }
}

void DBHelper::closeConnectionDB() {
    if (isConnected()) {
        conn->disconnect();
        // conn->close();
        // cout << "Connection is closed." << endl;
    } else {
        cout << "Please connect to a DB before close it!" << endl;
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
            cout << "A work has been started, please not start a work again." << endl;
            return;
        }
        if (txn) {
            delete txn;
        }
        txn = new work(*conn);
        // cout << "A work is started." << endl;
        return;
    } else {
        cout << "Please connect to DB before start a work!" << endl;
    }
}

void DBHelper::workCommit() {
    if (isWorkStarted()) {
        txn->commit();
        txn = nullptr;
        // cout << "A work is commited." << endl;
        return;
    } else {
        cout << "Please start a work first!" << endl;
        return;
    }
}

bool DBHelper::check_table_exist(const string &table_name) {
    try {
        if (isConnected()) {
            workStart();

            string check = "SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = '" + table_name + "');";

            result res = txn->exec(check);

            bool table_exists = res[0][0].as<bool>();

            workCommit();

            return table_exists;
        } else {
            cout << "You have not connected to a DB yet!" << endl;
            return false;
        }
    } catch (const exception &e) {
        cerr << e.what() << endl;
        throw e;
    }    
}

bool DBHelper::check_in_table(const string & table_name, const string &index) {
    //查看一个index是否在表里
    try {
        if (isConnected()) {
            workStart();

            result res = txn->exec("SELECT EXISTS (SELECT 1 FROM " + table_name + " WHERE index='" + index + "')");

            bool exists = res[0][0].as<bool>();

            // if (exists) {
            //     cout << index << " exists in table " << table_name << "." << endl;
            // } else {
            //     cout << index << " not exists in table " << table_name << "." << endl;
            // }

            workCommit();

            return exists;
        } else {
            cout << "You have not connected to a DB yet!" << endl;
            return false;
        }
    } catch (const exception &e) {
        cerr << e.what() << endl;
        throw e;
    }
}

void DBHelper::print_table(const string &table_name) {
    try {
        if (isConnected()) {
            workStart();

            stringstream print;
            print << "SELECT * FROM " << table_name;
            result res = txn->exec(print.str());

            for (auto row : res) {
                stringstream output;
                for (auto field : row) {
                    output << field.c_str() << " ";
                }
                cout << output.str() << endl;
            }

            workCommit();

            cout << "Table printed successfully" << endl;
        } else {
            cout << "You have not connected to a DB yet!" << endl;
            return;
        }
    } catch (const exception &e) {
        cerr << e.what() << endl;
        throw e;
    }
}

void DBHelper::create_table(const string &table_name) {
    try {
        if (isConnected()) {
            workStart();

            //创建表的SQL语句
            stringstream create;
            create << "CREATE TABLE IF NOT EXISTS " << table_name << " ("
                << "id SERIAL PRIMARY KEY, "
                << "index VARCHAR(50), "
                << "decimal_index INTEGER, "
                << "num INTEGER, "
                << "address VARCHAR(250), "
                << "pre_id INTEGER, "
                << "next_id INTEGER)";

            txn->exec(create.str());

            workCommit();

            cout << "Table created successfully" << endl;
        } else {
            cout << "You have not connected to a DB yet!" << endl;
            return;
        }
    } catch (const exception &e) {
        cerr << e.what() << endl;
        throw e;
    }
}

pair<int, int> DBHelper::find_pre_next_nodes(const string &table_name, int query_index) {
    try {
        if (isConnected()) {
            workStart();

            // query_index = node.decimal_index;
            int pre_node_id;
            int next_node_id;

            stringstream query1;
            query1 << "SELECT id FROM " << table_name << " WHERE decimal_index < " << query_index << " ORDER BY decimal_index DESC LIMIT 1";
            result R1 = txn->exec(query1.str());

            stringstream query2;
            query2 << "SELECT id FROM " << table_name << " WHERE decimal_index > " << query_index << " ORDER BY decimal_index ASC LIMIT 1";
            result R2 = txn->exec(query2.str());

            if (R1.size() == 0) {
                pre_node_id = -1;
            } else {
                for (auto row: R1) {
                    pre_node_id = row[0].as<int>();
                }
            }

            if (R2.size() == 0) {
                next_node_id = -1;
            } else {
                for (auto row: R2) {
                    next_node_id = row[0].as<int>();
                }
            }

            workCommit();

            // cout << "Find prenode and nextnode successfully" << endl;

            return make_pair(pre_node_id, next_node_id);
        } else {
            cout << "You have not connected to a DB yet!" << endl;
            return make_pair(-1, -1);
        }
    } catch (const exception &e) {
        cerr << e.what() << endl;
        throw e;
    }
}

string DBHelper::search_address(const string &table_name, int query_index) {
    try {
        if (isConnected()) {
            workStart();

            //双链表查询，找到对应节点的address
            stringstream search;
            search << "SELECT address FROM " << table_name << " WHERE decimal_index = " << query_index;
            result res = txn->exec(search.str());

            string address = "";

            if (res.size() == 0) {
                return address;
            } else {
                for (auto row : res) {
                    address = row[0].as<string>();
                    // cout << "Find address " << address << "." << endl;
                }
            }

            workCommit();

            return address;
        } else {
            cout << "You have not connected to a DB yet!" << endl;
            return "";
        }
    } catch (const exception &e) {
        cerr << e.what() << endl;
        throw e;
    }
}

void DBHelper::insert_item(const string &table_name, Model &node, const string &fromFilePath, const string& fromFileRow, const string& toFolder, TxtHelper& txtHelper) {
    try {
        if (isConnected()) {
            // workStart();
            // TxtHelper txtHelper;

            if (check_in_table(table_name, node.getIndex())) {
                workStart();
                //如果要插入的index已经在这个表里面
                //对应index的num+1
                txn->exec("UPDATE " + table_name + " SET num = num + 1 WHERE index = '" + node.getIndex() + "'");
                //TODO 把这个node对应的数据放入这个index对应的地址文件里
                //读取文件address
                //往文件的最后一行写入fromfilepath和行数
                stringstream search_address;
                search_address << "SELECT address FROM " << table_name << " WHERE decimal_index = " << node.getDecIndex();
                result res = txn->exec(search_address.str());
                string index_address = "";
                if (res.size() == 0) {
                    cout << "建表时没有存入文件地址" << endl;
                } else {
                    for (auto row : res) {
                        string index_address = row[0].as<string>();

                        if (std::filesystem::exists(index_address)) {
                            txtHelper.writeASCLine(index_address, make_pair(fromFilePath, fromFileRow));
                        } else {
                            cout << "表中存的文件地址不存在" << endl;
                        }
                    }
                }

                workCommit();

                // cout << "Insert successfully" << endl;
            } else {
                //如果要插入的index不在这个表里面
                //插入之前先要将index转成十进制decimal_index
                //先根据Node的decimal_index找到他的pre和next节点
                auto myPair = find_pre_next_nodes(table_name, node.getDecIndex());
                int pre_node_id = myPair.first;
                int next_node_id = myPair.second;

                //TO BE CONTINUE 此时是这个index第一次插入表中，我们要生成该index对应的文件地址
                //TODO 并把这个node对应的数据放入这个index对应的地址里
                // CSimpleIni ini;
                // ini.LoadFile("config.ini");
                // string index_folder = ini.GetValue("filepath", "index_folder", "");
                stringstream index_address;
                index_address << toFolder << table_name << "/" << node.getIndex() << ".txt";
                node.setAddress(index_address.str());

                workStart();

                txtHelper.createPath(node.getAddress());
                txtHelper.writeASCLine(node.getAddress(), make_pair(fromFilePath, fromFileRow));

                if (pre_node_id != -1 && next_node_id != -1) {
                    //如果前后节点都存在
                    // cout << "前后节点都存在" << endl;
                    stringstream insert;
                    insert << "INSERT INTO " << table_name << " (index, decimal_index, num, address, pre_id, next_id) VALUES ('" << node.getIndex() << "', " << node.getDecIndex() << ", " << 1 << ", '" << node.getAddress() << "', " << pre_node_id << ", " << next_node_id  << ") RETURNING id";
                    result id_res = txn->exec(insert.str());
                    int new_id = id_res[0][0].as<int>();
                    // cout << "New id is " << new_id << endl;

                    stringstream update_pre;
                    update_pre << "UPDATE " << table_name << " SET next_id = " << new_id << " WHERE id = " << pre_node_id;
                    txn->exec(update_pre.str());

                    stringstream update_next;
                    update_next << "UPDATE " << table_name << " SET pre_id = " << new_id << " WHERE id = " << next_node_id;
                    txn->exec(update_next.str());

                    workCommit();

                    // cout << "Insert successfully" << endl;
                } else if (pre_node_id != -1 && next_node_id == -1) {
                    //如果前节点存在，后节点不存在。那么这个节点是最后一个节点
                    // cout << "前节点存在，后节点不存在。那么这个节点是最后一个节点" << endl;
                    stringstream insert;
                    insert << "INSERT INTO " << table_name << " (index, decimal_index, num, address, pre_id, next_id) VALUES ('" << node.getIndex() << "', " << node.getDecIndex() << ", " << 1 << ", '" << node.getAddress() << "', " << pre_node_id << ", NULL) RETURNING id";
                    result id_res = txn->exec(insert.str());
                    int new_id = id_res[0][0].as<int>();
                    // cout << "New id is " << new_id << endl;

                    stringstream update_pre;
                    update_pre << "UPDATE " << table_name << " SET next_id = " << new_id << " WHERE id = " << pre_node_id;

                    txn->exec(update_pre.str());

                    workCommit();

                    // cout << "Insert successfully" << endl;

                } else if (pre_node_id == -1 && next_node_id != -1) {
                    //如果前节点不存在，后节点存在。那么这个节点是第一个节点
                    // cout << "前节点不存在，后节点存在。那么这个节点是第一个节点" << endl;
                    stringstream insert;
                    insert << "INSERT INTO " << table_name << " (index, decimal_index, num, address, pre_id, next_id) VALUES ('" << node.getIndex() << "', " << node.getDecIndex() << ", " << 1 << ", '" << node.getAddress() << "', NULL, " << next_node_id << ") RETURNING id";
                    result id_res = txn->exec(insert.str());
                    int new_id = id_res[0][0].as<int>();
                    // cout << "New id is " << new_id << endl;

                    stringstream update_next;
                    update_next << "UPDATE " << table_name << " SET pre_id = " << new_id << " WHERE id = " << next_node_id;
                    txn->exec(update_next.str());

                    workCommit();

                    // cout << "Insert successfully" << endl;

                } else if (pre_node_id == -1 && next_node_id == -1) {
                    //前后节点都不存在。表里只有这一个节点
                    // cout << "前后节点都不存在。表里只有这一个节点" << endl;
                    stringstream insert;
                    insert << "INSERT INTO " << table_name << " (index, decimal_index, num, address, pre_id, next_id) VALUES ('" << node.getIndex() << "', " << node.getDecIndex() << ", " << 1 << ", '" << node.getAddress() << "', NULL, NULL)";
                    txn->exec(insert.str());

                    workCommit();

                    // cout << "Insert successfully" << endl;
                }
            }
        } else {
            cout << "You have not connected to a DB yet!" << endl;
            return;
        }
    } catch (const exception &e) {
        cerr << e.what() << endl;
        throw e;
    }
}

void DBHelper::insert_items(unordered_map<string, vector<pair<string, Model> > >& ModelDict, const string &fromFilePath, const string& toFolder) {
    try {
        if (isConnected()) {
            TxtHelper txtHelper;
            for (auto& record : ModelDict) {
                string indexName = record.first;
                vector<pair<string, Model> >& dataList = record.second;

                if (!dataList.empty()) {
                    if (!check_table_exist(indexName)) {
                        create_table(indexName);
                    }

                    for (auto& data : dataList) {
                        string& cellname = data.first;
                        Model& node = data.second;

                        insert_item(indexName, node, fromFilePath, cellname, toFolder, txtHelper);
                    }
                }
            }
            txtHelper.flushDataToFile();
        } else {
            cout << "You have not connected to a DB yet!" << endl;
            return;
        }
    } catch (const exception &e) {
        cerr << e.what() << endl;
        throw e;
    }
}

void DBHelper::delete_item(int delete_index) {

}

void DBHelper::drop_table(const string &table_name) {
    try {
        if (isConnected()) {
            workStart();

            stringstream drop;
            drop << "DROP TABLE IF EXISTS " << table_name;
            txn->exec(drop.str());

            workCommit();

            cout << "Table dropped successfully" << endl;
        } else {
            cout << "You have not connected to a DB yet!" << endl;
            return;
        }
    } catch (const exception &e) {
        cerr << e.what() << endl;
        throw e;
    }
}

void DBHelper::insert_address(const string &table_name, int query_index, const string &address) {
    try {
        if (isConnected()) {
            workStart();

            stringstream insert_address;
            // "UPDATE " + table_name + " SET num = num + 1 WHERE index = '" + node.getIndex() + "'"
            insert_address << "UPDATE " << table_name << " SET address = '" << address << "' WHERE decimal_index = " << query_index;
            txn->exec(insert_address.str());

            workCommit();

            cout << "Insert address successfully" << endl;
        } else {
            cout << "You have not connected to a DB yet!" << endl;
            return;
        }
    } catch (const exception &e) {
        cerr << e.what() << endl;
        throw e;
    }
}
