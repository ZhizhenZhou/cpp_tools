#ifndef DB_HELPER_HPP
#define DB_HELPER_HPP

#include <iostream>
#include <sstream>
#include <pqxx/pqxx>
#include "linktable.hpp"
#include "txtHelper.hpp"
#include <unordered_map>



using namespace std;
using namespace pqxx;


class DBHelper {
    public:
        DBHelper();
        ~DBHelper();
        void setHost(const string &host);
        void setPort(const string &port);
        void setName(const string &name);
        void setUser(const string &user);
        void setPassword(const string &password);
        string getHost();
        string getPort();
        string getName();
        string getUser();
        string getPassword();
        //检查是否连接到数据库
        bool isConnected();
        //开启一个PG数据库连接
        void connectionDB();
        //关闭数据库连接
        void closeConnectionDB();
        //检查是否开启了事务work
        bool isWorkStarted();
        //开启事务work
        void workStart();
        //关闭事务work
        void workCommit();
        //检查一个表是否在数据库里
        bool check_table_exist(const string &table_name);
        //查看一个index是否在表里
        bool check_in_table(const string & table_name, const string &index);
        //打印表
        void print_table(const string &table_name);
        //创建表
        void create_table(const string &table_name);
        //根据index转换成的十进制值，查询这个双链表中其前后的节点的id
        pair<int, int> find_pre_next_nodes(const string &table_name, int query_index);
        //通过index搜索一个节点，返回存放的sc的地址
        string search_address(const string &table_name, int query_index);
        //插入一个节点
        void insert_item(const string &table_name, Model &node, const string &fromFilePath, const string& fromFileRow, const string& toFolder, TxtHelper& txtHelper);
        void insert_items(unordered_map<string, vector<pair<string, Model> > >& ModelDict, const string &fromFilePath, const string& toFolder);
        //删除一个节点
        void delete_item(int delete_index);
        //删除一张表
        void drop_table(const string &table_name);

        void insert_address(const string &table_name, int query_index, const string &address);

    private:
        string dbhost;
        string dbport;
        string dbname;
        string dbuser;
        string dbpassword;

        pqxx::connection* conn;
        pqxx::work* txn;

};

#endif