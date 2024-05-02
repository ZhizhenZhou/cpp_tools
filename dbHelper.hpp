#ifndef DB_HELPER_HPP
#define DB_HELPER_HPP

#include <iostream>
#include <fstream>
#include <sstream>
#include <pqxx/pqxx>
#include <unordered_map>

class DBHelper {
    public:
        DBHelper();
        ~DBHelper();
        void setHost(const std::string& host);
        void setPort(const std::string& port);
        void setName(const std::string& name);
        void setUser(const std::string& user);
        void setPassword(const std::string& password);
        void setSQLScriptPath(const std::string& sqlPath);
        std::string getHost();
        std::string getPort();
        std::string getName();
        std::string getUser();
        std::string getPassword();
        std::string getSQLScriptPath();
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
        //搜索
        void query(const std::unordered_map<std::string, std::vector<std::string> >& queryindexs, std::unordered_map<std::string, int>& CellIndexCount);
        //插入数据（包含创建表）
        void insert_table_item(const std::string& table_name, const std::string& cellname, const std::string& filepath, const std::vector<std::string>& column_list);
        void insert_table_items(const std::unordered_map<std::string, std::unordered_map<std::string, std::vector<std::string> > >& ModelDict, const std::string& filepath);



    private:
        std::string dbhost;
        std::string dbport;
        std::string dbname;
        std::string dbuser;
        std::string dbpassword;
        std::string dbSQLScriptPath;

        pqxx::connection* conn;
        pqxx::work* txn;

        void injectSqlFunctions(const std::string& sqlFile);
        std::string construct_sql_query(const std::unordered_map<std::string, std::vector<std::string> >& queryindexs);

};

#endif