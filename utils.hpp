#ifndef UTILS_HPP
#define UTILS_HPP

#include <string>
#include <time.h>
#include <chrono>


// inline int INDEX_LENGTH = 0;

// inline void init(const char* iniPath) {
//     std::string gene_marker_path;
//     std::string index_folder;
//     std::string index_marker_length;

//     std::string python_command;
//     std::string python_script_path;
//     std::string dbhost;
//     std::string dbport;
//     std::string dbname;
//     std::string dbuser;
//     std::string dbpassword;

//     CSimpleIni ini;
//     ini.LoadFile(iniPath);

//     dbhost = ini.GetValue("database", "host", "");
//     dbport = ini.GetValue("database", "port", "");
//     dbname = ini.GetValue("database", "name", "");
//     dbuser = ini.GetValue("database", "user", "");
//     dbpassword = ini.GetValue("database", "password", "");

//     gene_marker_path = ini.GetValue("filepath", "marker_gene_path", "");
//     index_folder = ini.GetValue("filepath", "index_folder", "");
//     index_marker_length = ini.GetValue("filepath", "index_marker_length", "0");

//     python_command = ini.GetValue("pythonscript", "command", "");
//     python_script_path = ini.GetValue("pythonscript", "script_path", "");

//     dbHelper.setHost(dbhost);
//     dbHelper.setPort(dbport);
//     dbHelper.setName(dbname);
//     dbHelper.setUser(dbuser);
//     dbHelper.setPassword(dbpassword);
// }

inline std::string getCurrentTime() {
    time_t curtime;
 
    time(&curtime);

    return std::string(ctime(&curtime));
}


#endif