/* MySQLPoolCli0.cpp

   This program tests a client connection with mysql.
   
   Ver:  1.0, D. Graham, 8/18/2009
*/

#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <mysql.h>
#include <assert.h>

using namespace std;

#include <iostream>
#include <vector>

int main (int argc, char *argv[]) {

       int ret;
       MYSQL *db;
       MYSQL_RES *res;
       MYSQL_ROW row;
       unsigned int i, j;
       unsigned int num_fields;
       vector<string> str_data;
       string str_item;
       db = new MYSQL;
       mysql_init(db);
       mysql_real_connect(db, "localhost", "mysql", "", "test",
                          3306, NULL, 0);
       ret = mysql_query(db, "insert into tt (a,b,c) values ('hello',1,NULL)");
       if (ret) {
          perror("Failed query:");
          return(-1);
       } 
       res = mysql_store_result(db);
       if (res == NULL) {
           exit(1);
       }
       num_fields = mysql_num_fields(res);
       cout << "NUMFIELDS: " << num_fields << "\n";
       i = 0;
       while (row = mysql_fetch_row(res)) {
        str_item = "";
        for (j = 0; j < num_fields; j++) {
            cout << "RES : " << row[0] << "\n";
          if (row[j] == NULL) {
                str_item.append("NULL");
                str_item.append("\0");
            } else { 
                str_item.append(row[j]);
                str_item.append("\0"); 
            } 
        }
        str_data.push_back(str_item); 
        cout << "RESULT: " << str_item << "\n";
    }
       mysql_free_result(res);
     for (i = 0; i < str_data.size(); i++) {
         cout << str_data[i] << "\n";
    }
}


  
