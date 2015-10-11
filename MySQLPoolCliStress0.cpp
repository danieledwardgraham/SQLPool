/* MySQLPoolCliStress0.cpp

   This program tests a non-pooling client connection with mysql.
   
   Ver:  1.0, D. Graham, 8/18/2009
*/

#include <stdlib.h>
#include <sys/timeb.h>
#include <mysql.h>
#include <iostream>
#include <vector>
#include <string>

using namespace std;

int main (int argc, char *argv[]) {

       char q[200];
       char resc[100];
       vector<vector<string> > resv;
       vector<string> subres;
       int ret;
       MYSQL *db;
       MYSQL_RES *res;
       MYSQL_ROW row;
       unsigned int i, j, rn;
       unsigned int num_fields;
       vector<string> str_data;
       timeb tp; 
       ftime(&tp);
       srand((unsigned int) tp.millitm);
       string str_item;
       rn = rand();
       rn = rn % 500000;
       db = new MYSQL;
       mysql_init(db);
       mysql_real_connect(db, "localhost", "mysql", "", "test",
                          3306, NULL, 0);
       sprintf(q, "select a,b,c from tt where b=%d", rn);
       ret = mysql_query(db, q);
       if (ret < 0) {
          perror("Error query:");
          return(-1);
       } 
       res = mysql_store_result(db);
       if (res == NULL) {
           cerr << "Failed query with " << rn << "\n";
           exit(1);
       }
       num_fields = mysql_num_fields(res);
       i = 0;
       while (row = mysql_fetch_row(res)) {
        resv.push_back(subres);
        for (j = 0; j < num_fields; j++) {
          if (row[j] == NULL) {
                resv[i].push_back("NULL");
            } else { 
                resv[i].push_back(row[j]);
            } 
        }
        i++;
      }
      mysql_free_result(res);
       for (j = 0; j<resv[0][1].length(); j++) {
           resc[j] = resv[0][1][j];
       } 
       resc[j] = '\0';
       if (atoi(resc) != rn) {
           cerr << "Failed query with " << rn << "\n";
       } 
}

