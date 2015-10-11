/* MySQLPoolCli.cpp

   This program tests a client connection with mysqlpool.
   
   Ver:  1.0, D. Graham, 8/18/2009
*/

#include "MySQLPoolCli.h"
#include <unistd.h>

using namespace std;

int main (int argc, char *argv[]) {

       mysqlpool *p;
       int i, j;
       vector<vector<string> > res;
       p = new mysqlpool(200, 100, 10000);
       res = p->run_query("localhost", "mysql", "", "test", 3306, "select * from tt where a='hello2'"); 
       for (i = 0; i < res.size(); i++) {
           for (j = 0; j < res[i].size(); j++) {
              cout << res[i][j] << "|"; 
           }
           cout << "\n";
       }
       res = p->run_query("localhost", "mysql", "", "test", 3306, "select * from tt where a='hello3'"); 
       for (i = 0; i < res.size(); i++) {
           for (j = 0; j < res[i].size(); j++) {
              cout << res[i][j] << "|"; 
           }
           cout << "\n";
       }
       res = p->run_query("localhost", "mysql", "", "test", 3306, "select * from tt where a='hello4'"); 
       for (i = 0; i < res.size(); i++) {
           for (j = 0; j < res[i].size(); j++) {
              cout << res[i][j] << "|"; 
           }
           cout << "\n";
       }
}


  
