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
       char q[200];
       vector<vector<string> > res;
       p = new mysqlpool(200, 10000, 10000);
       for (i=0; i<500000; i++) {
           sprintf(q, "insert into tt (a, b, c) values ('hello%d',%d,NULL)",i,i);
           res = p->run_query("localhost", "mysql", "", "test", 3306, q); 
           if ((i % 5000) == 0) {
               res = p->run_query("localhost", "mysql", "", "test", 3306, "commit");
           }
       }
}

