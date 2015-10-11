/* MySQLPoolCli.cpp

   This program tests a client connection with mysqlpool.
   
   Ver:  1.0, D. Graham, 8/18/2009
*/

#include "MySQLPoolCli.hh"
#include <stdlib.h>
#include <sys/timeb.h>

using namespace std;

int main (int argc, char *argv[]) {

       timeb tp;
       mysqlpool *p;
       unsigned int i, j;
       char q[200];
       char resc[100];
       vector<vector<string> > res;
       ftime(&tp);
       srand((unsigned int) tp.millitm);
       p = new mysqlpool(200, 20000, 100000, 0);
       i = rand();
       i = i % 500000;
       sprintf(q, "select a,b,c from tt where b=%d",i);
       res = p->run_query("localhost", "mysql", "", "test", 3306, q); 
       j = 0;
       if (res.size() > 0) {
           for (j = 0; j<res[0][1].length(); j++) {
               resc[j] = res[0][1][j];
           } 
       }
       resc[j] = '\0';
       if (atoi(resc) != i) {
           cerr << "Failed query with " << i << " got " << atoi(resc) << "\n";
           sprintf(q, "Failed query with %d got %d\n", i, atoi(resc));
           p->get_log()->write_message(q, NULL);
           exit(1);
       } else {
/*           cerr << "Successful query with " << i << " got " << atoi(resc) << "\n";
           sprintf(q, "Successful query with %d got %d\n", i, atoi(resc));
           p->get_log()->write_message(q, NULL); */
           exit(0);
       }

}

