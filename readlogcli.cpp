/* This program reads the shared memory log and writes it to standard output
   Arguments:
   1. size of log segment
 */

#include "readlogcli.hh"

main (int argc, char *argv[]) {
    string thelogstr;
    size_t thelogsize;
    shm_log_t *thelog; 
    if (argc != 2) {
        cerr << "Error: Usage: " << argv[0] << " <log size>\n";
        exit(-1);
    } 
    thelogsize = (size_t) atol(argv[1]);
    thelog = new shm_log_t(thelogsize);
    thelogstr = thelog->read_message();
    cout << thelogstr; 
    cout << "End of log.\n";
} 
