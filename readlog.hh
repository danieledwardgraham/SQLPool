
/* This file provides the definition of classes used in the readlog.cpp
   program. v. 1.0, D. Graham, 8/21/2009 */

#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/types.h>
#include <assert.h>

#define SHM_LOG_FILENAME "/tmp/mysqlpool_log_shm"

#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <map>

using namespace std;

class shm_log_t {
   public:
      shm_log_t(size_t);
      string read_message();
      ~shm_log_t();
   private:
      char *shm_ptr;
      int intid;
      size_t logsize;
}; 

/* shm_log_t */

shm_log_t::shm_log_t(size_t sz) {
    key_t mykey;
    cerr << "SIZE: " << sz << "\n";
/*    if (sz < (sizeof(size_t) + sizeof(unsigned int) * 2 + sizeof(char))) {
        cerr << "Readlog: Error in shm_log_t constructor: size too small.\n";
        exit(-1);
    } */
    this->logsize = sz;
    mykey = ftok(SHM_LOG_FILENAME, 1);
    if (mykey < 0) {
        perror("ftok");
        exit(-1);
    }
    this->intid = shmget(mykey, sz, 0666 | IPC_CREAT);
    if (this->intid < 0) {
        perror("shmget");
        exit(-1);
    }
    this->shm_ptr = (char *) shmat(this->intid, NULL, 0);
    if (this->shm_ptr == (char *) (-1)) {
        perror("shmat");
        exit(-1);
    }
}

string shm_log_t::read_message() {
    char *st, *end;
    char *trav;
    string retstr;
    char tmp1[2];
    tmp1[1] = '\0';
    int i = 0;
    assert(this->shm_ptr + *((unsigned int *)(this->shm_ptr + sizeof(size_t))) < (this->shm_ptr + *((size_t *) this->shm_ptr))); 
    assert(this->shm_ptr + *((unsigned int *) (this->shm_ptr + sizeof(size_t) + sizeof(unsigned int))) < (this->shm_ptr + *((size_t *) this->shm_ptr)));
    st = this->shm_ptr + *((unsigned int *) (this->shm_ptr + sizeof(size_t)));
    end = this->shm_ptr + *((unsigned int *) (this->shm_ptr + sizeof(size_t) + sizeof(unsigned int)));
    trav = st;
    while (trav != end) {
        tmp1[0] = *trav;
        retstr.append(tmp1); 
        trav++;
        if (trav >= (this->shm_ptr + *((size_t *) this->shm_ptr))) {
            trav = this->shm_ptr + sizeof(size_t) + 2 * sizeof(unsigned int); 
        }
   }
   return(retstr);
}

shm_log_t::~shm_log_t() {
    shmdt(this->shm_ptr);
}
