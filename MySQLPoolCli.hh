/* This file provides the definition of classes used in the MySQLPool.cpp
   program. v. 1.0, D. Graham, 7/23/2009 */

#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/msg.h>
#include <sys/sem.h>
#include <sys/types.h>
#include <assert.h>

#define SHM_CLILOG_FILENAME "/tmp/mysqlpool_clilog_shm"
#define SHM_DATA_FILENAME "/tmp/mysqlpool_data_shm"
#define MSG_DATA_FILENAME "/tmp/mysqlpool_data_msg"
#define SEM_FINISH_FILENAME "/tmp/mysqlpool_fin_sem"
#define SEM_START_FILENAME "/tmp/mysqlpool_st_sem"

#define FOUND_NEW_ZZ 1
#define FOUND_OLD_ZZ 0
#define TIMEOUT_ZZ -1
#define FOUND_ZZ 1
#define NOTFOUND_ZZ 0

#include <fstream>
#include <vector>
#include <string>
#include <map>

int Debug;
#include "CircularResultBuffer.hh"

using namespace std;

/* class shm_log_t; */
class shm_data_t;
class msg_data_t;

typedef union {
    int val;
    struct semid_ds *buf;
    unsigned short *array;
    struct seminfo *__buf;
} sumun;

class msg_t {
    public:
        msg_t(int, string, string, string, string, int, int, string);
        int wait_on_cond_var();
        int get_connect();
        char *get_host();
        char *get_user();
        char *get_pass();
        char *get_dbname();
        int get_dataid();
        string get_mapkey();
        unsigned int get_port();
        int get_socket();
        int init_mutex();
        char *get_query();
        int set_shm_data(shm_data_t *);
        int set_shm_log(shm_log_t *);
        shm_log_t *get_log();
    private:
        shm_data_t *shm_data_ptr;
        shm_log_t *shm_log_ptr;
        string host, user, pass, dbname, query;
        char hostc[50], userc[50], passc[50], dbnamec[50], queryc[200];
        unsigned int port, socket;
        int CONN_ID;
        int data_id;
        string mapKey;
        int mapIndex;
};

#ifndef SHM_LOG_T
#define SHM_LOG_T
class shm_log_t {
   public:
      shm_log_t(size_t);
      int write_message(string, msg_t *);
      ~shm_log_t();
   private:
      char *shm_ptr;
      void *attach_ptr;
      int intid;
      size_t logsize;
}; 
#endif

class shm_data_t {
    public:
        shm_data_t(size_t, shm_log_t *);
        vector<vector<string> > read_data(int);
        char *get_shm_ptr();
        ~shm_data_t();
    private:
        char *shm_ptr;
        void *attach_ptr;
        int intid;
        size_t datasize;
        shm_log_t *shm_log;
        CircularResultBuffer *CRB;
};

class mysqlpool {
    public:
        mysqlpool(long, size_t, size_t, int);
        vector<vector<string> > run_query(string, string, string, string, int, string);
        shm_log_t *get_log();
        ~mysqlpool();
    private:
        msg_t *msg;
        shm_data_t *shm_data;
        shm_log_t *shm_log;
        msg_data_t *msg_data;
        int intid_st;
        int intid_fin;
        int intid_rd;
        long max_conn;
};

class msg_data_t {
    public:
        msg_data_t();
        msg_t *get_msg();
        int put_msg(msg_t *);
        ~msg_data_t();
    private:
        int intid;
        struct msg_buf_t {
            long mtype;
            char mtext[10000];
        } msg_buf;
};

/*    Implementation     */

/* shm_log_t */

shm_log_t::shm_log_t(size_t sz) {
    key_t mykey;
    if (sz < (sizeof(size_t) + sizeof(unsigned int) * 2 + sizeof(char))) {
        cerr << "Error in shm_log_t constructor: size too small.\n";
        exit(-1);
    }
    this->logsize = sz;
    mykey = ftok(SHM_CLILOG_FILENAME, 1);
    if (mykey < 0) {
        perror("ftok");
        ofstream Log(SHM_CLILOG_FILENAME, ios::out);
        Log << '\0';
        Log.close();
        exit(-1);
    }
    this->intid = shmget(mykey, sz, 0666 | IPC_CREAT);
    if (this->intid < 0) {
        perror("shmget");
        exit(-1);
    }
/*    this->attach_ptr = malloc(sz);
    free(this->attach_ptr); */
    this->shm_ptr = (char *) shmat(this->intid, NULL, 0);
    if (this->shm_ptr == (char *) (-1)) {
        perror("shmat");
        exit(-1);
    }
    *((size_t *) this->shm_ptr) = sz;
    *((unsigned int *)(this->shm_ptr + sizeof(size_t))) = sizeof(size_t) + sizeof(unsigned int) * 2;
    *((unsigned int *)(this->shm_ptr + sizeof(size_t) + sizeof(unsigned int))) = sizeof(size_t) + sizeof(unsigned int) * 2;
}

int shm_log_t::write_message(string str, msg_t *mess) {
    int i;
    char *st, *end;
    if (mess != NULL) {
        str.append(" HOST=");
        str.append(mess->get_host());
        str.append(",DB=");
        str.append(mess->get_dbname());
        str.append(",QUERY=");
        str.append(mess->get_query());
        str.append("\n");
    }
    assert(this->shm_ptr + *((unsigned int *)(this->shm_ptr + sizeof(size_t))) < (this->shm_ptr + *((size_t *) this->shm_ptr))); 
    assert(this->shm_ptr + *((unsigned int *) (this->shm_ptr + sizeof(size_t) + sizeof(unsigned int))) < (this->shm_ptr + *((size_t *) this->shm_ptr)));
    st = this->shm_ptr + *((unsigned int *) (this->shm_ptr + sizeof(size_t)));
    end = this->shm_ptr + *((unsigned int *) (this->shm_ptr + sizeof(size_t) + sizeof(unsigned int)));
    for (i=0; i<str.length(); i++) {
        *end = str[i];
        end++;
        if (end >= (this->shm_ptr + *((size_t *) this->shm_ptr))) {
            end = this->shm_ptr + sizeof(size_t) + 2 * sizeof(unsigned int); 
        }
   }
   *((unsigned int *) (this->shm_ptr + sizeof(size_t))) = st - this->shm_ptr;
   *((unsigned int *) (this->shm_ptr + sizeof(size_t) + sizeof(unsigned int))) = end - this->shm_ptr;
   return(0);
}

shm_log_t::~shm_log_t() {
    shmdt(this->shm_ptr);  
}

/* shm_data_t */

shm_data_t::shm_data_t(size_t sz, shm_log_t *log) {
    key_t mykey;
    if (sz < (sizeof(size_t) + sizeof(int) + sizeof(unsigned int) + sizeof(int) + sizeof(unsigned int) + sizeof(char) * 2)) {
        cerr << "Error in shm_data_t constructor: size too small.\n";
        exit(-1);
    }
    this->datasize = sz;
    this->shm_log = log;
    mykey = ftok(SHM_DATA_FILENAME, 1);
    this->intid = shmget(mykey, sz, 0666 | IPC_CREAT);
    this->shm_ptr = (char *) shmat(this->intid, NULL, 0);
    CRB = new CircularResultBuffer(this->shm_ptr, log);
}

vector<vector<string> > shm_data_t::read_data(int data_id) {
    int i,j,ib,jb;
    int nodeCnt = 0;
    vector<vector<string> > res;
    vector<string> subres;
    list<char> data_list;
    string s1;
    string fieldstr;
    char chrstr[2];
    size_t endOfData;
    char err_msg[100];
    char err_msg2[2000];
    string err_msg_str;
    string err_msg_str2; 
    int errseen = 0;
    int errseen2 = 0;
    key_t mykey;
    int em;

    chrstr[1] = '\0';

    sprintf(err_msg, "Just Before readResult - data_id = %d\n", data_id);
    shm_log->write_message(err_msg, NULL);
    data_list = CRB->readResult(data_id);
    sprintf(err_msg, "Just After readResult - data_id = %d\n", data_id);
    shm_log->write_message(err_msg, NULL);
    if (data_list.empty()) {
        sprintf(err_msg, "Error - data_id = %d not found\n", data_id);
        shm_log->write_message(err_msg, NULL);
    }
    i = j = 0;
    ib = jb = -1; 
    fieldstr = "";
    while (!data_list.empty()) {
        while (!data_list.empty()) {
            while (!data_list.empty()) {
                if (data_list.front() == '\0') {
                    break;
                }
                chrstr[0] = data_list.front();
                fieldstr.append(chrstr);
                data_list.pop_front();
           }
           if (i > ib) {
               res.push_back(subres);
               ib = i;
           }
           if (j > jb) {
               res[i].push_back(s1);
               jb = j;
           }
           res[i][j] = fieldstr;
           if (data_list.empty()) {
               break;
           }
           fieldstr = "";
           data_list.pop_front();
           if (data_list.front() == '\0') {
               break;
           }
           j++;
        }
        if (data_list.empty()) {
            break;
        }
        j = 0;
        jb = -1;
        i++;
        data_list.pop_front();
    }

    return(res);
}
     
shm_data_t::~shm_data_t() {
    shmdt(this->shm_ptr); 
}

msg_data_t::msg_data_t() {
    key_t mykey;
    ofstream MSG(MSG_DATA_FILENAME, ios::out);
    MSG << '\0';
    MSG.close();
    mykey = ftok(MSG_DATA_FILENAME, 1);
    if (mykey < 0) {
        perror("ftok msg_data_t");
        exit(-1);
    }
    this->intid = msgget(mykey, 0666 | IPC_CREAT);
    if (this->intid < 0) {
        perror("msgget");
        exit(-1);
    }
}

msg_data_t::~msg_data_t() {
}

int msg_data_t::put_msg(msg_t *put_msg) {
   int msg_sz, i, j;
   int data_id, port, socket;
   char tmp1[2];
   string host, user, pass, dbname, query;
   tmp1[1] = '\0';
   i = 0;
   this->msg_buf.mtype = 1;
   *((int *) &(this->msg_buf.mtext[i])) = put_msg->get_dataid();
   i += sizeof(int);
   host = put_msg->get_host();
   for (j = 0; j < host.length(); j++) {
       this->msg_buf.mtext[i++] = host[j];
       if (i > 10000) {
           return(-1);
       }
   }
   this->msg_buf.mtext[i++] = '\0';
   if (i > 10000) {
       return(-1);
   }
   user = put_msg->get_user();
   for (j = 0; j < user.length(); j++) {
       this->msg_buf.mtext[i++] = user[j];
       if (i > 10000) {
           return(-1);
       }
   }
   this->msg_buf.mtext[i++] = '\0';
   if (i > 10000) {
       return(-1);
   }
   pass = put_msg->get_pass();
   for (j = 0; j < pass.length(); j++) {
       this->msg_buf.mtext[i++] = pass[j];
       if (i > 10000) {
           return(-1);
       }
   }
   this->msg_buf.mtext[i++] = '\0';
   if (i > 10000) {
       return(-1);
   }
   dbname = put_msg->get_dbname();
   for (j = 0; j < dbname.length(); j++) {
       this->msg_buf.mtext[i++] = dbname[j];
       if (i > 10000) {
           return(-1);
       }
   }
   this->msg_buf.mtext[i++] = '\0';
   if (i > 10000) {
       return(-1);
   }
   if (i + sizeof(int) > 10000) {
       return(-1);
   }
   port = put_msg->get_port();
   *((int *) &(this->msg_buf.mtext[i])) = port;
   i += sizeof(int);
   if (i + sizeof(int) > 10000) {
      return(-1);
   }
   socket = put_msg->get_socket();
   *((int *) &(this->msg_buf.mtext[i])) = socket;
   i += sizeof(int);
   query = put_msg->get_query();
   for (j = 0; j < query.length(); j++) {
       this->msg_buf.mtext[i++] = query[j];
       if (i > 10000) {
           return(-1);
       }
   }
   this->msg_buf.mtext[i++] = '\0';
   if (i > 10000) {
       return(-1);
   }
   if (msgsnd(this->intid, &(this->msg_buf), i, 0)) {
        perror("msgsnd");
        exit(1);
   }
   return(0);
}

msg_t::msg_t(int data_id, string host, string user, string pass, string dbname, int port, int socket, string query) {
    int i;
    this->data_id = data_id;
    this->host = host;
    this->user = user;
    this->pass = pass;
    this->dbname = dbname;
    this->port = port;
    this->socket = socket;
    this->query = query;
    for (i = 0; i < host.length(); i++) {
        this->hostc[i] = host[i];
    }
    this->hostc[i] = '\0';
    for (i = 0; i < user.length(); i++) {
        this->userc[i] = user[i];
    }
    this->userc[i] = '\0';
    for (i = 0; i < pass.length(); i++) {
        this->passc[i] = pass[i];
    }
    this->passc[i] = '\0';
    for (i = 0; i < dbname.length(); i++) {
        this->dbnamec[i] = dbname[i];
    }
    this->dbnamec[i] = '\0';
    for (i = 0; i < query.length(); i++) {
        this->queryc[i] = query[i];
    }
    this->queryc[i] = '\0';
    this->mapKey = this->mapKey.append(host);
    this->mapKey = this->mapKey.append(user);
    this->mapKey = this->mapKey.append(pass);
    this->mapKey = this->mapKey.append(dbname);
}

char *msg_t::get_host() {
    return(this->hostc);
}

char *msg_t::get_user() {
    return(this->userc);
}

char *msg_t::get_pass() {
    return(this->passc);
}

char *msg_t::get_dbname() {
    return(this->dbnamec);
}

unsigned int msg_t::get_port() {
    return(this->port);
}

int msg_t::get_socket() {
    return(this->socket);
}

int msg_t::init_mutex() {
    return(0);
}

char *msg_t::get_query() {
    return(this->queryc);
}

int msg_t::set_shm_data(shm_data_t * shm) {
    this->shm_data_ptr = shm;
}

int msg_t::set_shm_log(shm_log_t * log) {
    this->shm_log_ptr = log;
}

string msg_t::get_mapkey() {
    return this->mapKey;
}

shm_log_t *msg_t::get_log() {
    return this->shm_log_ptr;
}

int msg_t::get_dataid() {
    return this->data_id;
}

char *shm_data_t::get_shm_ptr() {
    return this->shm_ptr;
}
mysqlpool::mysqlpool(long max_conn, size_t data_size, size_t log_size, int debug) {
    key_t mykey;
    sumun semu;
    int ret;
    this->shm_log = new shm_log_t(log_size);
    this->shm_data = new shm_data_t(data_size, this->shm_log);
    this->msg_data = new msg_data_t();
    this->max_conn = max_conn;
    Debug = debug;
    mykey = ftok(SEM_FINISH_FILENAME, 1);
    this->intid_fin = semget(mykey, max_conn, 0666 | IPC_CREAT);
    if (this->intid_fin < 0) {
        perror("semget (fin)");
        exit(-1);
    }
    mykey = ftok(SEM_START_FILENAME, 1);
    this->intid_st = semget(mykey, 1, 0666 | IPC_CREAT);
    if (this->intid_st < 0) {
        perror("semget (st)");
        ofstream Sem(SEM_START_FILENAME, ios::out);
        Sem << '\0';
        Sem.close();
        exit(-1);
    }
}

mysqlpool::~mysqlpool() {
    delete this->shm_log;
    delete this->shm_data;
    delete this->msg_data;
}

vector<vector<string> > mysqlpool::run_query(string host, string user, string pass, string dbname, int port, string query) {
    int data_id;
    struct sembuf opr;
    char DataId[100];
    string DataIdMess;
    int ret;
    vector<vector<string> > res;
    opr.sem_num = 0;
    opr.sem_op = -1;
    opr.sem_flg = 0; 
    ret = semop(this->intid_st, &opr, 1);
    if (ret < 0) {
        perror("semop (st,-1)");
        exit(-1);
    }
    data_id = *((int *) ((this->shm_data)->get_shm_ptr() + sizeof(size_t)));
    *((int *) ((this->shm_data)->get_shm_ptr() + sizeof(size_t))) = ((data_id >= (10000000 - 1)) ? 0:(data_id + 1));
    opr.sem_num = 0;
    opr.sem_op = 1;
    opr.sem_flg = 0;
    ret = semop(this->intid_st, &opr, 1);
    if (ret < 0) {
        perror("semop (st,1)");
        exit(-1);
    }
    this->msg = new msg_t(data_id, host, user, pass, dbname, port, 0, query);
    if (Debug) {
        (this->shm_log)->write_message("Preparing to put msg\n", this->msg);
    }
    (this->msg_data)->put_msg(this->msg);
    opr.sem_num = (data_id % this->max_conn);
    opr.sem_op = -1;
    opr.sem_flg = 0;
    ret = semop(this->intid_fin, &opr, 1);
    if (ret < 0) {
        perror("semop (fin,-1)");
        exit(-1);
    }
    if (Debug) {
        sprintf(DataId, "Preparing to read results with data_id=%d\n", data_id);
        DataIdMess = DataId;
        (this->shm_log)->write_message(DataIdMess, this->msg);
    }
    res = (this->shm_data)->read_data(data_id);
    delete this->msg;
    return(res);
}

shm_log_t *mysqlpool::get_log() {
    return this->shm_log;
} 
