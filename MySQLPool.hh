/* This file provides the definition of classes used in the MySQLPool.cpp
   program. v. 1.0, D. Graham, 7/23/2009 */

#include <stdio.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/msg.h>
#include <sys/sem.h>
#include <sys/types.h>
#include <pthread.h>
#include <mysql.h>
#include <assert.h>

#define SHM_LOG_FILENAME "/tmp/mysqlpool_log_shm"
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

#include "CircularResultBuffer.hh"

using namespace std;

class state_t;
class shm_log_t; 
class shm_data_t;

class msg_t {
    public:
        msg_t(int, string, string, string, string, int, int, string);
        int wait_on_cond_var();
        int get_connect();
        MYSQL *get_db();
        char *get_host();
        char *get_user();
        char *get_pass();
        char *get_dbname();
        string get_mapkey();
        unsigned int get_port();
        int get_socket();
        int init_mutex();
        pthread_mutex_t *get_mutex();
        char *get_query();
        int release_connect();
        int write_result(MYSQL_RES *);
        int notify_completion();
        int set_state(state_t *);
        int set_shm_data(shm_data_t *);
        int set_shm_log(shm_log_t *);
        pthread_t *get_thread();
        shm_log_t *get_log();
        int get_dataid();
    private:
        state_t *state_ptr;
        shm_data_t *shm_data_ptr;
        shm_log_t *shm_log_ptr;
        string host, user, pass, dbname, query;
        char hostc[50], userc[50], passc[50], dbnamec[50], queryc[200];
        unsigned int port, socket;
        int CONN_ID;
        int data_id;
        string mapKey;
        int mapIndex;
        pthread_t pthr;
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
      pthread_mutex_t mut;
}; 
#endif

class shm_data_t {
    public:
        shm_data_t(size_t, shm_log_t *);
        int write_data(int,list<char>);
        ~shm_data_t();
    private:
        char *shm_ptr;
        void *attach_ptr;
        int intid;
        size_t datasize;
        shm_log_t *shm_log;
        pthread_mutex_t mut;
        CircularResultBuffer *CRB;
};

class state_t {
    friend class msg_t;
    public:
        state_t(long, long);
        int add_key(msg_t *);
        int lookup(msg_t *);
        ~state_t();
    private:
        map<string,pthread_cond_t *> COND;
        map<string,pthread_mutex_t *> MUTCOND;
        map<string,int> CONDVAR;
        map<string, vector<MYSQL *> > CONN;
        map<string, vector<char> > STAT;
        map<string, vector<pthread_mutex_t *> > MUTSQL;
        int intid;
        pthread_mutex_t mut;
        long MAX_CONN_DB;
        long MAX_CONN;
}; 

class msg_data_t {
    public:
        msg_data_t();
        msg_t *get_msg();
        ~msg_data_t();
    private:
        int intid;
        pthread_mutex_t mut;
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
/*    cerr << "SIZE : " << sz << "\n";  */
    ofstream Log(SHM_LOG_FILENAME, ios::out);
    Log << '\0';
    Log.close();
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
/*    cerr << "INTID: " << this->intid << "\n"; */
/*    this->attach_ptr = malloc(sz);
    free(this->attach_ptr); */
    this->shm_ptr = (char *) shmat(this->intid, NULL, 0);
    if (this->shm_ptr == (char *) (-1)) {
        perror("shmat");
        exit(-1);
    }
/*    cerr << *(this->shm_ptr) << "\n"; */
    *((size_t *) this->shm_ptr) = sz;
    *((unsigned int *)(this->shm_ptr + sizeof(size_t))) = sizeof(size_t) + sizeof(unsigned int) * 2;
/*    cerr << "Check0: " << (unsigned long) (this->shm_ptr) << "\n"; */
/*    cerr << "Check1: " << (unsigned long) (this->shm_ptr + sizeof(size_t) + sizeof(char *) + sizeof(char *)) << "\n"; */
/*    cerr << "Check: " << (unsigned long) *((char **)(this->shm_ptr + sizeof(size_t))) << "\n"; */
    *((unsigned int *)(this->shm_ptr + sizeof(size_t) + sizeof(unsigned int))) = sizeof(size_t) + sizeof(unsigned int) * 2;
    pthread_mutex_init(&(this->mut), NULL);
}

int shm_log_t::write_message(string str, msg_t *mess) {
    int i;
    char *st, *end;
    char Port[100];
    char Socket[100];
    char DataId[100];
    if (mess != NULL) {
        str.append(" HOST=");
        str.append(mess->get_host());
        str.append(",DB=");
        str.append(mess->get_dbname());
        str.append(",QUERY=");
        str.append(mess->get_query());
        str.append(",port=");
        sprintf(Port, "%d", mess->get_port());
        str.append(Port);
        str.append(",socket=");
        sprintf(Socket, "%d", mess->get_socket());
        str.append(Socket);
        str.append(",data_id=");
        sprintf(DataId, "%d", mess->get_dataid());
        str.append(DataId); 
        str.append("\n");
    }
    pthread_mutex_lock(&(this->mut));
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
   pthread_mutex_unlock(&(this->mut));
   return(0);
}

shm_log_t::~shm_log_t() {
    shmdt(this->shm_ptr);
    pthread_mutex_destroy(&(this->mut));
}

/* shm_data_t */

shm_data_t::shm_data_t(size_t sz, shm_log_t *log) {
    key_t mykey;
    if (sz < (sizeof(size_t) + sizeof(int) + sizeof(size_t) + sizeof(int) + sizeof(size_t) + sizeof(char) * 2)) {
        cerr << "Error in shm_data_t constructor: size too small.\n";
        exit(-1);
    }
    this->shm_log = log;
    this->datasize = sz;
    ofstream Data(SHM_DATA_FILENAME, ios::out);
    Data << '\0';
    Data.close();
    mykey = ftok(SHM_DATA_FILENAME, 1);
    this->intid = shmget(mykey, sz, 0666 | IPC_CREAT);
/*    this->attach_ptr = malloc(sz);
    free(this->attach_ptr); */
    this->shm_ptr = (char *) shmat(this->intid, NULL, 0);
    *((size_t *) this->shm_ptr) = sz;
    *((int *) (this->shm_ptr + sizeof(size_t))) = 0;
    CRB = new CircularResultBuffer(this->shm_ptr, log);
    CRB->reset();
    pthread_mutex_init(&(this->mut), NULL);
}

int shm_data_t::write_data(int data_id, list<char> res) {
    char logmsg[100];
    pthread_mutex_lock(&(this->mut));
    if (this->CRB->startResult(data_id) < 0) { 
        sprintf(logmsg, "Error! Results larger than data segment for data_id=%d\n", data_id);
        this->shm_log->write_message(logmsg, NULL);
        pthread_mutex_unlock(&(this->mut));
        return(-1);
    }
    if (this->CRB->addResultData(res) < 0) { 
        sprintf(logmsg, "Error! Results larger than data segment for data_id=%d\n", data_id);
        this->shm_log->write_message(logmsg, NULL);
        pthread_mutex_unlock(&(this->mut));
        return(-1);
    }
    pthread_mutex_unlock(&(this->mut));
    return(0);
}
     
shm_data_t::~shm_data_t() {
    shmdt(this->shm_ptr);
    pthread_mutex_destroy(&(this->mut));
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
    pthread_mutex_init(&(this->mut), NULL);
}

msg_data_t::~msg_data_t() {
      if (msgctl(this->intid, IPC_RMID, NULL) == -1) {
            perror("msgctl");
            exit(1);
        }
}
msg_t *msg_data_t::get_msg() {
   msg_t *ret_msg;
   int msg_sz, i, j;
   int data_id, port, socket;
   char tmp1[2];
   string host, user, pass, dbname, query;
   tmp1[1] = '\0';
   msg_sz = msgrcv(this->intid, &(this->msg_buf), 10000, 0, 0);
   i = 0;
   if (i + sizeof(int) > msg_sz) {
       return(NULL);
   }
   data_id = *((int *) &(this->msg_buf.mtext[i]));
   i += sizeof(int);
   j = 0;
   while (this->msg_buf.mtext[i] != '\0') {
       tmp1[0] = this->msg_buf.mtext[i++];
       host.append(tmp1);
       if (i > msg_sz) {
           return(NULL);
       }
   }
   i++;
   if (i > msg_sz) {
       return(NULL);
   }
   j = 0;
   while (this->msg_buf.mtext[i] != '\0') {
       tmp1[0] = this->msg_buf.mtext[i++];
       user.append(tmp1);
       if (i > msg_sz) {
           return(NULL);
       }
   }
   i++;
   if (i > msg_sz) {
       return(NULL);
   }
   j = 0;
   while (this->msg_buf.mtext[i] != '\0') {
       tmp1[0] = this->msg_buf.mtext[i++];
       pass.append(tmp1);
       if (i > msg_sz) {
           return(NULL);
       }
   }
   i++;
   if (i > msg_sz) {
       return(NULL);
   }
   j = 0;
   while (this->msg_buf.mtext[i] != '\0') {
       tmp1[0] = this->msg_buf.mtext[i++];
       dbname.append(tmp1);
       if (i > msg_sz) {
           return(NULL);
       }
   }
   i++;
   if (i > msg_sz) {
       return(NULL);
   }
   if (i + sizeof(int) > msg_sz) {
       return(NULL);
   }
   port = *((int *) &(this->msg_buf.mtext[i]));
   i += sizeof(int);
   if (i + sizeof(int) > msg_sz) {
      return(NULL);
   }
   socket = *((int *) &(this->msg_buf.mtext[i]));
   i += sizeof(int);
   j = 0;
   while (this->msg_buf.mtext[i] != '\0') {
       tmp1[0] = this->msg_buf.mtext[i++];
       query.append(tmp1);
       if (i > msg_sz) {
           return(NULL);
       }
   }
   ret_msg = new msg_t(data_id, host, user, pass, dbname, port, socket, query);
   return ret_msg;
}

state_t::state_t(long MAX_CONN, long MAX_CONN_DB) {
    key_t mykey;
    int intid_st, ret, i;
    union semun {
            int val;
            struct semid_ds *buf;
            unsigned short  *array;
    } arg;
    ofstream Sem(SEM_FINISH_FILENAME, ios::out);
    Sem << '\0';
    Sem.close();
    mykey = ftok(SEM_FINISH_FILENAME, 1);
    this->intid = semget(mykey, MAX_CONN, 0666 | IPC_CREAT);
    if (this->intid < 0) {
        perror("semget");
        exit(-1);
    }
/*    arg.val = 0;
    for (i = 0; i<MAX_CONN; i++) {
        ret = semctl(this->intid, i, SETVAL, arg);
        if (ret < 0) {
            perror("semctl fin");
            exit(-1);
        }
    }   */
    ofstream Sem2(SEM_START_FILENAME, ios::out);
    Sem2 << '\0';
    Sem2.close();
    mykey = ftok(SEM_START_FILENAME, 1);
    intid_st = semget(mykey, 1, 0666 | IPC_CREAT);
    if (intid_st < 0) {
        perror("semget (st)");
        exit(-1);
    }
    arg.val = 1;
    ret = semctl(intid_st, 0, SETVAL, arg); 
    if (ret < 0) {
        perror("semctl st");
        exit(-1);
    }
    pthread_mutex_init(&(this->mut), NULL);
    this->MAX_CONN_DB = MAX_CONN_DB;
    this->MAX_CONN = MAX_CONN;
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

int state_t::lookup(msg_t * m) {
    if (this->COND.find(m->get_mapkey()) != this->COND.end()) {
        return(FOUND_ZZ);
    } else {
        return(NOTFOUND_ZZ);
    }
} 

int state_t::add_key(msg_t * m) {
    int i;
    string mapKey;
    mapKey = m->get_mapkey();
    pthread_mutex_lock(&(this->mut));
    this->COND[mapKey] = new pthread_cond_t;
    pthread_cond_init(this->COND[mapKey], NULL);
    for (i = 0; i < this->MAX_CONN_DB; i++) {
        this->CONN[mapKey].push_back(new MYSQL);
    }
    for (i = 0; i < this->MAX_CONN_DB; i++) {
        this->STAT[mapKey].push_back('0');
    }
    for (i = 0; i < this->MAX_CONN_DB; i++) {
        this->MUTSQL[mapKey].push_back(new pthread_mutex_t);
/*        pthread_mutex_init(this->MUTSQL[mapKey][i], NULL); */
    }
    this->MUTCOND[mapKey] = new pthread_mutex_t;
    pthread_mutex_init(this->MUTCOND[mapKey], NULL); 
    this->CONDVAR[mapKey] = this->MAX_CONN_DB;
    pthread_mutex_unlock(&(this->mut));
    return(0);
}

int msg_t::wait_on_cond_var() {
    pthread_mutex_lock(state_ptr->MUTCOND[this->mapKey]);
    if (this->state_ptr->CONDVAR[this->mapKey] <= 0) {
        pthread_cond_wait(this->state_ptr->COND[this->mapKey], this->state_ptr->MUTCOND[this->mapKey]);
    }
    pthread_mutex_unlock(this->state_ptr->MUTCOND[this->mapKey]);
    return(0);
}

int msg_t::get_connect() {
    int i;
    pthread_mutex_lock(&(this->state_ptr->mut));
    pthread_mutex_lock(this->state_ptr->MUTCOND[this->mapKey]);
    this->state_ptr->CONDVAR[this->mapKey]--;
    for (i = 0; i < (this->state_ptr)->MAX_CONN_DB; i++) {
        if (this->state_ptr->STAT[this->mapKey][i] == '1') {
            this->mapIndex = i;
            this->state_ptr->STAT[this->mapKey][i] = '2';
            pthread_mutex_unlock(this->state_ptr->MUTCOND[this->mapKey]);
            pthread_mutex_unlock(&(this->state_ptr->mut));
            return(FOUND_OLD_ZZ);
        }
    }
    for (i = 0; i < (this->state_ptr)->MAX_CONN_DB; i++) {
        if (this->state_ptr->STAT[this->mapKey][i] == '0') {
            this->mapIndex = i;
            this->state_ptr->STAT[this->mapKey][i] = '2';
            pthread_mutex_unlock(this->state_ptr->MUTCOND[this->mapKey]);
            pthread_mutex_unlock(&(this->state_ptr->mut));
            return(FOUND_NEW_ZZ);
        }
    }
    this->shm_log_ptr->write_message("PANIC: Could not allocate connection index. Shutting down.\n", this);
    cerr << "PANIC: Could not allocate connection index. Shutting down\n";
    exit(1);
}

MYSQL *msg_t::get_db() {
   return(this->state_ptr->CONN[this->mapKey][this->mapIndex]);
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
    pthread_mutex_init(this->state_ptr->MUTSQL[this->mapKey][this->mapIndex], NULL); 
    return(0);
}

pthread_mutex_t *msg_t::get_mutex() {
    return(this->state_ptr->MUTSQL[this->mapKey][this->mapIndex]);
}

char *msg_t::get_query() {
    return(this->queryc);
}

int msg_t::release_connect() {
    pthread_mutex_lock(&(this->state_ptr->mut));
    pthread_mutex_lock(this->state_ptr->MUTCOND[this->mapKey]);
    this->state_ptr->CONDVAR[this->mapKey]++;
    this->state_ptr->STAT[mapKey][mapIndex] = '1';
    if (this->state_ptr->CONDVAR[this->mapKey] == 1) {
        pthread_cond_broadcast(this->state_ptr->COND[this->mapKey]);
    }
    pthread_mutex_unlock(this->state_ptr->MUTCOND[this->mapKey]);
    pthread_mutex_unlock(&(this->state_ptr->mut));
    return(0);
}

int msg_t::write_result(MYSQL_RES *res) {
    MYSQL_ROW row;
    unsigned int i, j, k, ret;
    unsigned int num_fields;
    list<char> list_data;
    char Rowcnt[100];
    string RowcntMess;
    struct sembuf opr;
    i = 0;
    if (res != NULL) {
        num_fields = mysql_num_fields(res);
        while (row = mysql_fetch_row(res)) {
            for (j = 0; j < num_fields; j++) {
                if (row[j] == NULL) {
                    list_data.push_back('N');
                    list_data.push_back('U');
                    list_data.push_back('L');
                    list_data.push_back('L');
                    list_data.push_back('\0');
                } else {
                    k = 0;
                    while (row[j][k] != '\0') { 
                        list_data.push_back(row[j][k]);
                        k++;
                    }
                    list_data.push_back('\0');
                }
            }
            i++;
            list_data.push_back('\0');
        }
    } else {
        list_data.push_back('\0');
    }
    if (Debug) {
        sprintf(Rowcnt, "rows returned: %d\n", i);
        RowcntMess = Rowcnt;
        shm_log_ptr->write_message(RowcntMess, this);
    }
    shm_data_ptr->write_data(this->data_id, list_data);
    if (Debug) {
        shm_log_ptr->write_message("Complete on write data\n", this);
    }
    return(0);
}
    
int msg_t::notify_completion() {
    struct sembuf opr;
    opr.sem_num = (u_short) (this->data_id % (this->state_ptr)->MAX_CONN);
    opr.sem_op = 1;
    opr.sem_flg = 0;
    semop((this->state_ptr)->intid, &opr, 1);
}

int msg_t::set_state(state_t * st) {
    this->state_ptr = st;
}

int msg_t::set_shm_data(shm_data_t * shm) {
    this->shm_data_ptr = shm;
}

int msg_t::set_shm_log(shm_log_t * log) {
    this->shm_log_ptr = log;
}

pthread_t *msg_t::get_thread() {
    return &(this->pthr);
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
