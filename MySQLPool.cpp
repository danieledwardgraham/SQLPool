/* MySQLPool.cpp

   Desc: This program is a daemon for cross-process connection pooling, in MySQL.

   Args: 1. Total Maximum simultaneous connections
         2. Maximum simultaneous connections per DB
         3. Shared memory size for DATA
         4. Shared memory size for LOG
         5. Debug flag (0=no debug)

   Ver:  1.0, D. Graham, 8/18/2009
*/

#include <stdlib.h>
#include <unistd.h>
int Debug;
#include "MySQLPool.hh"
#define MAX_CONN_UPPER_LIMIT 10000

using namespace std;

void *run_query_found(void *);
void *run_query_notfound(void *);
int run_query_mysql(msg_t *);

int main (int argc, char *argv[]) {

shm_log_t *shm_log;
shm_data_t *shm_data;
msg_data_t *msg_data;

msg_t *msg[MAX_CONN_UPPER_LIMIT];
int i, rc;
char err_msg[201];

state_t *state;

size_t log_size, data_size;
long MAX_CONN, MAX_CONN_DB;

pthread_attr_t attr;
void *stackaddr;
size_t stacksize;

    if (argc != 6) {
        cerr << "Error: Usage: " << argv[0] << " <MAX_CONN> <MAX_CONN_DB> <data_size> <log_size> <debug flag>\n";
        exit(-1);
    }

    MAX_CONN = atol(argv[1]);
    if (MAX_CONN > MAX_CONN_UPPER_LIMIT) {
       cerr << "Error: MAX_CONN exceeds MAX_CONN_UPPER_LIMIT\n";
       exit(-1);
    }
 
    MAX_CONN_DB = atol(argv[2]);
    data_size = (size_t) atol(argv[3]);
    log_size = (size_t) atol(argv[4]);
    Debug = atoi(argv[5]);
    
    if (!(shm_log = new shm_log_t(log_size))) {
        cerr << "Error: falied to set up SHM_LOG\n";
        exit(-1);
    }

    shm_log->write_message("BEGIN OF PROGRAM.\n", NULL);
    if (!(shm_data = new shm_data_t(data_size, shm_log))) {
        cerr << "Error: failed to set up SHM_DATA\n";
        exit(-1);
    }

    if (!(msg_data = new msg_data_t)) {
        cerr << "Error: failed to set up MSG_DATA\n";
        exit(-1);
    }

    if (mysql_library_init(0, NULL, NULL) < 0) {
        perror("mysql_library_init");
        cerr << "Error: failed to initialize mysql library\n";
        exit(-1);
    } 

    shm_log->write_message("Setup Status.\n", NULL);
    state = new state_t(MAX_CONN, MAX_CONN_DB);

    if (daemon(0,0) < 0) {
        cerr << "Error: failed to daemonize\n";
        exit(-1);
    }  

   pthread_attr_init(&attr);
   stacksize = 100000;   
   if (rc = pthread_attr_setstacksize(&attr, stacksize)) {
       strncpy(err_msg, strerror(rc), 100);
       shm_log->write_message("Error in setstacksize\n", NULL);
       shm_log->write_message(err_msg, NULL);
   } 
   pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
   for (i=0; i<MAX_CONN_UPPER_LIMIT; i++) {
       msg[i] = NULL;
   }
   shm_log->write_message("Setup Complete.\n", NULL);
   i=0;
   while (1) {
       msg[i] = msg_data->get_msg();
       if (Debug) {
           shm_log->write_message("Got one message:\n", msg[i]); 
       }
       if (msg[i] == NULL) {
           continue;
       }
       msg[i]->set_state(state);
       msg[i]->set_shm_data(shm_data);
       msg[i]->set_shm_log(shm_log);
       if (state->lookup(msg[i]) == FOUND_ZZ) {
           rc = pthread_create(msg[i]->get_thread(), &attr, run_query_found, (void *)msg[i]); 
           if (rc) {
               strncpy(err_msg, strerror(rc), 100);
               shm_log->write_message("Error creating thread:\n", NULL);
               shm_log->write_message(err_msg, msg[i]);
               msg[i]->release_connect();
               msg[i]->notify_completion();
           }
       } else {
           if (Debug) {
               shm_log->write_message("Adding key...\n", msg[i]);
           }
           state->add_key(msg[i]);
           rc = pthread_create(msg[i]->get_thread(), &attr, run_query_notfound, (void *) msg[i]);
           if (rc) {
               strncpy(err_msg, strerror(rc), 100);
               shm_log->write_message("Error creating thread:\n", NULL);
               shm_log->write_message(err_msg, msg[i]);
               msg[i]->release_connect();
               msg[i]->notify_completion();
           }
       }
       if (msg[i+1] != NULL) {
           delete msg[i+1];
       }
       i = (i >= (MAX_CONN_UPPER_LIMIT - 2)) ? 0:i+1;
   }
}

void *run_query_found (void * p) {
    msg_t *msg;
    msg = (msg_t *) p;
    if (msg->wait_on_cond_var() == TIMEOUT_ZZ) {
        msg->get_log()->write_message("Error: timed out", msg);
        return(NULL);
    } 
    run_query_mysql(msg);
/*    free(msg);  */
    pthread_exit(NULL);
}

void *run_query_notfound (void * p) {
    msg_t *msg;
    msg = (msg_t *) p;
    run_query_mysql(msg);
/*    free(msg);  */
    pthread_exit(NULL);
}

int run_query_mysql(msg_t *msg) {
    long ret;
    MYSQL_RES *res;
    char err_msg[101];
    if (msg->get_connect() == FOUND_NEW_ZZ) {
       mysql_init(msg->get_db());
       mysql_real_connect(msg->get_db(), msg->get_host(), msg->get_user(), msg->get_pass(), msg->get_dbname(),
                          msg->get_port(), NULL, 0);
       msg->init_mutex();
       if (Debug) {
           msg->get_log()->write_message("Created new connection\n", msg);
       }
    } else {
       mysql_thread_init();
       if (Debug) {
           msg->get_log()->write_message("Reusing connection\n", msg);
       }
    } 
    pthread_mutex_lock(msg->get_mutex());
    ret = mysql_query(msg->get_db(), msg->get_query());
    if (ret) {
        strncpy(err_msg, strerror(ret), 100);
        msg->get_log()->write_message("Error: failed mysql_query", msg);
        msg->get_log()->write_message(err_msg, NULL);
        msg->get_log()->write_message("\n", NULL);
        pthread_mutex_unlock(msg->get_mutex());
        msg->release_connect();
        msg->notify_completion();
        return(-1);
    } 
    res = mysql_store_result(msg->get_db());
    pthread_mutex_unlock(msg->get_mutex());
    msg->release_connect();
    msg->write_result(res);
    msg->notify_completion();
    mysql_free_result(res);
    mysql_thread_end();
}


  
