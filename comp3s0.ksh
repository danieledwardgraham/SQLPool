#!/bin/ksh
export LD_RUN_PATH=/usr/lib/gcc/i386-redhat-linux/4.1.1
g++ -I"/home/dgraham/mysql/mysql-connector-c-6.0.1-linux-glibc2.3-x86-32bit/include" -L"/home/dgraham/mysql/mysql-connector-c-6.0.1-linux-glibc2.3-x86-32bit/lib" -lstdc++ -lmysqlclient_r -o MySQLPoolCliStress0 MySQLPoolCliStress0.cpp

