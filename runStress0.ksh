#!/bin/bash
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/home/dgraham/mysql/mysql-connector-c-6.0.1-linux-glibc2.3-x86-32bit/lib"
for i in {1..100}
do
   for j in {1..200}
   do
     ./MySQLPoolCliStress0 & 
   done
wait
done
