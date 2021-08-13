import os
import subprocess
import datetime
import string
import random
import taos
import sys
import getopt
import time

##this program will read the queries from queryList_3.txt, and execute each line through python connector
##the result will be stored in query_execution_time_3.log

def executeQueryCommand(query, c1):
    ##this function will execute the query and record the time, but will not reset the cache
    with open("query_execution_time_3.log", 'a') as f:
        f.write(f"execute\t{query}\n")
        print(f"execute\t{query}\n")
        timeStart = datetime.datetime.now()
        try:
            c1.execute(query)
            c1.fetchall()
        except Exception as e:
            f.write(f'error {e} has occured\n')
        timeEnd = datetime.datetime.now()
        print(f"start time:\t {timeStart}\n")
        print(f"end time:\t {timeEnd}\n\n")
        print(f"time passed :\t {(timeEnd - timeStart)}\n\n")
        f.write(f"start time:\t {timeStart}\n")
        f.write(f"end time:\t {timeEnd}\n\n")
        f.write(f"time passed :\t {(timeEnd - timeStart)}\n\n")     

add = '20.98.75.200'
try:
    opts, args = getopt.gnu_getopt(sys.argv,"ha:",["address="])
except getopt.GetoptError:
    print ('test.py -a <address> ')
    sys.exit(2)
for opt, arg in opts:
    if opt == '-h':
        print ('test.py -a <address> ')
        sys.exit()
    elif opt in ("-a", "--address"):
        add = arg

conn = taos.connect(host=add, user="root", password="taosdata", config="/etc/taos")
c1 = conn.cursor()
c1.execute('use db')


with open(f"query_execution_time_3.log", 'w') as f:
    f.write("query execution start\n")
with open("queryList_3.txt", 'r') as queryFile:
    Lines = queryFile.readlines()

    for i in Lines:
            executeQueryCommand(i, c1)
            time.sleep(5)

c1.close()
conn.close()
