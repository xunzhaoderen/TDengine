import os
import subprocess
import datetime
import string
import random
import taos
import sys
import getopt
import time

def executeQueryCommand(query):
    with open("query_execution_time.log", 'a') as f:
        f.write(f"execute\t{query}\n")
        timeStart = datetime.datetime.now()
        c1.execute('select count(tbname) from stb')
        timeEnd = datetime.datetime.now()
        f.write(f"start time:\t {timeStart}\n")
        f.write(f"end time:\t {timeEnd}\n\n")
        f.write(f"time passed :\t {timeEnd - timeStart}\n\n")     

add = 'localhost'
try:
    opts, args = getopt.gnu_getopt(sys.argv,"ha:o:",["address="])
except getopt.GetoptError:
    print ('test.py -i <inputfile> -o <outputfile>')
    sys.exit(2)
for opt, arg in opts:
    if opt == '-h':
        print ('test.py -a <address> -o <address>')
        sys.exit()
    elif opt in ("-a", "--address"):
        add = arg

conn = taos.connect(host=add, user="root", password="taosdata", config="/etc/taos")
c1 = conn.cursor()
c1.execute('use db')

with open(f"query_execution_time.log", 'w') as f:
    f.write("query execution start\n")
with open("queryList_3.txt", 'r') as queryFile:
    Lines = queryFile.readlines()

    for i in Lines:
        executeQueryCommand(i)

c1.close()
conn.close()
