import sys
import os
import subprocess
import time
import taos
import threading
import getopt
import execute_file




## this is the py file that will lanuch in the clients other than the main client
id = 1
taosdemo = 5
addr = '20.98.75.200'
daemon = 'False'
tableNum = 100000000
try:
    opts, args = getopt.gnu_getopt(sys.argv, "hi:t:d:n:", ["id=,taosdemo=,daemon=,tableNum="])
except getopt.GetoptError:
    print('test.py -t <testType>')
    sys.exit(2)
for opt, arg in opts:
    if opt == '-h':
        print('test.py -t <testType>')
        sys.exit()
    elif opt in ("-i", "--id"):
        id = arg
    elif opt in ("-t", "--taosdemo"):
        taosdemo = arg
    elif opt in ("-d", "--daemon"):
        taosdemo = arg
    elif opt in ("-n", "--tableNum"):
        tableNum = arg

if daemon == 'False':
    execute_file.executeInsertfileParallel(id,taosdemo,int(tableNum))
else:
    execute_file.executeInsertfile_daemon(taosdemo,3)

