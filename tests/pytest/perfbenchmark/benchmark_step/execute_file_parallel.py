import sys
import os
import subprocess
import time
import taos
import threading
import getopt
import execute_file





id = 1
taosdemo = 5
addr = '20.98.75.200'
try:
    opts, args = getopt.gnu_getopt(sys.argv, "hi:t:", ["id=,taosdemo="])
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


execute_file.executeInsertfileParallel(id,taosdemo)

