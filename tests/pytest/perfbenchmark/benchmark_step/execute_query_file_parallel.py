import sys
import os
import subprocess
import time
import taos
import threading
import getopt



def executeQueryFile(query):
    path = '/root/TDinternal/community/tests/pytest/perfbenchmark/benchmark_step/JSON'
    # os.system(
    #     f'sudo taosdemo -f {path}/{query}.json')
    print(f'sudo taosdemo -f {path}/{query}.json')


id = 1
query = ""
taosdemo = 2
try:
    opts, args = getopt.gnu_getopt(sys.argv, "hq:t:", ["query=,taosdemo="])
except getopt.GetoptError:
    print('test.py -t <testType>')
    sys.exit(2)
for opt, arg in opts:
    if opt == '-h':
        print('test.py -t <testType>')
        sys.exit()
    elif opt in ("-q", "--query"):
        id = arg
    elif opt in ("-t", "--taosdemo"):
        taosdemo = arg

#time.sleep(30)
threadList = []
for i in range(0, int(taosdemo)):
    threadList.append(threading.Thread(
        target=executeQueryFile, args=(query,)))

for i in range(len(threadList)):
    print(threadList[i])
    threadList[i].start()

for i in range(len(threadList)):
    print(threadList[i])
    threadList[i].join()

