import sys
import os
import subprocess
import time
import taos
import threading



def selfThread(ThreadID):
        path = '/home/ubuntu/TDinternal/community/tests/pytest/perfbenchmark/billion_benchmark/'
        os.system(f'sudo taosdemo -f {path}temp/insert_test_insert_volume{ThreadID}.json > 1 > /dev/null')

threadDic = []

for i in range(10,30):
    threadDic.append(threading.Thread(target = selfThread, args = (i,)))


for i in range(len(threadDic)):
    print(threadDic[i])
    threadDic[i].start()

for i in range(len(threadDic)):
    print(threadDic[i])
    threadDic[i].join()

# singleThread = threading.Thread(target = ConnThread, args = (conn1,1,))
# singleThread.start()
# with conn1.cd('~/bschang_test/TDinternal/community/tests/pytest/perfbenchmark/billion_benchmark'):
#     conn1.run(f'taosdemo -f temp/insert_test_insert_volume{2}.json ')
os.system('rm -rf *.txt')
