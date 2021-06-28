import sys
import os
import subprocess
import time
import taos
import threading

from fabric import Connection


def ConnThread(connection, ThreadID):
    with connection.cd('~/TDengine/tests/pytest/perfbenchmark/billion_benchmark'):
        connection.run(f'sudo python3 concurrent_insert_server_{ThreadID}.py')


def selfThread(ThreadID):
    os.system(
        f'sudo taosdemo -f /home/bryan/Documents/Github/TDinternal/community/tests/pytest/perfbenchmark/billion_benchmark/temp/insert_test_insert_volume{ThreadID}.json > 1 > /dev/null')


threadDic = []
IP1 = '192.168.1.179'
IP2 = '192.168.1.180'
conn1 = Connection("{}@{}".format('ubuntu', IP1),
                   connect_kwargs={"password": "{}".format('tbase125!')})
conn2 = Connection("{}@{}".format('ubuntu', IP2),
                   connect_kwargs={"password": "{}".format('tbase125!')})


for i in range(0, 10):
    threadDic.append(threading.Thread(target=selfThread, args=(i,)))

threadDic.append(threading.Thread(target = ConnThread, args = (conn1,1,)))
threadDic.append(threading.Thread(target = ConnThread, args = (conn2,2,)))

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
conn1.close()
conn2.close()
