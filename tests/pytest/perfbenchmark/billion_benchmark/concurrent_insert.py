import sys
import os
import subprocess
import time
import taos
import threading

from fabric import Connection
def ConnThread(connection, ThreadID):
    with connection.cd('~/bschang_test/TDinternal/community/tests/pytest/perfbenchmark/billion_benchmark'):
        connection.run(f'sudo taosdemo -f temp/insert_test_insert_volume{ThreadID}.json > 1 > /dev/null')

def selfThread(ThreadID):
        os.system(f'sudo taosdemo -f temp/insert_test_insert_volume{ThreadID}.json > 1 > /dev/null')

threadDic = []
IP1 = '192.168.1.125'
IP2 = '192.168.1.126'
conn1 = Connection("{}@{}".format('ubuntu', IP1),
                   connect_kwargs={"password": "{}".format('tbase125!')})
conn1.run("sudo systemctl stop taosd")

conn1.run("sudo systemctl start taosd")

conn2 = Connection("{}@{}".format('ubuntu', IP2),
                   connect_kwargs={"password": "{}".format('tbase125!')})
conn2.run("sudo systemctl stop taosd")

conn2.run("sudo systemctl start taosd")
time.sleep(10)

connTaos = taos.connect(host = IP1, user = 'root', password = 'taosdata', cnfig = '/etc/taos')
c1 = connTaos.cursor()
try:
    c1.execute('create dnode \'lyq-2:6030\'')
except BaseException:
    pass
time.sleep(5)
c1.close()
connTaos.close()

# for i in range(20):
#     threadDic.append(threading.Thread(target = ConnThread, args = (conn1,i,)))

# for i in range(20,40):
#     threadDic.append(threading.Thread(target = ConnThread, args = (conn2,i,)))


# for i in range(40,50):
#     threadDic.append(threading.Thread(target = selfThread, args = (i,)))

# for i in range(len(threadDic)):
#     print(threadDic[i])
#     threadDic[i].start()

# singleThread = threading.Thread(target = ConnThread, args = (conn1,1,))
# singleThread.start()
with conn1.cd('~/bschang_test/TDinternal/community/tests/pytest/perfbenchmark/billion_benchmark'):
    conn1.run(f'taosdemo -f temp/insert_test_insert_volume{2}.json ')
conn1.close()
conn2.close()