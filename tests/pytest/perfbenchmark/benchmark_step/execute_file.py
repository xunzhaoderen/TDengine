import sys
import os
import subprocess
import time
import taos
import threading


def executeFile(ThreadID, fileName):
    path = '/home/ubuntu/TDinternal/community/tests/pytest/perfbenchmark/billion_benchmark/benchmark_step/JSON'
    os.system(
        f'sudo taosdemo -f {path}/{fileName}_{ThreadID}.json > 1 > /dev/null')


def executeCreatefile(id):
    threadDic = []
    if id == 1:
        threadDic.append(threading.Thread(target=executeFile, args=(0,"insert_create",)))
        threadDic.append(threading.Thread(target=executeFile, args=(1,"insert_create",)))
        threadDic.append(threading.Thread(target = executeFile, args = (2,"insert_create",)))
    else:
        threadDic.append(threading.Thread(target=executeFile, args=(3,"insert_create",)))
        threadDic.append(threading.Thread(target = executeFile, args = (4,"insert_create",)))

    for i in range(len(threadDic)):
        print(threadDic[i])
        threadDic[i].start()

    for i in range(len(threadDic)):
        print(threadDic[i])
        threadDic[i].join()

def executeInsertfile(id, taosdemoNum):
    threadList = []
    if id == 1:
        for i in range(0,taosdemoNum,2):
            print(i)
            threadList.append(threading.Thread(target=executeFile, args=(i,"insert_insert",)))
    else:
        for i in range(1,taosdemoNum,2):
            print(i)
            threadList.append(threading.Thread(target=executeFile, args=(i,"insert_insert",)))
    
    for i in range(len(threadList)):
        print(threadList[i])
        threadList[i].start()

    for i in range(len(threadList)):
        print(threadList[i])
        threadList[i].join()


