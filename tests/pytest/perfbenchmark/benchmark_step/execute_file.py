import sys
import os
import subprocess
import time
import taos
import threading

## taosdemo file execution
def executeFile(ThreadID, fileName, tableNum):
    path = '/root/TDinternal/community/tests/pytest/perfbenchmark/benchmark_step/JSON'
    os.system(
        f'sudo taosdemo -f {path}/{fileName}_{tableNum}_{ThreadID}.json 2>&1')
    print(f'sudo taosdemo -f {path}/{fileName}_{tableNum}_{ThreadID}.json')

## since for create, taosdemo does not consume too much memory space, all five taosdemos 
## are executed in the same machine through different thread
def executeCreatefile(tableNum):
    threadDic = []
    threadDic.append(threading.Thread(
        target=executeFile, args=(0, "insert_create",tableNum,)))
    threadDic.append(threading.Thread(
        target=executeFile, args=(1, "insert_create",tableNum,)))
    threadDic.append(threading.Thread(
        target=executeFile, args=(2, "insert_create",tableNum,)))
    threadDic.append(threading.Thread(
        target=executeFile, args=(3, "insert_create",tableNum,)))
    threadDic.append(threading.Thread(
        target=executeFile, args=(4, "insert_create",tableNum,)))

    for i in range(len(threadDic)):
        print(threadDic[i])
        threadDic[i].start()

    for i in range(len(threadDic)):
        print(threadDic[i])
        threadDic[i].join()

#start the thread to lanuch taosdemo
def executeInsertfile(taosdemoNum, tableNum):
    threadList = []
    for i in range(0, taosdemoNum):
        threadList.append(threading.Thread(
            target=executeFile, args=(i, "insert_insert",tableNum,)))

    for i in range(len(threadList)):
        print(threadList[i])
        threadList[i].start()

    for i in range(len(threadList)):
        print(threadList[i])
        threadList[i].join()

## lanuch three taosdemo for table creation in first taosdemo, lanuch two taosdemo for tablecreation in the second taosdemo
def executeCreatefileParallel(id, tableNum):
    threadDic = []
    if id == 1:
        threadDic.append(threading.Thread(
            target=executeFile, args=(0, "insert_create",tableNum,)))
        threadDic.append(threading.Thread(
            target=executeFile, args=(1, "insert_create",tableNum,)))
        threadDic.append(threading.Thread(
            target=executeFile, args=(2, "insert_create",tableNum,)))
    else:
        threadDic.append(threading.Thread(
            target=executeFile, args=(3, "insert_create",tableNum,)))
        threadDic.append(threading.Thread(
            target=executeFile, args=(4, "insert_create",tableNum,)))

    for i in range(len(threadDic)):
        print(threadDic[i])
        threadDic[i].start()

    for i in range(len(threadDic)):
        print(threadDic[i])
        threadDic[i].join()

## for received number for taosdemo, lanuch half of taosdemo in the first client, and lanuch half of them in the other client
## if the number is odd, first taosdemo will launch one more taosdemo.
def executeInsertfileParallel(id,taosdemoNum,tableNum):
    threadList = []
    if id == 1: 
        for i in range(0, int(taosdemoNum),2):
            threadList.append(threading.Thread(
                target=executeFile, args=(i, "insert_insert",tableNum,)))
    else:
        for i in range(1, int(taosdemoNum),2):
            threadList.append(threading.Thread(
                target=executeFile, args=(i, "insert_insert",tableNum,)))
    for i in range(len(threadList)):
        print(threadList[i])
        threadList[i].start()

    for i in range(len(threadList)):
        print(threadList[i])
        threadList[i].join()

## lanuch taosdemo in daemon thread. will stop if the caller program stops
def executeInsertfile_daemon(taosdemoNum, initial=0):
    threadList = []
    for i in range(initial, taosdemoNum):
        threadList.append(threading.Thread(
            target=executeFile, args=(i, "insert_insert",), daemon=True))

    for i in range(len(threadList)):
        print(threadList[i])
        threadList[i].start()

