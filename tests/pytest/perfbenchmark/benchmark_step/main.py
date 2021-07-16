import sys
import os
import subprocess
import time
import taos
import threading
import execute_file
import datetime
import getopt

from fabric import Connection

# function for launching taosdemo query with thread  
def concurrentQuery(path, fileName):
    # os.system(
    #     f'sudo taosdemo -f {path}/{fileName}.json')
    print(f'sudo taosdemo -f {path}/{fileName}')


# function for letting the other ubuntu run taosdemo insert
def ConnThread(connection, ThreadID, taosdemo):
    with connection.cd('/root/TDinternal/community/tests/pytest/perfbenchmark/benchmark_step'):
        connection.run(f'sudo python3 execute_file_parallel.py -i {ThreadID} -t {taosdemo}')


# function for invoking multiple taosdemo to run query more than 100
def queryThread(round, path, fileName, file, concurrent = False,round2 = 0):
    threadList = []

    #create round number of thread for query
    for i in range(round):
        threadList.append(threading.Thread(
            target=concurrentQuery, args=(path, fileName,)))

    time_start = datetime.datetime.now()
    for i in range(len(threadList)):
        threadList[i].start()
    
    #if concurrent set to true, lanuch the second ubuntu and lanuch round2 number of taosdemo for query
    if concurrent:
        conn1 = Connection("{}@{}".format('root', '52.151.27.227'),
                connect_kwargs={"password": "{}".format('tbase125!')})
        with conn1.cd('/root/TDinternal/community/tests/pytest/perfbenchmark/benchmark_step'):
            conn1.run(f'sudo execute_query_file_parallel.py -q {fileName} -t {round2}')
    
    
    for i in range(len(threadList)):
        threadList[i].join()
    time_end = datetime.datetime.now()
    conn1.close()

    #writting log. For memory, please use grafana
    file.write(f"start time {time_start}\n")
    file.write(f"end time {time_end}\n")
    file.write(f"durination {time_end - time_start}\n\n\n")



testType = "none"
addr = '20.98.75.200'
try:
    opts, args = getopt.gnu_getopt(sys.argv, "ht:a:", ["type=,address="])
except getopt.GetoptError:
    print('test.py -t <testType>')
    sys.exit(2)
for opt, arg in opts:
    if opt == '-h':
        print('test.py -t <testType>')
        sys.exit()
    elif opt in ("-t", "--type"):
        testType = arg
    elif opt in ("-a", "--address"):
        addr = arg


f = open("output.log", "w")

if testType == "create":
    f.write("start running creation benchmark test")
    time_start = datetime.datetime.now()
    execute_file.executeCreatefile()
    time_end = datetime.datetime.now()
    f.write(f"start time {time_start}\n")
    f.write(f"end time {time_end}\n")
    f.write(f"durination {time_end - time_start}\n")

elif testType == "insert":
    time_start = datetime.datetime.now()
    execute_file.executeInsertfile(5)
    time_end = datetime.datetime.now()
    f.write("start running insertion benchmark test\n")
    f.write(f"start time {time_start}\n")
    f.write(f"end time {time_end}\n")
    f.write(f"durination {time_end - time_start}\n")

elif testType == "insertParallel":
    conn1 = Connection("{}@{}".format('root', '52.151.27.227'),
                   connect_kwargs={"password": "{}".format('tbase125!')})
    thread1 = threading.Thread(target=ConnThread, args=(conn1, 2,5))
    time_start = datetime.datetime.now()
    thread1.start()
    execute_file.executeInsertfileParallel(1,5)
    thread1.join()
    time_end = datetime.datetime.now()
    f.write(f"start time {time_start}\n")
    f.write(f"end time {time_end}\n")
    f.write(f"durination {time_end - time_start}\n")
    conn1.close()


elif testType == "query_normal":
    os.system(f'python3 multiIndividualQuery.py -a {addr}')

elif testType == "query_concurrent":
    path = '/root/TDinternal/community/tests/pytest/perfbenchmark/benchmark_step/JSON'
    f.write("start running concurrent query benchmark test\n")

    #test 4 query 1
    for i in range(4):
        f.write(f"1-{50*i+50}\n")
        queryThread(i+1,path, 'query_create_1_1.json', f)

    #test 4 query 2
    f.write("2-100\n")
    queryThread(1,path, 'query_create_2.json', f)

    f.write("2-500\n")
    queryThread(3,path, 'query_create_2.json', f,True,2)

    f.write("2-1000\n")
    queryThread(5,path, 'query_create_2.json', f,True, 5)

    f.write("2-5000\n")
    queryThread(25,path, 'query_create_2.json', f, True, 25)

    #test4 query 3
    f.write("3-100\n")
    queryThread(1,path, 'query_create_3.json', f)

    f.write("3-500\n")
    queryThread(3,path, 'query_create_3.json', f,True,2)

    f.write("3-1000\n")
    queryThread(10,path, 'query_create_3.json', f,True, 5)

    f.write("3-5000\n")
    queryThread(50,path, 'query_create_3.json', f, True, 25)

    #test4 query 4
    f.write("4-30%\n")
    queryThread(1,path, 'query_create_4_1.json', f)

    f.write("4-70%\n")
    queryThread(1,path, 'query_create_4_2.json', f)

    f.write("4-100%\n")
    queryThread(1,path, 'query_create_4_3.json', f)

    #test4 query 5
    for i in range(3):
        f.write(f"5-{50*i+50}\n")
        queryThread(i+1,path, 'query_create_5_1.json', f)

    #test4 query 6
    for i in range(4):
        f.write(f"6-{100*i+100}\n")
        queryThread(i+1,path, 'query_create_6.json', f)
    
    #test4 query 7
    for i in range(3):
        f.write(f"7-{50*i+50}\n")
        queryThread(i+1,path, 'query_create_7_1.json', f)

elif testType == "query_continous":
    path = '/root/TDinternal/community/tests/pytest/perfbenchmark/benchmark_step/JSON'
    f.write("start running concurrent query benchmark test with insert\n")
    execute_file.executeInsertfile_daemon(5)

    for i in range(7):
        for j in [1,2,4]:
            f.write(f"{i+1}-{50*j}\n")
            queryThread(j,path, f'query_create_5_{i+1}_1.json', f)
        f.write(f"{i+1}-{50*10}\n")
        queryThread(5,path, f'query_create_5_{i+1}_1.json', f,True,5)

elif testType == 'contious_query':
    execute_file.executeInsertfile_daemon(5)
    conn = taos.connect(host=addr, user="root", password="taosdata", config="/etc/taos")
    c1 = conn.cursor()
    c1.execute('use db')
    f.write(f"1 start time {datetime.datetime.now()}\n")
    c1.execute('create table stream1 as select last_row(*) from stb interval(1s);')
    time.sleep(300)
    f.write(f"2 start time {datetime.datetime.now()}\n")
    c1.execute('create table stream2 as select max(col2), min(col1), average(col3) from stb interval(10s) sliding (5s);')
    time.sleep(300)
    f.write(f"3 start time {datetime.datetime.now()}\n")
    c1.execute('create table stream3 as select max(col2), min(col1), average(col3) from stb where t0 = 1 interval(10s) sliding (5s);')
    time.sleep(300)
    f.write(f"4 start time {datetime.datetime.now()}\n")
    c1.execute('create table stream4 as select spread(col2) from stb where t1 = ‘beijing’ interval(7s);')
    time.sleep(300)
    f.write(f"5 start time {datetime.datetime.now()}\n")
    c1.execute('create table stream5 as select max(col2), min(col1), average(col3) from stb where t0 = 1 interval(10s) sliding (5s);')
    time.sleep(300)
    f.write(f"6 start time {datetime.datetime.now()}\n")
    c1.execute('create table stream6 as select count(*) from stb where ts > now - 10m and col1 > x interval (20s);')
    time.sleep(300)


f.close()
