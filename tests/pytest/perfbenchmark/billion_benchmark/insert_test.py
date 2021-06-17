###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

import sys
import os
import subprocess
import time
import taos

from fabric import Connection
tableNum = 32765
rowNum = 100
conn1 = Connection("{}@{}".format('ubuntu', "192.168.1.86"),
                   connect_kwargs={"password": "{}".format('tbase125!')})
conn1.run("sudo systemctl stop taosd")

with conn1.cd('/data/taos/log'):
    conn1.run('sudo rm -rf *')

with conn1.cd('/data/taos/data'):
    conn1.run('sudo rm -rf *')
conn1.run("sudo systemctl start taosd")

conn2 = Connection("{}@{}".format('ubuntu', "192.168.1.86"),
                   connect_kwargs={"password": "{}".format('tbase125!')})
conn2.run("sudo systemctl stop taosd")

with conn2.cd('/data/taos/log'):
    conn2.run('sudo rm -rf *')

with conn2.cd('/data/taos/data'):
    conn2.run('sudo rm -rf *')
conn2.run("sudo systemctl start taosd")
time.sleep(10)

connTaos = taos.connect(host = '192.168.1.86', user = 'root', password = 'taosdata', cnfig = '/etc/taos')
c1 = connTaos.cursor()
c1.execute('create dnode \'bsc-2:6030\'')
time.sleep(5)
c1.close()
connTaos.close()



file_out = open('temp/insertOutput.csv', "w")
file_out.write(
    '# of node, runtime, table creation speed,# of rows,write speed/s\n')

with conn1.cd('~/bschang_test/TDinternal/community/tests/pytest/perfbenchmark/billion_benchmark'):
    result = conn1.run('sudo taosdemo -f temp/insert_test_create_billion.json > 1 > /dev/null')
    stderr = result.stderr.strip()
    print(stderr.split(' '))
    runtime = stderr.split(' ')[1]
    file_out.write(f'{tableNum},{runtime}, {tableNum/float(runtime)}\n')

    result = conn1.run('sudo taosdemo -f temp/insert_test_insert_billion.json > 1 > /dev/null')
    stderr = result.stderr.strip()
    timeIndex = [stderr.find('Spent') + 6, stderr.find('seconds') - 1]
    timeUsed = float(stderr[timeIndex[0]:timeIndex[1]])
    file_out.write(f'{tableNum},{timeUsed},,{tableNum*rowNum},{tableNum*rowNum/timeUsed}\n')

    ## the following is the query test asserting the query runtime without the 
    result = conn1.run('sudo taosdemo -f temp/query_billion_query.json')
    # stdout = result.stdout.strip()
    # print(stdout)

conn1.close()
conn2.close()