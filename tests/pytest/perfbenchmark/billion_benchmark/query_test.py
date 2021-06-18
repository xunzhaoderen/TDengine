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
import threading
from fabric import Connection

def data_insert(conn):
    with conn.cd('~/bschang_test/TDinternal/community/tests/pytest/perfbenchmark/billion_benchmark'):
        print('running insert')
        conn.run(
            'sudo taosdemo -f temp/insert_test_insert_billion.json > 1 > /dev/null')
    

def data_query(conn):
    try:
        with conn.cd('~/bschang_test/TDinternal/community/tests/pytest/perfbenchmark/billion_benchmark'):
            print('running query')
            conn.run('sudo taosdemo -f temp/query_billion_query.json')
    except BaseException:
        pass


tableNum = 32765
rowNum = 100
conn1 = Connection("{}@{}".format('ubuntu', "192.168.1.125"),
                   connect_kwargs={"password": "{}".format('tbase125!')})
print('connection established')
# conn2 = Connection("{}@{}".format('ubuntu', "192.168.1.125"),
#                    connect_kwargs={"password": "{}".format('tbase125!')})

conn1.run("sudo systemctl stop taosd")
print('taosd stopped')
time.sleep(10)
conn1.run("sudo systemctl start taosd")
print('taosd started')
time.sleep(10)

insertThread = threading.Thread(target = data_insert, args = (conn1,))
queryThread = threading.Thread(target = data_query, args = (conn1,))
insertThread.start()
queryThread.start()
insertThread.join()
queryThread.join()
conn1.close()