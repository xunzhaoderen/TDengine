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
from util.log import *
from util.cases import *
from util.sql import *
from util.taosdemoCfg import *
from util.pathFinding import *
import os
import subprocess
import datetime 


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()
        file_out = open('perfbenchmark/billion_benchmark/temp/insertOutput.csv', "w")
        file_out.write('# of node, runtime, table creation speed,# of rows,write speed/s\n')
        binPath = tdFindPath.getTaosdemoPath()

        ##template for table creation speed test
        insertTemplate = taosdemoCfg.get_template('insert_stbs')
        insertTemplate['childtable_count'] = 10000
        insertTemplate['insert_rows'] = 0
        insertTemplate['columns'] = [{'type':'DOUBLE', 'count':1}]
        taosdemoCfg.append_sql_stb('insert_stbs', insertTemplate)
        cfgFileName = taosdemoCfg.generate_insert_cfg('perfbenchmark/billion_benchmark/temp','test_create')
        p = subprocess.Popen([f"{binPath}taosdemo", "-f", f"{cfgFileName}"], stdout = subprocess.DEVNULL, stderr=subprocess.PIPE) 
        stderr = p.communicate()
        runtime = stderr[1].decode('utf-8').split(' ')[1]
        file_out.write(f'10000,, {10000/float(runtime)}\n')

        ##template for table insertion speed test
        insertTemplate = taosdemoCfg.get_template('insert_stbs')
        insertTemplate['childtable_count'] = 10000
        insertTemplate['child_table_exists'] = 'yes'
        insertTemplate['insert_rows'] = 10
        insertTemplate['childtable_limit'] = 10000

        taosdemoCfg.import_stbs([insertTemplate])
        taosdemoCfg.alter_db('drop', 'no')
        cfgFileName = taosdemoCfg.generate_insert_cfg('perfbenchmark/billion_benchmark/temp','test_insert')
        p = subprocess.Popen([f"{binPath}taosdemo", "-f", f"{cfgFileName}"], stdout = subprocess.DEVNULL, stderr=subprocess.PIPE) 
        stderr = p.communicate()
        stderr = stderr[1].decode('utf-8')
        timeIndex = [stderr.find('Spent') + 6, stderr.find('seconds') - 1]
        timeUsed = float(stderr[timeIndex[0]:timeIndex[1]])
        file_out.write(f'10000,{timeUsed},,{10000*10},{10000*10/timeUsed}\n')

        ##template for table creation && insertion speed test
        insertTemplate = taosdemoCfg.get_template('insert_stbs')
        taosdemoCfg.alter_db('drop', 'yes')
        insertTemplate['childtable_count'] = 10000
        insertTemplate['insert_rows'] = 10
        insertTemplate['columns'] = [{'type':'DOUBLE', 'count':1}]
        taosdemoCfg.import_stbs([insertTemplate])
        cfgFileName = taosdemoCfg.generate_insert_cfg('perfbenchmark/billion_benchmark/temp','test_CI')
        p = subprocess.Popen([f"{binPath}taosdemo", "-f", f"{cfgFileName}"], stdout = subprocess.DEVNULL, stderr=subprocess.PIPE) 
        stderr = p.communicate()
        stderr = stderr[1].decode('utf-8').split('\n\n')
        createTimeIndex = [stderr[0].find('Spent') + 6, stderr[0].find('seconds') - 1]
        insertTimeIndex = [stderr[1].find('Spent') + 6, stderr[1].find('seconds') - 1]
        createTimeIndex = float(stderr[0][createTimeIndex[0]:createTimeIndex[1]])
        insertTimeIndex = float(stderr[1][insertTimeIndex[0]:insertTimeIndex[1]])
        file_out.write(f'10000,{insertTimeIndex},{10000/createTimeIndex},{10000*10},{10000*10/insertTimeIndex}\n')

        file_out.close()


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
