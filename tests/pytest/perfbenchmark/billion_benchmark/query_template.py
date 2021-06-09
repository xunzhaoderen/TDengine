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
        file_out = open('perfbenchmark/billion_benchmark/temp/queryOutput.csv', "w")
        file_out.write('# of node, runtime, table creation speed,# of rows,write speed/s\n')
        binPath = tdFindPath.getTaosdemoPath()

        insertTemplate = taosdemoCfg.get_template('insert_stbs')
        taosdemoCfg.alter_db('drop', 'yes')
        insertTemplate['childtable_count'] = 10000
        insertTemplate['insert_rows'] = 10
        insertTemplate['columns'] = [{'type':'DOUBLE', 'count':1}]
        taosdemoCfg.import_stbs([insertTemplate])
        cfgFileName = taosdemoCfg.generate_insert_cfg('perfbenchmark/billion_benchmark/temp','test_CI')
        p = subprocess.Popen([f"{binPath}taosdemo", "-f", f"{cfgFileName}"], stdout = subprocess.DEVNULL, stderr=subprocess.PIPE) 
        p.communicate()

        ##template for table creation speed test
        queryTemplate = taosdemoCfg.get_template('query_table')
        queryTemplate["result"] = "perfbenchmark/billion_benchmark/temp/query_res0.txt"
        taosdemoCfg.append_sql_stb('query_table', queryTemplate)
        # queryTemplate = taosdemoCfg.get_template('query_stable')
        # queryTemplate["result"] = "perfbenchmark/billion_benchmark/temp/query_res2.txt"
        # taosdemoCfg.append_sql_stb('query_stable', queryTemplate)
        cfgFileName = taosdemoCfg.generate_query_cfg('perfbenchmark/billion_benchmark/temp','test')
        print(cfgFileName)
        p = subprocess.Popen([f"{binPath}taosdemo", "-f", f"{cfgFileName}"]) 
        stderr = p.communicate()
        print(stderr)
        file_out.close()


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
