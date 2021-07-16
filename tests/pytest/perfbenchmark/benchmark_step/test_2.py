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
from perfbenchmark.benchmark_step.json_generation import *
import os
import subprocess
import datetime
import getopt


class TDTestCase:
    def init(self, conn, logSql):
        self.testType = "create"
        self.address = '20.98.75.200'
        self.schema = '1'
        self.tableNum = '100000000'
        self.taosdemoNum = '5'
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        try:
            opts, args = getopt.gnu_getopt(sys.argv,"hf:t:a:s:b:d:",["testType=,address=,schema=,table=,taosdemoNum="])
            print(opts, "opts\n")
            print(args, "args\n")
        except getopt.GetoptError:
            print ('test.py -t <testType>')
            sys.exit(2)
        for opt, arg in opts:
            if opt == '-h':
                print ('test.py -t <testType> -a <address> -c <concurrent> -s <schema> -b <table> -d <taosdemoNum>')
                sys.exit()
            elif opt in ("-t", "--testType"):
                self.testType = arg
            elif opt in ("-a", "--address"):
                self.address = arg
            elif opt in ("-s", "--schema"):
                self.schema = arg
            elif opt in ("-b", "--table"):
                self.tableNum = arg
            elif opt in ("-d", "--taosdemoNum"):
                self.taosdemoNum = arg
        self.jsonMaker = stepJsonGeneration(self.address, int(self.tableNum))
    def run(self):
        pass
        if self.testType == 'create':
            self.jsonMaker.csvGeneration(1, "1")
            self.jsonMaker.csvGeneration(2, "2")
            self.jsonMaker.csvGeneration(3, "3")
            self.jsonMaker.csvGeneration(4, "4")
            self.jsonMaker.create_JSON_gen(self.tableNum,int(self.schema))
        elif self.testType == 'insert':
            self.jsonMaker.insert_JSON_gen(self.taosdemoNum)
        elif self.testType == 'query_concurrent':
            self.jsonMaker.query_JSON_gen(50, "select sum(col1), avg(col2) from stb", "1_1")
            self.jsonMaker.query_JSON_gen(100, "select sum(col1), avg(col2) from stb", "1_2")
            self.jsonMaker.query_JSON_gen(100, "select last_row(*) from stb group by tbname", "2")
            self.jsonMaker.query_JSON_gen(100, "select last(col1) – first(col1) from stb_x where ts between now – 5m and now", "3")
            self.jsonMaker.query_JSON_gen(50, "select max(*) from stb where t0 = 1 or t0 = 2 or t0 = 3 or t0 = 4 or t0 = 5 or t0 = 6 or t0 = 7 group by t1", "4_1")
            self.jsonMaker.query_JSON_gen(50, "select max(*) from stb where t0 = 2 or t0 = 7 or t0 = 9 or t0 = 10 group by t1 ", "4_2")
            self.jsonMaker.query_JSON_gen(50, "select max(*) from stb group by t1", "4_3")
            self.jsonMaker.query_JSON_gen(50, "select avg(col1), spread(col1) from stb interval (1m, 10s)", "5_1")
            self.jsonMaker.query_JSON_gen(100, "select avg(col1), spread(col1) from stb interval (1m, 10s)", "5_2")
            self.jsonMaker.query_JSON_gen_rand_table(100, "select ts, info from ", " limit 30", "6")
            self.jsonMaker.query_JSON_gen(50, "select top(col2, 10) from (select avg(col2) as col2 from stb group by tbname)", "7_1")
            self.jsonMaker.query_JSON_gen(100, "select top(col2, 10) from (select avg(col2) as col2 from stb group by tbname)", "7_2")
        elif self.testType == 'query_conncurrent_write':
            self.jsonMaker.insert_JSON_gen(self.taosdemoNum)
            self.jsonMaker.query_JSON_gen(50, "select bottom(col0) from (select avg(col0) as col0 from stb interval (1m) sliding (30s))", "1_1")
            self.jsonMaker.query_JSON_gen(100, "select bottom(col0) from (select avg(col0) as col0 from stb interval (1m) sliding (30s))", "1_2")
            self.jsonMaker.query_JSON_gen(50, "select avg(col0),sum(col1), max(col2), min(col3) from stb interval(100s)", "2_1")
            self.jsonMaker.query_JSON_gen(100, "select avg(col0),sum(col1), max(col2), min(col3) from stb interval(100s)", "2_2")
            self.jsonMaker.query_JSON_gen(50, "select last (*) from stb group by tbname limit 1000000", "3_1")
            self.jsonMaker.query_JSON_gen(100, "select last (*) from stb group by tbname limit 1000000", "3_2")
            self.jsonMaker.query_JSON_gen(50, "select count(*) from stb where ts > now – 10m and col1 > x", "4_1")
            self.jsonMaker.query_JSON_gen(100, "select count(*) from stb where ts > now – 10m and col1 > x", "4_2")
            self.jsonMaker.query_JSON_gen(50, "select top(col3, 10) from stb where t1 = “beijing”", "5_1")
            self.jsonMaker.query_JSON_gen(100, "select top(col3, 10) from stb where t1 = “beijing”", "5_2")
            self.jsonMaker.query_JSON_gen(50, "select avg(col0),sum(col1), max(col2), min(col3) from stb where t0 = 1", "6_1")
            self.jsonMaker.query_JSON_gen(100, "select avg(col0),sum(col1), max(col2), min(col3) from stb where t0 = 1", "6_2")
            self.jsonMaker.query_JSON_gen(50, "select last_row(*) from stb;", "7_1")
            self.jsonMaker.query_JSON_gen(100, "select last_row(*) from stb;", "7_2")
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
