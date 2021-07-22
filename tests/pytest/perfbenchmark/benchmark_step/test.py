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


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        self.jsonMaker = stepJsonGeneration('20.98.75.200', 5)


    def run(self):
        self.jsonMaker.csvGeneration(1, "1")
        self.jsonMaker.csvGeneration(2, "2")
        self.jsonMaker.csvGeneration(3, "3")
        self.jsonMaker.csvGeneration(4, "4")
        self.jsonMaker.create_JSON_gen(100000000,1)
        self.jsonMaker.insert_JSON_gen(5)
        self.jsonMaker.create_JSON_gen(10000000,1)
        self.jsonMaker.insert_JSON_gen(5)
        self.jsonMaker.create_JSON_gen(30000000,1)
        self.jsonMaker.insert_JSON_gen(5)
        self.jsonMaker.create_JSON_gen(70000000,1)
        self.jsonMaker.insert_JSON_gen(5)
        self.jsonMaker.query_JSON_gen(50, "select sum(col1), avg(col2) from stb", "1_1")
        self.jsonMaker.query_JSON_gen(100, "select sum(col1), avg(col2) from stb", "1_2")
        self.jsonMaker.query_JSON_gen(100, "select last_row(*) from stb group by t1", "2")
        self.jsonMaker.query_JSON_gen(100, "select last_row(*) from stb_0_10000", "2_1")
        self.jsonMaker.query_JSON_gen(100, "select last(col1) - first(col1) from stb where ts between '2021-07-15 04:00:00.000' and '2021-07-15 06:00:00.000'", "3")
        self.jsonMaker.query_JSON_gen(50, "select max(col0) from stb where t0 = '1' or t0 = '2' or t0 = '3' or t0 = '4' or t0 = '5' or t0 = '6' or t0 = '7' group by t1", "4_1")
        self.jsonMaker.query_JSON_gen(50, "select max(col0) from stb where t0 = 2 or t0 = 7 or t0 = 9 or t0 = 10 group by t1 ", "4_2")
        self.jsonMaker.query_JSON_gen(50, "select max(col0) from stb group by t1", "4_3")
        self.jsonMaker.query_JSON_gen(50, "select avg(col1), spread(col1) from stb interval (1m, 30s)", "5_1")
        self.jsonMaker.query_JSON_gen(100, "select avg(col1), spread(col1) from stb interval (1m, 30s)", "5_2")
        self.jsonMaker.query_JSON_gen_rand_table(100, "select ts, col1 from ", " limit 30", "6")
        self.jsonMaker.query_JSON_gen(50, "select sum(col1), avg(col2) from stb;", "5_1_1")
        self.jsonMaker.query_JSON_gen(50, "select last(col1) - first(col1) from stb where ts between '2020-10-01 00:00:00.000' and '2020-10-01 00:00:30.000';", "5_2_1")
        self.jsonMaker.query_JSON_gen(50, "select max(col0) from stb where t0 = '1' or t0 = '2' or t0 = '3' or t0 = '4' or t0 = '5' or t0 = '6' or t0 = '7' group by t1;", "5_3_1")
        self.jsonMaker.query_JSON_gen(50, "select max(col0) from stb where t0 = 2 or t0 = 7 or t0 = 9 or t0 = 10 group by t1;", "5_4_1")
        self.jsonMaker.query_JSON_gen(50, "select max(col0) from stb group by t1;", "5_5_1")
        self.jsonMaker.query_JSON_gen(50, "select avg(col1), spread(col1) from stb interval (1m, 30s);", "5_6_1")
        self.jsonMaker.query_JSON_gen(100, "select ts, col1 from stb_0_10000 limit 30;", "5_7_1")
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
