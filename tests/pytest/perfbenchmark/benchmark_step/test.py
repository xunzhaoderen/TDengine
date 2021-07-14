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
        self.jsonMaker = stepJsonGeneration('20.98.75.200', 100000000)


    def run(self):
        self.jsonMaker.csvGeneration(1, "1")
        self.jsonMaker.csvGeneration(2, "2")
        self.jsonMaker.csvGeneration(3, "3")
        self.jsonMaker.csvGeneration(4, "4")
        self.jsonMaker.create_JSON_gen(100000000,1)
        self.jsonMaker.insert_JSON_gen(5)
        self.jsonMaker.query_JSON_gen(100, "select count(*) from stb", 1)
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
