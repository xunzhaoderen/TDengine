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

    def stable_creation(self, iteration, startNum, insertTemplate):
        iterated = iteration - 1
        stbNum = startNum + 1
        selfTemplate = insertTemplate

        selfTemplate['name'] = f'stb{stbNum}'
        selfTemplate['childtable_count'] = 10
        selfTemplate['childtable_prefix'] = f'stb{stbNum}_'
        selfTemplate['batch_create_tbl_num'] = 25
        selfTemplate['insert_rows'] = iterated
        selfTemplate['columns'] = [{"type": "INT", "count": 2}, {
            "type": "DOUBLE", "count": 2}, {"type": "BINARY", "len": 32, "count": 1}]
        selfTemplate['tags'] = [{"type": "INT", "count": 2}, {
            "type": "BINARY", "len": 32, "count": 1}]
        taosdemoCfg.append_sql_stb("insert_stbs", selfTemplate)
        if iterated != 0:
            self.stable_creation(int(iterated), int(stbNum), dict(insertTemplate))

    def run(self):
        table_creation = 10000
        tdSql.prepare()
        taosdemoCfg.alter_db('replica', 1)
        stbTemplate = dict(taosdemoCfg.get_template("insert_stbs"))
        tdSql.prepare()
        taosdemoCfg.alter_db('replica', 1)
        self.stable_creation(table_creation,0,dict(stbTemplate))
        taosdemoCfg.generate_insert_cfg(
            'perfbenchmark/billion_benchmark/million_stable/temp', f'{table_creation}_stb')
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
