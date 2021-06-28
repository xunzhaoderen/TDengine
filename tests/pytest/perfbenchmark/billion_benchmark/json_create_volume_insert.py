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

    def create_single_file(self, tableNum, rowInsert, fileNum, stbCfg):
        stbCfg['child_table_exists'] = 'yes'
        stbCfg['insert_rows'] = rowInsert
        stbCfg['childtable_limit'] = tableNum
        stbCfg['childtable_offset'] = fileNum * tableNum
        stbCfg['columns'] = [
            {'type': 'DOUBLE', 'count': 2}, {'type': 'int', 'count': 3}]
        stbCfg['tags'] = [{'type': 'bigint', 'count': 1}, {
            "type": "BINARY", "len": 32, "count": 1}]
        taosdemoCfg.import_stbs([stbCfg])
        taosdemoCfg.generate_insert_cfg(
            'perfbenchmark/billion_benchmark/temp', f'test_insert_volume{fileNum}')

    def run(self):
        total_table = 100000000
        table_per_insert = 2000000
        row_insert = 1000
        host = '192.168.1.86'
        stbCfg = taosdemoCfg.get_template('insert_stbs')

        stbCfg['childtable_count'] = total_table
        taosdemoCfg.alter_insert_cfg('host', host)
        taosdemoCfg.alter_db('drop', 'no')
        for i in range(0,total_table//table_per_insert):
            taosdemoCfg.alter_insert_cfg('result_file', f"./insert_res{i}.txt")
            self.create_single_file(table_per_insert, row_insert,
                                    i, stbCfg)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
