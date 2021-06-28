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
        self.IP = '127.0.0.1'

    def run(self):

        tdSql.prepare()
        binPath = tdFindPath.getTaosdemoPath()

        # template for table creation speed test
        insertTemplate = taosdemoCfg.get_template('insert_stbs')
        insertTemplate['childtable_count'] = 100
        insertTemplate['insert_rows'] = 0
        insertTemplate['tags_file'] = './tags.csv'
        insertTemplate['columns'] = [
            {'type': 'DOUBLE', 'count': 2}, {'type': 'int', 'count': 2}]
        insertTemplate['tags'] = [{'type': 'bigint', 'count': 1}, {
            "type": "BINARY", "len": 32, "count": 2}]
        insertTemplate['batch_create_tbl_num'] = 25
        taosdemoCfg.alter_db('keep', 3650)
        taosdemoCfg.alter_insert_cfg('host', self.IP)
        taosdemoCfg.append_sql_stb('insert_stbs', insertTemplate)

        cfgFileName = taosdemoCfg.generate_insert_cfg(
            'perfbenchmark/benchmark_step/temp', 'test_create_billion')

        # template for table insertion speed test
        insertTemplate = taosdemoCfg.get_template('insert_stbs')
        insertTemplate['childtable_count'] = 100
        insertTemplate['child_table_exists'] = 'yes'
        insertTemplate['insert_rows'] = 100
        insertTemplate['childtable_limit'] = 32765
        insertTemplate['interlace_rows'] = 1
        insertTemplate['insert_interval'] = 1000
        insertTemplate['tags_file'] = './tags.csv'
        insertTemplate['columns'] = [
            {'type': 'DOUBLE', 'count': 2}, {'type': 'int', 'count': 2}]
        insertTemplate['tags'] = [{'type': 'bigint', 'count': 1}, {
            "type": "BINARY", "len": 32, "count": 2}]
        insertTemplate['timestamp_step'] = 60

        taosdemoCfg.import_stbs([insertTemplate])
        taosdemoCfg.alter_db('drop', 'no')
        cfgFileName = taosdemoCfg.generate_insert_cfg(
            'perfbenchmark/benchmark_step/temp', 'test_insert_billion')


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
