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
        self.IP = '192.168.1.86'

    def run(self):

        tdSql.prepare()
        file_out = open(
            'perfbenchmark/billion_benchmark/temp/insertOutput.csv', "w")
        file_out.write(
            '# of node, runtime, table creation speed,# of rows,write speed/s\n')
        binPath = tdFindPath.getTaosdemoPath()

        # template for table creation speed test
        insertTemplate = taosdemoCfg.get_template('insert_stbs')
        insertTemplate['childtable_count'] = 100000000
        insertTemplate['insert_rows'] = 0
        insertTemplate['columns'] = [
            {'type': 'DOUBLE', 'count': 2}, {'type': 'int', 'count': 2}]
        insertTemplate['tags'] = [{'type': 'bigint', 'count': 1}, {
            "type": "BINARY", "len": 32, "count": 1}]
        insertTemplate['batch_create_tbl_num'] = 25
        taosdemoCfg.alter_db('keep', 3650)
        taosdemoCfg.alter_insert_cfg('host', self.IP)
        taosdemoCfg.append_sql_stb('insert_stbs', insertTemplate)

        cfgFileName = taosdemoCfg.generate_insert_cfg(
            'perfbenchmark/billion_benchmark/JSON', 'test_create_billion')

        # template for table insertion speed test
        insertTemplate = taosdemoCfg.get_template('insert_stbs')
        insertTemplate['childtable_count'] = 32765
        insertTemplate['child_table_exists'] = 'yes'
        insertTemplate['insert_rows'] = 10000
        insertTemplate['childtable_limit'] = 32765
        insertTemplate['interlace_rows'] = 1
        insertTemplate['insert_interval'] = 1000
        insertTemplate['columns'] = [
            {'type': 'DOUBLE', 'count': 2}, {'type': 'int', 'count': 2}]
        insertTemplate['tags'] = [{'type': 'bigint', 'count': 1}, {
            "type": "BINARY", "len": 32, "count": 1}]
        insertTemplate['timestamp_step'] = 60

        taosdemoCfg.import_stbs([insertTemplate])
        taosdemoCfg.alter_db('drop', 'no')
        cfgFileName = taosdemoCfg.generate_insert_cfg(
            'perfbenchmark/billion_benchmark/JSON', 'test_insert_billion')


        insertTemplate = taosdemoCfg.get_template('insert_stbs')
        insertTemplate['childtable_count'] = 32765
        insertTemplate['child_table_exists'] = 'yes'
        insertTemplate['insert_rows'] = 64800
        insertTemplate['childtable_limit'] = 32766
        insertTemplate['interlace_rows'] = 1
        insertTemplate['insert_interval'] = 1000
        insertTemplate['columns'] = [
            {'type': 'DOUBLE', 'count': 2}, {'type': 'int', 'count': 2}]
        insertTemplate['tags'] = [{'type': 'bigint', 'count': 1}, {
            "type": "BINARY", "len": 32, "count": 1}]
        insertTemplate['timestamp_step'] = 60

        taosdemoCfg.import_stbs([insertTemplate])
        taosdemoCfg.alter_db('drop', 'no')
        cfgFileName = taosdemoCfg.generate_insert_cfg(
            'perfbenchmark/billion_benchmark/JSON', 'test_insert_billion_continue')


        # template for table creation speed test
        queryTemplate = {
            "sql": "select count(*) from stb ",
            "result": "temp/query_1.txt"
        }
        taosdemoCfg.append_sql_stb('query_table', queryTemplate)
        queryTemplate = {
            "sql": "select avg(col0), max(col1), min(col2) from stb ",
            "result": "temp/query_2.txt"
        }
        taosdemoCfg.append_sql_stb('query_table', queryTemplate)
        queryTemplate = {
            "sql": "select avg(col0), max(col1), min(col2) from stb interval(10s)",
            "result": "temp/query_3.txt"
        }
        taosdemoCfg.append_sql_stb('query_table', queryTemplate)
        queryTemplate = {
            "sql": "select last_row(*) from stb",
            "result": "temp/query_4.txt"
        }
        taosdemoCfg.append_sql_stb('query_table', queryTemplate)
        queryTemplate = {
            "sql": "select * from stb",
            "result": "temp/query_5.txt"
        }
        taosdemoCfg.append_sql_stb('query_table', queryTemplate)
        queryTemplate = {
            "sql": "select avg(col0), max(col1), min(col2) from stb where ts >= '2020-10-10 00:00:00.000' and ts <= '2020-10-20 00:00:00.000'",
            "result": "temp/query_6.txt"
        }
        taosdemoCfg.append_sql_stb('query_table', queryTemplate)

        queryTemplate = {
            "sql": "select count(*) from stb where t1 = 'beijing'",
            "result": "temp/query_7.txt"
        }
        taosdemoCfg.append_sql_stb('query_table', queryTemplate)

        taosdemoCfg.alter_query_cfg('query_times', 1)
        taosdemoCfg.alter_query_tb("concurrent", 1)
        print(taosdemoCfg.get_tb_query())
        cfgFileName = taosdemoCfg.generate_query_cfg(
            'perfbenchmark/billion_benchmark/JSON', 'billion_query')

        subTemplate = {
            "sql": "select * from stb;",
            "result": "temp/subscribe_res0.txt"
        }
        taosdemoCfg.append_sql_stb('sub_table', subTemplate)
        cfgFileName = taosdemoCfg.generate_subscribe_cfg('perfbenchmark/billion_benchmark/JSON','test_billion')

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
