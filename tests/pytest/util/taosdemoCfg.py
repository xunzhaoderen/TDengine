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
import time
import datetime
import inspect
import psutil
import shutil
import json
from log import *
from multiprocessing import cpu_count


class TDTaosdemoCfg:
    def __init__(self):
        self.insert_cfg = {
            "filetype": "insert",
            "cfgdir": "/etc/taos",
            "host": "127.0.0.1",
            "port": 6030,
            "user": "root",
            "password": "taosdata",
            "thread_count": cpu_count(),
            "thread_count_create_tbl": cpu_count(),
            "result_file": "/tmp/insert_res.txt",
            "confirm_parameter_prompt": "no",
            "insert_interval": 0,
            "num_of_records_per_req": 32766,
            "max_sql_len": 32766,
            "databases": None
        }

        self.db = {
            "name": 'db',
            "drop": 'yes',
            "replica": 1,
            "days": 10,
            "cache": 16,
            "blocks": 8,
            "precision": "ms",
            "keep": 3650,
            "minRows": 100,
            "maxRows": 4096,
            "comp": 2,
            "walLevel": 1,
            "cachelast": 0,
            "quorum": 1,
            "fsync": 3000,
            "update": 0
        }

        self.query_cfg = {
            "filetype": "query",
            "cfgdir": "/etc/taos",
            "host": "127.0.0.1",
            "port": 6030,
            "user": "root",
            "password": "taosdata",
            "confirm_parameter_prompt": "no",
            "databases": "db",
            "query_times": 2,
            "query_mode": "taosc",
            "specified_table_query": None,
            "super_table_query": None
        }

        self.table_query = {
            "query_interval": 1,
            "concurrent": 3,
            "sqls": None
        }

        self.stable_query = {
            "stblname": "stb",
            "query_interval": 1,
            "threads": 3,
            "sqls": None
        }

        self.sub_cfg = {
            "filetype": "subscribe",
            "cfgdir": "/etc/taos",
            "host": "127.0.0.1",
            "port": 6030,
            "user": "root",
            "password": "taosdata",
            "databases": "db",
            "confirm_parameter_prompt": "no",
            "specified_table_query": None,
            "super_table_query": None
        }

        self.table_sub = {
            "concurrent": 1,
            "mode": "sync",
            "interval": 0,
            "restart": "yes",
            "keepProgress": "yes",
            "sqls": None
        }

        self.stable_sub = {
            "stblname": "stb",
            "threads": 1,
            "mode": "sync",
            "interval": 10000,
            "restart": "yes",
            "keepProgress": "yes",
            "sqls": None
        }

        self.stbs = []
        self.stb_template = {
            "name": "stb",
            "child_table_exists": "no",
            "childtable_count": 100,
            "childtable_prefix": "stb_",
            "auto_create_table": "no",
            "batch_create_tbl_num": 5,
            "data_source": "rand",
            "insert_mode": "taosc",
            "insert_rows": 100,
            "childtable_limit": 10,
            "childtable_offset": 100,
            "interlace_rows": 32766,
            "insert_interval": 0,
            "max_sql_len": 32766,
            "disorder_ratio": 0,
            "disorder_range": 1000,
            "timestamp_step": 1,
            "start_timestamp": "2020-10-01 00:00:00.000",
            "sample_format": "csv",
            "sample_file": "./sample.csv",
            "tags_file": "",
            "columns": [{"type": "INT", "count": 1}],
            "tags": [{"type": "BIGINT", "count": 1}]
        }

        self.tb_query_sql = []
        self.tb_query_sql_template = {
            "sql": "select last_row(*) from stb_0 ",
            "result": "temp/query_res0.txt"
        }

        self.stb_query_sql = []
        self.stb_query_sql_template = {
            "sql": "select last_row(ts) from xxxx",
            "result": "temp/query_res2.txt"
        }

        self.tb_sub_sql = []
        self.tb_sub_sql_template = {
            "sql": "select * from stb_0 ;",
            "result": "temp/subscribe_res0.txt"
        }

        self.stb_sub_sql = []
        self.stb_sub_sql_template = {
            "sql": "select * from xxxx where ts > '2021-02-25 11:35:00.000' ;",
            "result": "temp/subscribe_res1.txt"
        }

    # The following functions are import functions for different dicts and lists
    def importinsert_cfg(self, dict_in):
        self.insert_cfg = dict_in

    def import_db(self, dict_in):
        self.db = dict_in

    def import_stb(self, dict_in):
        self.defaultStb = dict_in

    def import_query_cfg(self, dict_in):
        self.query_cfg = dict_in

    def import_table_query(self, dict_in):
        self.table_query = dict_in

    def import_stable_query(self, dict_in):
        self.stable_query = dict_in

    def import_sub_cfg(self, dict_in):
        self.sub_cfg = dict_in

    def import_table_sub(self, dict_in):
        self.table_sub = dict_in

    def import_stable_sub(self, dict_in):
        self.stable_sub = dict_in

    def import_sql(self, Sql_in, mode):
        if mode == 'query_table':
            self.tb_query_sql = Sql_in
        elif mode == 'query_stable':
            self.stb_query_sql = Sql_in
        elif mode == 'query_all':
            self.stb_query_sql = Sql_in
            self.tb_query_sql = Sql_in
        elif mode == 'sub_table':
            self.tb_sub_sql = Sql_in
        elif mode == 'sub_stable':
            self.stb_sub_sql = Sql_in
        elif mode == 'sub_all':
            self.tb_sub_sql = Sql_in
            self.stb_sub_sql = Sql_in

    # The following functions are alter functions for different dicts
    def alter_insert_cfg(self, key, value):
        if key == 'databases':
            self.insert_cfg[key] = [
                {
                    'dbinfo': self.db,
                    'super_tables': self.stbs
                }
            ]
        else:
            self.insert_cfg[key] = value

    def alter_db(self, key, value):
        self.db[key] = value

    def alter_query_tb(self, key, value):
        if key == "sqls":
            self.table_query[key] = self.tb_query_sql
        else:
            self.table_query[key] = value

    def alter_query_stb(self, key, value):
        if key == "sqls":
            self.stable_query[key] = self.stb_query_sql
        else:
            self.stable_query[key] = value

    def alter_query_cfg(self, key, value):
        if key == "specified_table_query":
            self.query_cfg["specified_table_query"] = self.table_query
        elif key == "super_table_query":
            self.query_cfg["super_table_query"] = self.stable_query
        else:
            self.table_query[key] = value

    def alter_sub_cfg(self, key, value):
        if key == "specified_table_query":
            self.sub_cfg["specified_table_query"] = self.table_sub
        elif key == "super_table_query":
            self.sub_cfg["super_table_query"] = self.stable_sub
        else:
            self.table_query[key] = value

    def alter_sub_stb(self, key, value):
        if key == "sqls":
            self.stable_sub[key] = self.stb_sub_sql
        else:
            self.stable_sub[key] = value

    def alter_sub_tb(self, key, value):
        if key == "sqls":
            self.table_sub[key] = self.tb_sub_sql
        else:
            self.table_sub[key] = value

    def apped_sql_stb(self, target, value):
        if target == 'insert_stbs':
            self.stbs.append(value)
        elif target == 'query_table':
            self.tb_query_sql.append(value)
        elif target == 'query_stable':
            self.stb_query_sql.append(value)
        elif target == 'sub_table':
            self.tb_sub_sql.append(value)
        elif target == 'sub_stable':
            self.stb_sub_sql.append(value)

    def del_sql_stb(self, target, index):
        if target == 'insert':
            self.stbs.pop(index)
        elif target == 'query_table':
            self.tb_query_sql.pop(index)
        elif target == 'query_stable':
            self.stb_query_sql.pop(index)
        elif target == 'sub_table':
            self.tb_sub_sql.pop(index)
        elif target == 'sub_stable':
            self.stb_sub_sql.pop(index)

    # The following functions are get functions for different dicts
    def get_db_cfg(self):
        return self.db

    def get_stb_cfg(self):
        return self.stbs

    def get_insert_cfg(self):
        return self.insert_cfg

    def get_query_cfg(self):
        return self.query_cfg

    def get_tb_query(self):
        return self.table_query

    def get_stb_query(self):
        return self.stable_query

    def get_sub_cfg(self):
        return self.sub_cfg

    def get_tb_sub(self):
        return self.table_sub

    def get_stb_sub(self):
        return self.stable_sub

    def get_sql(self, target):
        if target == 'query_table':
            return self.tb_query_sql
        elif target == 'query_stable':
            return self.stb_query_sql
        elif target == 'sub_table':
            return self.tb_sub_sql
        elif target == 'sub_stable':
            return self.stb_sub_sql

    def get_template(self, target):
        if target == 'insert_stbs':
            return self.stb_template
        elif target == 'query_table':
            return self.tb_query_sql_template
        elif target == 'query_stable':
            return self.stb_query_sql_template
        elif target == 'sub_table':
            return self.tb_sub_sql_template
        elif target == 'sub_stable':
            return self.stb_sub_sql_template
        else:
            print(f'did not find {target}')

    def insert_cfg_generation(self, fileName):
        cfgFileName = f'temp/insert_{fileName}.json'
        self.alter_insert_cfg('databases', None)
        with open(cfgFileName, 'w') as file:
            json.dump(self.insert_cfg, file)

    def query_cfg_generation(self, fileName):
        cfgFileName = f'temp/query_{fileName}.json'
        self.alter_query_tb('sqls', None)
        self.alter_query_stb('sqls', None)
        self.alter_query_cfg('specified_table_query', None)
        self.alter_query_cfg('super_table_query', None)
        with open(cfgFileName, 'w') as file:
            json.dump(self.query_cfg, file)

    def sub_cfg_generation(self, fileName):
        cfgFileName = f'temp/subscribe_{fileName}.json'
        self.alter_sub_tb('sqls', None)
        self.alter_sub_stb('sqls', None)
        self.alter_sub_cfg('specified_table_query', None)
        self.alter_sub_cfg('super_table_query', None)
        with open(cfgFileName, 'w') as file:
            json.dump(self.sub_cfg, file)

    def drop_cfg_file(self, fileName):
        os.remove(f'temp/{fileName}')


tdTaosCfg = TDTaosdemoCfg()
