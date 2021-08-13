import sys
from util.log import *
from util.cases import *
from util.sql import *
from util.taosdemoCfg import *
from util.pathFinding import *
import os
import subprocess
import datetime
import string
import random


class stepJsonGeneration:
    def __init__(self, host, tables):
        self.schema1 = [[{'type': 'DOUBLE', 'count': 2}, {'type': 'int', 'count': 2}], [
            {"type": "BINARY", "len": 32, "count": 3}]]
        self.schema2 = [[{'type': 'DOUBLE', 'count': 10}, {'type': 'int', 'count': 10}], [
            {"type": "BINARY", "len": 32, "count": 5}]]
        self.schema3 = [[{'type': 'DOUBLE', 'count': 50}, {'type': 'int', 'count': 50}], [
            {"type": "BINARY", "len": 32, "count": 9}]]
        self.schema4 = [[{'type': 'DOUBLE', 'count': 250}, {'type': 'int', 'count': 250}], [
            {"type": "BINARY", "len": 32, "count": 17}]]
        self.host = host
        self.table = tables
        self.schemaScale = 1

    def create_JSON_gen(self, tables, schemaScale):
        self.table = tables
        self.schemaScale = schemaScale
        stbCfg = taosdemoCfg.get_template('insert_stbs')
        taosdemoCfg.alter_insert_cfg('host', self.host)
        taosdemoCfg.alter_db('drop', 'no')
        taosdemoCfg.alter_insert_cfg('thread_count', 10)
        taosdemoCfg.alter_insert_cfg('thread_count_create_tbl', 10)
        taosdemoCfg.alter_insert_cfg('result_file', "./insert_res.txt")
        stbCfg["batch_create_tbl_num"]=100
        stbCfg['child_table_exists'] = 'no'
        stbCfg['max_sql_len'] = 1024000
        stbCfg['insert_rows'] = 0
        stbCfg['tags_file'] = './tags.csv'
        stbCfg["sample_format"] = "csv"
        stbCfg["name"] = "stb"
        if schemaScale == 1:
            stbCfg["columns"] = self.schema1[0]
            stbCfg["tags"] = self.schema1[1]
            stbCfg['tags_file'] = '/root/TDinternal/community/tests/pytest/perfbenchmark/benchmark_step/JSON/tags_1.csv'
        elif schemaScale == 2:
            stbCfg["columns"] = self.schema2[0]
            stbCfg["tags"] = self.schema2[1]
            stbCfg['tags_file'] = '/root/TDinternal/community/tests/pytest/perfbenchmark/benchmark_step/JSON/tags_2.csv'
        elif schemaScale == 3:
            stbCfg["columns"] = self.schema3[0]
            stbCfg["tags"] = self.schema3[1]
            stbCfg['tags_file'] = '/root/TDinternal/community/tests/pytest/perfbenchmark/benchmark_step/JSON/tags_3.csv'
        elif schemaScale == 4:
            stbCfg["columns"] = self.schema4[0]
            stbCfg["tags"] = self.schema4[1]
            stbCfg['tags_file'] = '/root/TDinternal/community/tests/pytest/perfbenchmark/benchmark_step/JSON/tags_4.csv'
        for i in range(5):
            stbCfg["childtable_prefix"] = f"stb_{i}_"
            stbCfg['childtable_count'] = tables//5
            taosdemoCfg.import_stbs([stbCfg])
            taosdemoCfg.generate_insert_cfg(
                'perfbenchmark/benchmark_step/JSON', f"create_{tables}_{i}")

    def insert_JSON_gen(self, taosdemo):
        taosdemoPerGroup = taosdemo // 5
        tablePerJson = self.table //5
        stbCfg = taosdemoCfg.get_template('insert_stbs')
        taosdemoCfg.alter_insert_cfg('host', self.host)
        taosdemoCfg.alter_db('drop', 'no')
        taosdemoCfg.alter_insert_cfg('thread_count', 10)
        taosdemoCfg.alter_insert_cfg('thread_count_create_tbl', 10)
        taosdemoCfg.alter_insert_cfg('result_file', "./insert_res.txt")
        stbCfg['insert_rows'] = 30
        stbCfg["batch_create_tbl_num"] = 25
        stbCfg['timestamp_step'] = 1000
        stbCfg["max_sql_len"] = 1024000
        stbCfg["childtable_count"] = self.table
        stbCfg['start_timestamp'] = "now"
        stbCfg['child_table_exists'] = 'yes'
        for i in range(5):
            stbCfg["childtable_prefix"] = f"stb_{i}_"
            stbCfg["childtable_offset"] = i * tablePerJson
            stbCfg["childtable_limit"] = tablePerJson
            if self.schemaScale == 1:
                stbCfg["columns"] = self.schema1[0]
                stbCfg["tags"] = self.schema1[1]
                stbCfg['tags_file'] = '/root/TDinternal/community/tests/pytest/perfbenchmark/benchmark_step/JSON/tags_1.csv'
            elif self.schemaScale == 2:
                stbCfg["columns"] = self.schema2[0]
                stbCfg["tags"] = self.schema2[1]
                stbCfg['tags_file'] = '/root/TDinternal/community/tests/pytest/perfbenchmark/benchmark_step/JSON/tags_2.csv'
            elif self.schemaScale == 3:
                stbCfg["columns"] = self.schema3[0]
                stbCfg["tags"] = self.schema3[1]
                stbCfg['tags_file'] = '/root/TDinternal/community/tests/pytest/perfbenchmark/benchmark_step/JSON/tags_3.csv'
            elif self.schemaScale == 4:
                stbCfg["columns"] = self.schema4[0]
                stbCfg["tags"] = self.schema4[1]
                stbCfg['tags_file'] = '/root/TDinternal/community/tests/pytest/perfbenchmark/benchmark_step/JSON/tags_4.csv'
            taosdemoCfg.import_stbs([stbCfg])
            taosdemoCfg.generate_insert_cfg(
                'perfbenchmark/benchmark_step/JSON', f"insert_{self.table}_{i}")

    def query_JSON_gen(self, concurrent, sql, fileName):
        taosdemoCfg.alter_query_cfg("host", self.host)
        taosdemoCfg.alter_query_cfg("query_times", 10)
        taosdemoCfg.alter_query_tb("query_interval", 0)
        taosdemoCfg.alter_query_tb("concurrent", concurrent)
        sql = {
            "sql": sql,
            "result": ""
        }
        taosdemoCfg.import_sql([sql],'query_table')
        taosdemoCfg.generate_query_cfg(
            'perfbenchmark/benchmark_step/JSON', f"create_{fileName}")
    
    def query_JSON_gen_rand_table(self, concurrent,sql1, sql2, filename):

        taosdemoCfg.alter_query_cfg("host", self.host)
        taosdemoCfg.alter_query_cfg("query_times", 10)
        taosdemoCfg.alter_query_tb("query_interval", 0)
        taosdemoCfg.alter_query_tb("concurrent", concurrent)
        sql = []
        for i in range(20):
            sql_temp = {
                "sql": "".join([sql1, " ", f"stb_0_{random.randint(0,2000000)}", " ", sql2]),
                "result": ""
            }
            sql.append(sql_temp)
            sql_temp = {
                "sql": "".join([sql1, " ", f"stb_1_{random.randint(2000000,4000000)}", " ", sql2]),
                "result": ""
            }
            sql.append(sql_temp)
            sql_temp = {
                "sql": "".join([sql1, " ", f"stb_2_{random.randint(4000000,6000000)}", " ", sql2]),
                "result": ""
            }
            sql.append(sql_temp)
            sql_temp = {
                "sql": "".join([sql1, " ", f"stb_3_{random.randint(6000000,8000000)}", " ", sql2]),
                "result": ""
            }
            sql.append(sql_temp)
            sql_temp = {
                "sql": "".join([sql1, " ", f"stb_4_{random.randint(8000000,10000000)}", " ", sql2]),
                "result": ""
            }
            sql.append(sql_temp)
        taosdemoCfg.import_sql(sql,'query_table')
        taosdemoCfg.generate_query_cfg(
            'perfbenchmark/benchmark_step/JSON', f"create_{filename}")

    def csvGeneration(self, schema, fileName="1"):
        with open(f"perfbenchmark/benchmark_step/JSON/tags_{fileName}.csv", 'w') as f:
            for i in range(10):
                if i % 3 == 0:
                    csvLine = f"'{i}', 'shenzen', 'china'"
                elif i % 3 == 1:
                    csvLine = f"'{i}', 'beijing', 'china'"
                elif i % 3 == 2:
                    csvLine = f"'{i}', 'shanghai', 'china'"
                if schema == 1:
                    f.write(csvLine + '\n')
                elif schema == 2:
                    for i in range(2):
                        tag = ''.join(random.choices(
                            string.ascii_uppercase + string.digits, k=20))
                        csvLine += "," + f'\"{tag}\"'
                    f.write(csvLine + '\n')
                elif schema == 3:
                    for i in range(6):
                        tag = ''.join(random.choices(
                            string.ascii_uppercase + string.digits, k=20))
                        csvLine += "," + f'\"{tag}\"'
                    f.write(csvLine + '\n')
                elif schema == 4:
                    for i in range(14):
                        tag = ''.join(random.choices(
                            string.ascii_uppercase + string.digits, k=20))
                        csvLine += "," + f'\"{tag}\"'
                    f.write(csvLine + '\n')
