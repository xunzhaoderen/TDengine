from taosdemoCfg import *
import time
tdTaosCfg.apped_sql_stb('insert_stbs', tdTaosCfg.get_template('insert_stbs'))
tdTaosCfg.insert_cfg_generation('test')

tdTaosCfg.apped_sql_stb('query_table', tdTaosCfg.get_template('query_table'))
tdTaosCfg.apped_sql_stb('query_stable', tdTaosCfg.get_template('query_stable'))
tdTaosCfg.query_cfg_generation('test_1')

tdTaosCfg.apped_sql_stb('sub_table', tdTaosCfg.get_template('sub_table'))
tdTaosCfg.apped_sql_stb('sub_stable', tdTaosCfg.get_template('sub_stable'))
#tdTaosCfg.alter_sub_stb('sqls', None)
tdTaosCfg.sub_cfg_generation('test_1')

