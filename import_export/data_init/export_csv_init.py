#!/usr/bin/env python
# -*- coding:utf-8 -*-
import cx_Oracle
import datetime
import os


def connectDB():
    db = cx_Oracle.connect('BIetluser/qawsedrF123@172.16.105.58:1521/pdborcl')
    return db


def sqlSelect(sql, db):
    cr = db.cursor()
    cr.execute(sql)
    rs = cr.fetchall()
    cr.close()
    return rs


def sqlDML(sql, db):
    cr = db.cursor()
    cr.execute(sql)
    cr.close()
    db.commit()


def sqlDML2(sql, params, db):
    cr = db.cursor()
    cr.execute(sql, params)
    cr.close()
    db.commit()


# 链接数据库
YESTODAY = datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(days=-1), '%Y%m%d')

sql_string = "select bet.table_id," \
             "       bet.table_name," \
             "       bet.ext_program," \
             "       bet.ext_driver," \
             "       bet.ext_connect," \
             "       bet.ext_user," \
             "       bet.ext_password," \
             "       bet.ext_sql_text," \
             "       bie.ext_data_file_init," \
             "       bie.ext_data_file_init2," \
             "       bie.execution_time," \
             "       bie.initialization_range," \
             "       bie.initialization_times" \
             "  from bi_etl_table bet" \
             " right join bi_init_export bie" \
             "    on bet.table_name = bie.table_name" \
             " where is_valid = 1" \
             "   and bie.if_initialize = 'N'"

db = connectDB()
tables = sqlSelect(sql_string, db)

for table in tables:
    table_id = table[0]
    table_name = table[1]
    ext_program = table[2]
    ext_driver = table[3]
    ext_connect = table[4]
    ext_user = table[5]
    ext_password = table[6]
    ext_sql_text = table[7]
    ext_data_file_init = table[8]
    ext_data_file_init2 = table[9]
    execution_time = table[10]
    initialization_range = table[11]
    initialization_times = table[12]

    # 创建数据路径
    data_path_Include = 'init_data/'
    ext_data_file_init2 = ext_data_file_init.replace('{#file_name}', data_path_Include + table_name).replace(
        '{#yesterday}', YESTODAY)
    sql_update = 'update bi_init_export set ext_data_file_init2=:ext_data_file_init2 where table_id=:table_id'
    param = {'ext_data_file_init2': ext_data_file_init2, 'table_id': table_id}
    sqlDML2(sql_update, param, db)

    # 创建sql路径
    sql_path_Include = 'init_sql/'
    table_name = sql_path_Include + table_name
    ext_sql_text = ext_sql_text.replace('{#file_name}', table_name)

    # 判断execution_time是否有值
    if execution_time is None:
        execution_time = '99999999'

    # 拼接执行命令及控制脚本
    cmd_export = "\"C:/Program Files/Java/jdk1.7.0_80/jre/bin/java.exe\" -jar " + ext_program + " " + ext_driver + " " + ext_connect + " " + ext_user + " " + ext_password + " " + ext_sql_text + " " + ext_data_file_init2 + " " + execution_time

    # 执行命令
    os.system(cmd_export)

    # 判断execution_time是否为空
    if_initialize = 'Y'

    if initialization_range is None:
        initialization_times = 1
        sql_update = "update bi_init_export " \
                     "   set initialization_range = : execution_time, " \
                     "       if_initialize = : if_initialize, " \
                     "       initialization_times = : initialization_times " \
                     " where table_id = : table_id"
    else:
        initialization_times = initialization_times + 1
        execution_time = execution_time[-8:]
        sql_update = "update bi_init_export " \
                     "   set initialization_range = substr(initialization_range,0,9)||: execution_time, " \
                     "       if_initialize = : if_initialize, " \
                     "       initialization_times = : initialization_times " \
                     " where table_id = : table_id"
    param = {'execution_time': execution_time, 'if_initialize': if_initialize,
             'initialization_times': initialization_times, 'table_id': table_id}
    sqlDML2(sql_update, param, db)
