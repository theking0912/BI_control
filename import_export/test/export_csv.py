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

# �������ݿ�

YESTODAY=datetime.datetime.strftime(datetime.datetime.now()+datetime.timedelta(days = -1), '%Y%m%d')

sql_string = ""\
"select table_id, table_name, run_schedule, run_today, run_today_date, run_lastdate, run_status," \
"ext_sql_param_expr, ext_sql_param_value, ext_data_file, ext_data_file2," \
"load_coverno_type, load_coverno_expr, load_coverno_value " \
" from bi_etl_table" \
" where is_valid =1"

db=connectDB()
tables=sqlSelect(sql_string, db)

for table in tables:
    table_id=table[0]
    table_name = table[1]
    run_schedule = table[2]
    run_today = table[3]
    run_today_date = table[4]
    run_lastdate = table[5]
    run_status = table[6]
    ext_sql_param_expr = table[7]
    ext_sql_param_value = table[8]
    ext_data_file = table[9]
    ext_data_file2 = table[10]
    load_coverno_type = table[11]
    load_coverno_expr = table[12]
    load_coverno_value = table[13]

    run_today=0

    if run_schedule=='DAY':
        run_today=1
        run_today_date=datetime.datetime.now()
        if ext_sql_param_expr is not None:
            rows=sqlSelect(ext_sql_param_expr, db)
            for row in rows:
                ext_sql_param_value=row[0]
        ext_data_file2 = ext_data_file.replace('{#file_name}', table_name).replace('{#yesterday}', YESTODAY)
        if load_coverno_type=='EXPR':
            rows=sqlSelect(load_coverno_expr, db)
            for row in rows:
                load_coverno_value=row[0]

    sql_upate='update bi_etl_table set run_today=:run_today, run_today_date=:run_today_date, ext_sql_param_value=:ext_sql_param_value, ext_data_file2=:ext_data_file2, load_coverno_value=:load_coverno_value where table_id=:table_id'
    param = {'run_today': run_today, 'run_today_date': run_today_date, 'ext_sql_param_value': ext_sql_param_value,'ext_data_file2': ext_data_file2, 'load_coverno_value': load_coverno_value, 'table_id': table_id}
    sqlDML2(sql_upate, param, db)

sql_string = "" \
             "select c.table_id, c.table_name, c.ext_system, c.ext_type, c.ext_program, c.ext_driver, c.ext_connect, c.ext_user, c.ext_password, c.ext_sql_text, c.ext_sql_param_expr, c.ext_sql_param_value, c.ext_data_file2, c.load_table_name, c.load_coverno_expr, c.load_coverno_value, c.is_valid, c.run_schedule, c.run_today, c.run_today_date, c.run_lastdate, c.run_status, c.run_last_error" \
             " from BIetluser.bi_job a" \
             "     inner join BIetluser.bi_job_step b" \
             "           on a.job_id = b.job_id" \
             "              and b.object_type='TABLE'" \
             "     inner join bi_etl_table c" \
             "           on b.object_id=c.table_id" \
             "				and c.is_valid = 1" \
             " order by b.sort"
tables=sqlSelect(sql_string, db)
for table in tables:
    table_id = table[0]
    table_name = table[1]
    ext_system = table[2]
    ext_type = table[3]
    ext_program = table[4]
    ext_driver = table[5]
    ext_connect = table[6]
    ext_user = table[7]
    ext_password = table[8]
    ext_sql_text = table[9]
    ext_sql_param_expr = table[10]
    ext_sql_param_value = table[11]
    ext_data_file2 = table[12]
    load_table_name = table[13]
    load_coverno_expr = table[14]
    load_coverno_value = table[15]
    is_valid = table[16]
    run_schedule = table[17]
    run_today = table[18]
    run_today_date = table[19]
    run_lastdate = table[20]
    run_status = table[21]
    run_last_error = table[22]

    ext_sql_text = ext_sql_text.replace('{#file_name}', table_name)
    if ext_sql_param_value is None:
        ext_sql_param_value = '99999999'
    # ƴ��ִ��������ƽű�
    cmd_export = "\"C:/Program Files/Java/jdk1.7.0_80/jre/bin/java.exe\" -jar " + ext_program + " " + ext_driver + " " + ext_connect + " " + ext_user + " " + ext_password + " " + ext_sql_text + " " + ext_data_file2 + " " + ext_sql_param_value
    print (cmd_export)

    # ִ������
    os.system(cmd_export)

    sql_upate = 'update bi_etl_table set run_lastdate=sysdate, run_status=1 where table_id=:table_id'
    param = {'table_id':table_id}
    sqlDML2(sql_upate, param, db)