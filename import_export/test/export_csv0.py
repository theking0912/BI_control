#!/usr/bin/env python
# -*- coding:utf-8 -*-

import cx_Oracle
import time
import os

# �������ݿ�
conn = cx_Oracle.connect('BIetluser/qawsedrF123@172.16.105.58:1521/pdborcl')
cursor = conn.cursor()

sql_string = "" \
             "select c.table_id, c.table_name, c.ext_system, c.ext_type, c.ext_program, c.ext_driver, c.ext_connect, c.ext_user, c.ext_password, c.ext_sql_text, c.ext_sql_param_expr, c.ext_sql_param_value, c.ext_data_file, c.load_table_name, c.load_coverno_expr, c.load_coverno_value, c.is_valid, c.run_schedule, c.run_today, c.run_today_date, c.run_lastdate, c.run_status, c.run_last_error" \
             " from BIetluser.bi_job a" \
             "     inner join BIetluser.bi_job_step b" \
             "           on a.job_id = b.job_id" \
             "              and b.object_type='TABLE'" \
             "     inner join bi_etl_table c" \
             "           on b.object_id=c.table_id" \
             "				and c.is_valid = 1" \
             " order by b.sort"

cursor.execute(sql_string)
tables = cursor.fetchall()

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
    ext_data_file = table[12]
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

    yestoday = time.strftime('%Y%m%d', time.localtime(time.time()))
    ext_sql_text = ext_sql_text.replace('{#file_name}', table_name)
    ext_data_file = ext_data_file.replace('{#file_name}', table_name).replace('{#yesterday}', yestoday)

    # ƴ��ִ��������ƽű�
    cmd_export = "java -jar " + ext_program + " " + ext_driver + " " + ext_connect + " " + ext_user + " " + ext_password + " " + ext_sql_text + " " + ext_data_file + " " + ext_sql_param_value
    print (cmd_export)

    # ִ������
    os.system(cmd_export)

# �ر�����
conn.commit()
cursor.close()
conn.close()
