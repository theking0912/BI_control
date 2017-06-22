#!/usr/bin/env python
# -*- coding:utf-8 -*-

import cx_Oracle
import math
import os

#链接数据库 172.16.105.58:1521/PDBORCL
conn = cx_Oracle.connect('BIetluser/qawsedrF123@172.16.105.58:1521/PDBORCL')
cursor = conn.cursor()

#拼接执行命令及控制脚本
table_name = 'BI_ETL_TABLE'
cmd_str = "sqlldr userid=system/leroy@orcl control=C:\\Users\\theking\Desktop\control\control1.ctl  errors=9999999 rows=1000 direct=true log=C:\\Users\\theking\Desktop\control\import.log"
control_file = "OPTIONS (skip=1) \n" \
               "load data \n" \
               "infile 'C:\\Users\\theking\Desktop\control\\asd.csv' \n" \
               "badfile 'C:\\Users\\theking\Desktop\control\\asd.csv.bad' \n" \
               "truncate into table "+ table_name +"\n" \
               "fields terminated by ','  Optionally enclosed by '\"' \n"\
               "trailing nullcols\n(\n"

#拼接查表结构sql
sql_string = "select " \
             "b.column_name, "\
             "b.data_type, "\
             "case "\
             "when b.data_precision is null then "\
             "b.data_length "\
             "else "\
             "data_precision "\
             "end DATA_LEN "\
             "from all_tables a, all_tab_columns b, all_objects c "\
             "where a.table_name = b.table_name "\
             "and a.owner = b.owner "\
             "and a.owner = c.owner "\
             "and a.table_name = c.object_name "\
             "and a.table_name = '"+str(table_name)+"' "  \
             "and c.object_type = 'TABLE'" \
             "order by b.column_id asc"

print (sql_string)

#执行sql
cursor.execute(sql_string)
row = cursor.fetchall()
row_count = len(row)
char_len = 0
v_count = 0

#拼接表结构
for rad in row:
    control_file += rad[0] + "  "
    if(rad[2] < 512):
        char_len = 512
        v_count += 1
        if(v_count != row_count):
            control_file += "char("+str(char_len) + ")\n,"
        else:
            control_file += "char(" + str(char_len) + ")\n)"
    else:
        char_len = math.ceil((rad[2]/512))*512  #(rad[2]除512向上取整)乘以512
        v_count += 1
        if (v_count != row_count):
            control_file += "char(" + str(char_len) + ")\n,"
        else:
            control_file += "char(" + str(char_len) + ")\n)"
print (control_file)
# 输出控制文件到指定目录
file_object = open(r'C:\Users\theking\Desktop\control\control1.ctl', 'w')
file_object.write(control_file)
file_object.close()

#执行命令

os.popen(cmd_str)

#关闭链接
conn.commit()
cursor.close()
conn.close()