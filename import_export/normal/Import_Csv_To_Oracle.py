#!/usr/bin/env python
# -*- coding:utf-8 -*-
import cx_Oracle
import time
import math
import os

#链接数据库
import datetime

conn = cx_Oracle.connect('BIetluser/qawsedrF123@172.16.105.58:1521/pdborcl')
cursor = conn.cursor()
# 当前时间
now_date = time.strftime('%Y%m%d', time.localtime(time.time()))
YESTODAY=datetime.datetime.strftime(datetime.datetime.now()+datetime.timedelta(days = -1), '%Y%m%d')
#CONTROL_FILE_PATH
CONTROL_FILE_PATH = 'D:/BI/'

sql_string = "select c.table_id, c.table_name, c.ext_system, c.run_lastdate, c.run_status, c.load_table_name"\
			" from BIetluser.bi_job a"\
			"     inner join BIetluser.bi_job_step b"\
			"           on a.job_id = b.job_id"\
			"              and b.object_type='TABLE'"\
			"     inner join BIetluser.bi_etl_table c"\
			"           on b.object_id=c.table_id "\
			" where c.is_valid = 1"\
			"  and a.job_name='EXPORT_ALL' \n" \
			"  and to_char(run_lastdate,'YYYYMMDD') = "+now_date+"\n" \
			" order by b.sort"

cursor.execute(sql_string)
tables = cursor.fetchall()

for table in tables:
	table_id=table[0]
	table_name=table[1]
	ext_system=table[2]
	run_lastdate=table[3]
	run_status=table[4]
	load_table_name=table[5]


	# 控制文件名称
	control_file_name = load_table_name + "_control.ctl"
	# 最后运行时间
	run_lastdate = run_lastdate.strftime("%Y%m%d")
	control_file = ''
	cmd_str=''
	#1、判断是否为今天导入 2、是否导出成功，成功后才可执行导入
	if run_lastdate == now_date and run_status == 1:
		#拼接执行命令及控制脚本
		cmd_str = "sqlldr userid=BIetluser/qawsedrF123@172.16.105.58:1521/pdborcl control="+CONTROL_FILE_PATH+control_file_name+" errors=9999999 rows=1000 direct=true log="+CONTROL_FILE_PATH+load_table_name+"_import.log"

		control_file = "OPTIONS (skip=1) \n" \
					   "load data \n" \
					   "infile '"+CONTROL_FILE_PATH+"data/"+YESTODAY+"/"+table_name+"_"+YESTODAY+".csv'\n" \
					   "badfile '"+CONTROL_FILE_PATH+"data/"+YESTODAY+"/"+load_table_name+".csv.bad' \n" \
					   "truncate into table prd_bi_data."+ load_table_name +"\n" \
					   "fields terminated by ','  Optionally enclosed by '\"' \n"\
					   "trailing nullcols\n(\n"
	# 拼接查表结构sql
	query_table_sql = "select " \
					 "b.column_name, " \
					 "b.data_type, " \
					 "case " \
					 "when b.data_precision is null then " \
					 "b.data_length " \
					 "else " \
					 "data_precision " \
					 "end DATA_LEN " \
					 "from all_tables a, all_tab_columns b, all_objects c " \
					 "where a.table_name = b.table_name " \
					 "and a.owner = b.owner " \
					 "and a.owner = c.owner " \
					 "and a.table_name = c.object_name " \
					 "and a.table_name = '" + str(load_table_name) +\
					 "' " \
					 "and c.object_type = 'TABLE'" \
					 "order by b.column_id asc"

	# 执行sql
	cursor.execute(query_table_sql)
	row = cursor.fetchall()
	row_count = len(row)
	char_len = 0
	v_count = 0

	# 拼接表结构
	for rad in row:
		control_file += rad[0] + "  "
		if (rad[2] < 512):
			char_len = 512
			v_count += 1
			if (v_count != row_count):
				control_file += "char(" + str(char_len) + ")\n,"
			else:
				control_file += "char(" + str(char_len) + ")\n)"
		else:
			char_len = math.ceil((rad[2] / 512)) * 512  # (rad[2]除512向上取整)乘以512
			v_count += 1
			if (v_count != row_count):
				control_file += "char(" + str(char_len) + ")\n,"
			else:
				control_file += "char(" + str(char_len) + ")\n)"
	# 输出控制文件到指定目录
	file_object = open(CONTROL_FILE_PATH + control_file_name, 'w')
	file_object.write(control_file)
	file_object.close()
	#执行命令
	# os.system(cmd_str)

# Sp_str="BIetluser.p_del_tw_repeat_data('import');"
# x=cursor.callproc(Sp_str)

#关闭链接
conn.commit()
cursor.close()
conn.close()