#!/usr/bin/env python
# -*- coding:utf-8 -*-
import time
import cx_Oracle
import datetime


conn = cx_Oracle.connect('BIetluser/qawsedrF123@172.16.105.58:1521/pdborcl')
cursor = conn.cursor()
Sp_str="BIetluser.p_del_tw_repeat_data('import')"
cursor.callproc(Sp_str)

# str1='nice'
# str2=''#需要有值，即len(str2)>=len(str1)
# x=cursor.callproc('p_demo',[str1,str2])
# print(x)
#关闭链接
conn.commit()
cursor.close()
conn.close()