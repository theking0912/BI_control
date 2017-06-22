import cx_Oracle

conn = cx_Oracle.connect('BIetluser/qawsedrF123@172.16.105.58:1521/pdborcl')
cursor = conn.cursor()

Sp_str="BIetluser.asd"
x=cursor.callproc(Sp_str)

conn.commit()
cursor.close()
conn.close()