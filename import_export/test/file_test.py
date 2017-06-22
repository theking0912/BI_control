fh = open('D:/BI/control/test/aaa.sql','r+')
for  line in  fh.readlines():
    a='xxxxxxxx'
    print  (line+a)
fh.write(line+a)
fh.close()



# select distinct b.*
# from SAPEq1.EKBE a
# 	inner join SAPEq1.MSEG b
#         on a.mandt = b.mandt
#         and a.BELNR=b.MBLNR
#         and a.BUZEI=b.ZEILE
# 		and a.GJAHR=b.MJAHR
# WHERE a.mandt='800' AND a.CPUDT >= left(#{acycId},8) AND a.CPUDT < right(#{acycId}, 8) AND a.VGABE = '1'