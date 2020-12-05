from USStock.infos import *
import pymysql
import pandas as pd
import numpy as np

# 아이티 종목 시총 상위 30
conn=pymysql.connect(host='127.0.0.1',port=3306,
                   user='root',passwd=mysql_passwd,
                   db='SideProject2021',charset='utf8')
cursor = conn.cursor()

print('mysql connnected')
try:
    cursor.execute('drop table mid_table.s_cc_stock_top_30')
    print('table deleted')
except:
    pass
    print('table doesnt exists')

q = '''
create table if not exists mid_table.s_cc_stock_top_30 as 

select 
	A.ticker
	,A.companyname
	,B.sector
	,B.industry
    ,c.value as mktcap
    ,date_format(c.d_date,'%Y-%m-%d') as d_date
    ,d.close
from company a
/* 산업 정보가 없는 것들은 그냥 버리기 */ 
inner join 
	industry b 
	on a.industryid = b.industryid 
left join
	(select ticker
    ,value
    ,d_date 
	from SideProject2021.priceratio
	where variable like 'Market-%') c
	on A.ticker = c.ticker 
left join SideProject2021.price d 
on a.ticker = d.ticker and c.d_date = d.d_date
where b.sector = 'Consumer Cyclical'
order by 5 desc
limit 30
;
'''
cursor.execute(q)
conn.close()
print('s_cc_stock_top_30 table craeted')