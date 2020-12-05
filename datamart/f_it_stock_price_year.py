from USStock.infos import *
import pymysql
import pandas as pd
import numpy as np

# 아이티 종목 시총 상위 30
conn=pymysql.connect(host='127.0.0.1',port=3306,
                   user='root',passwd=mysql_passwd,
                   db='SideProject2021',charset='utf8')
cursor = conn.cursor()

table_name = 'f_it_stock_price_year'
print('mysql connnected')
try:
    cursor.execute('drop table mid_table.{}'.format(table_name))
    print('table deleted')
except:
    pass
    print('table doesnt exists')

q = '''
create table if not exists mid_table.{} as 

with basement as
(select 
	ticker
	,date_format(d_date,'%Y-%m-%d') as d_date
	,close
    ,lag(close) over(partition by ticker order by d_date) as pre_close
from SideProject2021.price
where ticker in (select ticker from mid_table.s_it_stock_top_30)
and date_format(d_date,'%Y-%m-%d') >= date_sub(now(),interval 365 day)) 
, start_price as 
(
select A.ticker,A.d_date,B.close as base_price
from basement B 
INNER JOIN
(select ticker,min(d_date) as d_date 
from basement  group by 1) A
on A.ticker = B.ticker and A.d_date = B.d_date 
)

select 
	A.ticker
	,A.d_date
	,A.close
	,A.pre_close
	,B.base_price
	,A.close/A.pre_close-1 as pct_change 
	,A.close/B.base_price as ytd_change
from basement A 
left join start_price B 
on A.ticker = B.ticker
where pre_close is not null 
;
'''.format(table_name)

cursor.execute(q)
conn.close()
print('{} table craeted'.format(table_name))
