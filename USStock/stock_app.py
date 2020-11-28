# simfin을 이용한 데이터베이스 구축
'''
이제 중간 집계 테이블을 만들어서, 내가 보고 싶은 데이터를 지속적으로 볼 수 있게 만들어보자.
그래서 어떤 중간 집계 테이블이 필요한 것인가
'''

''' ㅌㅔ이블이 잘 만들어져 있는지 확인해보기 '''
import pymysql
from USStock.infos import *
conn=pymysql.connect(host='127.0.0.1',port=3306,
                   user='root',passwd=mysql_passwd,
                   db='SideProject2021',charset='utf8')
cursor = conn.cursor()

import pandas as pd
df = pd.read_sql('select * from SideProject2021.income limit 3;',conn)
print(df)

df = pd.read_sql('select * from SideProject2021.price limit 3;',conn)
print(df)

df = pd.read_sql('select * from SideProject2021.priceratio limit 3;',conn)
print(df)

