from USStock.infos import *
import pymysql
import pandas as pd
import numpy as np

# 여기서 데이터를 불러와서 필요한 중간 집계 테이블을 만들기
# 중간집계 테이블을 만들기 해서 필요한 데이터 프레임은 무엇?
# 4가지 : 12개 섹터의 시가총액 상위 브랜드의 당일 성과, 내 관심 종목들의 동향, 배당주들의 동향, 최근 거래일 급등주들의 동향
conn=pymysql.connect(host='127.0.0.1',port=3306,
                   user='root',passwd=mysql_passwd,
                   db='SideProject2021',charset='utf8')
cursor = conn.cursor()

