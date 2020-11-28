from USStock.infos import *
import pymysql
import pandas as pd
import numpy as np
conn,cursor = connect_mysql()

# 여기서 데이터를 불러와서 필요한 중간 집계 테이블을 만들기
# 중간집계 테이블을 만들기 해서 필요한 데이터 프레임은 무엇?
# 4가지 : 12개 섹터의 시가총액 상위 브랜드의 당일 성과, 내 관심 종목들의 동향, 배당주들의 동향, 최근 거래일 급등주들의 동향

# 각 종목의 시가 총액 상위 브랜드
# 종목 리스트
company = pd.read_sql('select ticker,companyname,industryid from usstock_db.company;',conn)
industry = pd.read_sql('select industryid,sector from usstock_db.industry;',conn)

mktcap = pd.read_sql('select ticker,val as mktcap from usstock_db.priceratio where variable in ("Market-Cap");',conn)
per = pd.read_sql('select ticker,val as per from usstock_db.priceratio where variable in ("Price to Earnings Ratio (ttm)");',conn)
df2 = pd.merge(company,industry,on='industryid',how='left')
df2 = pd.merge(df2,mktcap,on='ticker',how='left')
df2 = pd.merge(df2,per,on='ticker',how='left')

# 여기에 최근 주가의 변동폭을 가져와서 덧붙여야한다
# 종가와 변동폭을 추가함
price = pd.read_sql('select ticker,d_date,close from usstock_db.price where d_date >= date_format(date_sub(now(),interval 4 day),"%Y-%m-%d");',conn)
price['pct_change'] = price.sort_values(['ticker','d_date']).groupby('ticker')['close'].apply(lambda x : x.pct_change())
idx = price['d_date'] == price['d_date'].max()
price = price[idx]

df3 = pd.merge(df2,price,on='ticker',how='inner').drop(columns=['industryid'])
print(df3.columns)
print(df3)

for sector in df3['sector'].unique():
    idx = df3['sector'] == sector
    df4 = df3[idx].sort_values('mktcap',ascending=False).head(20)
    print(df4)




