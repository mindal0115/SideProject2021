import pandas as pd

# 원격 데이터 베이스 접속
import pymysql
from USStock.infos import *
conn=pymysql.connect(host='192.168.0.11',port=3306,
                   user='root',passwd=mysql_passwd,
                   db='USStock_DB',charset='utf8')
cursor = conn.cursor()

# Simfin을 연결하여 필요한 데이터 불러오기
from USStock.infos import *
import simfin as sf
verofsimfin = sf.__version__
print('''
사용하는 Simfin 패키지의 버전은 {}
'''.format(verofsimfin))
# 파일의 디렉토리 설정
sf.set_data_dir('./SimfinData/')
sf.set_api_key(simfin_api)

# ====== 여기서부터는 테이블을 하나씩 업로드 =====
# 1. 기업리스트를 업로드하는 부분
q = 'truncate table usstock_db.company;'
cursor.execute(q)
conn.commit()
q = 'insert into usstock_db.company values(%s,%s,%s,%s);'
company = sf.load_companies(market='us')
company2 = company.reset_index().fillna(0)
data = company2.values.tolist()
cursor.executemany(q,data)
conn.commit()

# 2.산업 목록을 업로드하는 부분
q = 'truncate table usstock_db.industry;'
cursor.execute(q)
conn.commit()
q = 'insert into usstock_db.industry values(%s,%s,%s);'
industry = sf.load_industries()
data = industry.reset_index().fillna(0).values.tolist()
cursor.executemany(q,data)
conn.commit()

# 기업의 손익계산서를 업로드하는 부분
q = 'truncate table usstock_db.income;'
cursor.execute(q)
conn.commit()
q = 'insert into usstock_db.income values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'
income = sf.load_income(variant='quarterly-full', market='us')
income2 = pd.melt(income.reset_index(),
                  id_vars=['Ticker','Report Date','SimFinId', 'Currency', 'Fiscal Year',
                           'Fiscal Period', 'Publish Date','Restated Date', 'Shares (Basic)',
                           'Shares (Diluted)', 'Source'])
income2[['Report Date','Publish Date','Restated Date']] = income2[['Report Date','Publish Date','Restated Date']].astype(str)
data = income2.fillna(0).values.tolist()
cursor.executemany(q,data)
conn.commit()

# 기업의 주가 비율 지표를 업로드하는 부분
q = 'truncate table usstock_db.priceratio;'
cursor.execute(q)
conn.commit()
q = 'insert into usstock_db.priceratio values(%s,%s,%s,%s,%s);'
priceratio = sf.load_derived_shareprices(variant='latest', market='us')
priceratio2 = pd.melt(priceratio.reset_index(),id_vars=['Ticker','Date','SimFinId'])
priceratio2['Date'] = priceratio2['Date'].astype(str)
data = priceratio2.fillna(0).values.tolist()
cursor.executemany(q,data)
conn.commit()

# 기업들의 일별 종목을 업로드하는 부분
q = 'truncate table usstock_db.price;'
cursor.execute(q)
conn.commit()
q = 'insert into usstock_db.price values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'
price = sf.load_shareprices(variant='daily', market='us')
price2 = price.reset_index().fillna(0)
price2['Date'] =price2['Date'].astype(str)
data = price2.values.tolist()
cursor.executemany(q,data)
conn.commit()

conn.close()

