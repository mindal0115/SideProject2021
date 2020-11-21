# simfin을 이용한 데이터베이스 구축

from USStock.infos import *
import simfin as sf
verofsimfin = sf.__version__
print('''
사용하는 Simfin 패키지의 버전은 {}
'''.format(verofsimfin))
# 파일의 디렉토리 설정
sf.set_data_dir('./SimfinData/')
sf.set_api_key(simfin_api)
#
# # 데이터를 다운로드하는 구절
# # company = sf.load_companies(market='us')
# # industry = sf.load_industries()
# # income = sf.load_income(variant='quarterly-full', market='us')
# # balance = sf.load_balance(variant='quarterly-full', market='us')
# # cash = sf.load_cashflow(variant='quarterly-full', market='us')
# # priceratio = sf.load_derived_shareprices(variant='latest', market='us')
#
# print(price.head())
# print(price.columns)