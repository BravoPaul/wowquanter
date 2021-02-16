import pandas as pd
import json
from datetime import datetime

bull_stock = pd.read_excel('/Users/kunyue/project_personal/wowquanter/data/bull_stock_2016_2021_new (27).xlsx')
st_fund_stock = pd.read_excel('/Users/kunyue/project_personal/wowquanter/data/st_result (1).xlsx')
# st_fund_stock['start_date'] = st_fund_stock['start_date'].map(lambda x: datetime.strptime(x, '%Y-%m-%d'))
# st_fund_stock['end_date'] = st_fund_stock['end_date'].map(lambda x: datetime.strptime(x, '%Y-%m-%d'))
st_fund_stock = st_fund_stock.sort_values(by=['code', 'start_date'])

result_1_prev = [-1 for i in range(len(bull_stock))]
result_2_prev = [-1 for i in range(len(bull_stock))]
result_3_prev = [-1 for i in range(len(bull_stock))]
result_1_now = [-1 for i in range(len(bull_stock))]
result_2_now = [-1 for i in range(len(bull_stock))]
result_3_now = [-1 for i in range(len(bull_stock))]
result_1_up = [-1 for i in range(len(bull_stock))]
result_2_up = [-1 for i in range(len(bull_stock))]
result_3_up = [-1 for i in range(len(bull_stock))]

for i, value in bull_stock.iterrows():
    st_fund_stock_c = st_fund_stock[st_fund_stock['code'] == value['code']]
    s_raise_range = value['N_3']
    dict_range = json.loads(s_raise_range)
    for j, one_range in enumerate(dict_range):
        if j > 2:
            break
        start_date = datetime.strptime(one_range['start_time'], '%Y-%m')
        end_date = datetime.strptime(one_range['end_time'], '%Y-%m')
        st_fund_stock_c_tmp = st_fund_stock_c[
            (st_fund_stock_c['start_date'] > start_date) & (st_fund_stock_c['end_date'] < end_date)].head(1)

        st_fund_stock_c_prev = st_fund_stock_c[st_fund_stock_c['end_date'] < start_date].tail(1)

        if len(st_fund_stock_c_tmp) != 0:
            if len(st_fund_stock_c_prev) != 0:
                prev_value = int(st_fund_stock_c_prev['count'].values[0])
            else:
                prev_value = 1
            now_value = int(st_fund_stock_c_tmp['count'].values[0])

            globals().get('result_' + str(j + 1) + '_prev')[i] = prev_value
            globals().get('result_' + str(j + 1) + '_now')[i] = now_value
            globals().get('result_' + str(j + 1) + '_up')[i] = (now_value - prev_value) / prev_value

bull_stock['1_prev'] = result_1_prev
bull_stock['1_now'] = result_1_now
bull_stock['1_up'] = result_1_up

bull_stock['2_prev'] = result_2_prev
bull_stock['2_now'] = result_2_now
bull_stock['2_up'] = result_2_up

bull_stock['3_prev'] = result_3_prev
bull_stock['3_now'] = result_3_now
bull_stock['3_up'] = result_3_up

bull_stock.to_excel('/Users/kunyue/project_personal/wowquanter/data/bull_2.xlsx')
