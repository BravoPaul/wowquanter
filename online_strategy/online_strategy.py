# 导入函数库
from jqdata import *
import pandas as pd
from datetime import datetime, timedelta


# 初始化函数，设定基准等等
def initialize(context):
    # 设定沪深300作为基准
    set_benchmark('000300.XSHG')
    # 开启动态复权模式(真实价格)
    set_option('use_real_price', True)
    # 输出内容到日志 log.info()
    log.info('初始函数开始运行且全局只运行一次')
    # 过滤掉order系列API产生的比error级别低的log
    # log.set_level('order', 'error')
    g.index_security = '000300.XSHG'
    g.pd_stock_count = (datetime.strptime('2013-01-01', '%Y-%m-%d').date(), None)
    g.flag = 1
    g.code_bought = set()
    g.buy_list = set()
    g.sell_list = set()

    log.set_level('order', 'error')

    ### 股票相关设定 ###
    # 股票类每笔交易时的手续费是：买入时佣金万分之三，卖出时佣金万分之三加千分之一印花税, 每笔交易佣金最低扣5块钱
    set_order_cost(OrderCost(close_tax=0.001, open_commission=0.0003, close_commission=0.0003, min_commission=5),
                   type='stock')

    run_monthly(get_all_candidate_stock, -1, time='06:30', reference_security=g.index_security)

    run_monthly(contition_func, -1, time='07:30', reference_security=g.index_security)

    run_monthly(alpha_func, -1, time='08:00', reference_security=g.index_security)

    run_monthly(main_buy, -1, time='08:30', reference_security=g.index_security)

    run_monthly(sell_all, -1, time='10:30', reference_security=g.index_security)

    run_monthly(buy_with_cash_equal, -1, time='14:00', reference_security=g.index_security)
    ## 运行函数（reference_security为运行时间的参考标的；传入的标的只做种类区分，因此传入'000300.XSHG'或'510300.XSHG'是一样的）
    # 开盘前运行
    # run_daily(before_market_open, time='before_open', reference_security='000300.XSHG')
    # 开盘时运行
    # run_daily(market_open, time='open', reference_security='000300.XSHG')
    # 收盘后运行
    # run_daily(after_market_close, time='after_close', reference_security='000300.XSHG')
    # 测试是运行
    # run_daily(test_func, time='open', reference_security='000300.XSHG')


def util_run_func_by_chunk(list_code, n, func, **args):
    def chunks(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    result = []

    for once_code in chunks(list_code, n):
        result.append(func(once_code, **args))

    return result


# conditon turnOver
# 这里的turnOver condition 代表前3个月，不包括本月
def contition_1(context):
    stock_list = g.stock_list
    dt = context.current_dt.date()
    dt_str = dt.strftime('%Y-%m-%d')
    dt_start = (dt - timedelta(days=70)).strftime('%Y-%m-%d')
    dt_str_start = dt_start.split('-')[0] + '-' + dt_start.split('-')[1] + '-' + '01'
    dt_str_format = last_day_of_month(dt).strftime('%Y-%m-%d')
    pd_turnOver = util_get_month_turnOver(stock_list, dt_str)
    alpha_turnOver = alpha_turnOver_30(pd_turnOver, 3)
    return alpha_turnOver


# condition plus 10
def contition_2(context):
    stock_list = g.stock_list
    dt = context.current_dt.date()
    dt_str = dt.strftime('%Y-%m-%d')
    dt_start = (dt - timedelta(days=70)).strftime('%Y-%m-%d')
    dt_str_start = dt_start.split('-')[0] + '-' + dt_start.split('-')[1] + '-' + '01'

    if dt.month in [1, 4, 7, 10]:
        pd_fund_portfolio = g.pd_stock_count[1]
        dt_str = g.pd_stock_count[0]

        result_tmp = get_fund_portfolio_by_year(2014, 2022, dt_str, dt)
        result_tmp = util_normalize_code(result_tmp, 'symbol')
        result_tmp = util_add_pub_year(result_tmp)
        result_tmp = util_add_name(result_tmp)

        result_tmp = result_tmp[
            (result_tmp['display_name'] != '未知') & (result_tmp['pub_date'] != '未知')]
        result_tmp = result_tmp.sort_values(
            by=['code', 'display_name', 'start_date', 'end_date', 'report_type', 'stock_count']).drop_duplicates(
            subset=['code', 'display_name', 'start_date', 'end_date', 'report_type'], keep='last').drop(['symbol'],
                                                                                                        axis=1)
        result_tmp['pub_date'] = result_tmp['pub_date'].map(
            lambda x: x[0:10] if type(x) == str else x.strftime('%Y-%m-%d'))

        result_tmp['stock_count'] = result_tmp['stock_count'].map(lambda x: 10 if x < 10 else x)

        if pd_fund_portfolio is None:
            g.pd_stock_count = (dt, result_tmp)
        else:
            result_tmp = pd.concat([pd_fund_portfolio, result_tmp])
            g.pd_stock_count = (dt, result_tmp)

    pd_fund_portfolio = g.pd_stock_count[1]
    result_alpha_plus_10 = alpha_plus_10(pd_fund_portfolio)
    return result_alpha_plus_10


def contition_func(context):
    c1 = contition_1(context)
    c2 = contition_2(context)
    g.condition_data = pd.merge(c1, c2, on=['code', 'end_date'], how='left')
    print('contition_func处理完毕')


def alpha_1(context):
    pd_stock_count = g.pd_stock_count[1]
    pd_stock_count = pd_stock_count[pd_stock_count['code'].map(lambda x: x in g.stock_list)]
    alpha_1_result = util_pct_stock_count(pd_stock_count)
    return alpha_1_result


def alpha_2(context):
    pd_price_all = util_get_month_K(g.stock_list)
    pd_price_all = alpha_profit_month(pd_price_all)
    return pd_price_all


def alpha_func(context):
    a1 = alpha_1(context)
    a2 = alpha_2(context)
    c_all = pd.merge(a1, a2, on=['code', 'end_date'], how='outer')
    g.alpha_data = pd.merge(g.condition_data, c_all, on=['code', 'end_date'])
    print('alpha_func处理完毕')


def util_get_month_turnOver(list_code_tmp, end_date):
    list_result_tmp = util_run_func_by_chunk(list_code_tmp, 100, get_valuation, end_date=end_date,
                                             count=100,
                                             fields=['turnover_ratio'])
    pd_result = pd.concat(list_result_tmp)
    pd_result['end_date'] = pd.to_datetime(pd_result['day'], format='%Y-%m-%d')
    del pd_result['day']
    pd_result = pd.pivot_table(pd_result, index=['end_date'], columns=['code'], values=['turnover_ratio']).fillna(-1)
    pd_result = pd_result.resample('M').mean()
    pd_result.columns = pd_result.columns.levels[1]
    pd_result = pd_result.unstack().reset_index().rename(columns={0: 'turnOver_monthly'})
    pd_result['end_date'] = pd_result['end_date'].map(lambda x: x.strftime('%Y-%m-%d'))
    return pd_result


def alpha_turnOver_30(data_o, N):
    def alpha_turnOver_30_inner(data_grp_o):
        threshold = 0
        x = data_grp_o['turnOver_monthly'].values
        result = []
        for i in range(len(x)):
            one_x = x[i]
            if threshold >= N:
                result.append(one_x)
            else:
                result.append(0)
            if 0.3 < one_x < 0.8:
                threshold += 1
            else:
                threshold = 0
        return pd.Series(result, index=data_grp_o['end_date'])

    pd_tmp = data_o.groupby(['code']).apply(alpha_turnOver_30_inner).unstack().reset_index().rename(
        columns={0: 'alpha_turnOver_30'}).set_index(['code', 'end_date'])
    pd_tmp = pd_tmp[pd_tmp['alpha_turnOver_30'] > 0]
    return pd_tmp


def get_fund_portfolio_by_year(start_year, end_year, origin_date, now_date):
    dict_fund_pub_date = {'第四季度': ('12-31', '01-31'), '第一季度': ('03-31', '04-30'),
                          '第二季度': ('06-30', '07-31'), '第三季度': ('09-30', '10-31')}

    result = []

    for i in range(end_year - start_year + 1):
        year_iter = start_year + i

        for key, value in dict_fund_pub_date.items():

            if key == '第四季度' or key == '年度':
                year_real_start = str(year_iter - 1) + '-' + value[0]
            else:
                year_real_start = str(year_iter) + '-' + value[0]

            year_real_end = str(year_iter) + '-' + value[1]

            # print(year_real_start + '----' + year_real_end)
            # print(now_date, origin_date)
            if (datetime.strptime(year_real_end, '%Y-%m-%d').date() > now_date + timedelta(days=20)) or (
                    datetime.strptime(year_real_start, '%Y-%m-%d').date() < origin_date):
                continue
            print(year_real_start + '----' + year_real_end)

            q = query(finance.FUND_PORTFOLIO.code, finance.FUND_PORTFOLIO.total_asset).filter(
                finance.FUND_PORTFOLIO.pub_date <= year_real_end,
                finance.FUND_PORTFOLIO.pub_date >= year_real_start)
            fund_asset = finance.run_query(q)
            # 筛选出净值大约1个亿的基金，基金净值过小容易被清盘，风格漂移
            l_fund_asset_code = fund_asset[fund_asset['total_asset'] >= 100000000]['code'].values.tolist()

            total_fund = len(set(l_fund_asset_code))

            # 得到符合要求的基金的所有持仓，年报所有股票，季报为持仓TOP10的
            fund_portfolio = util_get_fund_portfolio_by_chunks(l_fund_asset_code, year_real_start, year_real_end, key)
            fund_portfolio['total_fund'] = total_fund
            result.append(fund_portfolio)
    if len(result) != 0:
        pd_result = pd.concat(result)
    else:
        raise ValueError()
    return pd_result


def util_get_fund_portfolio_by_chunks(code_list: list, start_date, end_date, rec_type):
    """
    在start_date 到 end_date 日期内，list_stock里的股票被所有基金的持仓情况统计
    """

    def chunks(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    pd_result = pd.DataFrame.from_dict({
        'symbol': [], 'start_date': [], 'end_date': [], 'report_type': [], 'stock_count': [],
        'share_sum': [], 'market_cap_sum': []
    })

    for once_code in chunks(code_list, 60):

        q = query(finance.FUND_PORTFOLIO_STOCK).filter(finance.FUND_PORTFOLIO_STOCK.code.in_(once_code),
                                                       finance.FUND_PORTFOLIO_STOCK.pub_date <= end_date,
                                                       finance.FUND_PORTFOLIO_STOCK.pub_date >= start_date,
                                                       finance.FUND_PORTFOLIO_STOCK.report_type == rec_type,
                                                       finance.FUND_PORTFOLIO_STOCK.rank <= 50,
                                                       )

        fund_portfolio = finance.run_query(q)

        stock_als = fund_portfolio.groupby(['symbol', 'report_type']).agg(
            {'shares': 'sum', 'symbol': 'count', 'market_cap': 'sum'}).rename(
            columns={'symbol': 'stock_count', 'shares': 'share_sum', 'market_cap': 'market_cap_sum'}).reset_index()

        if len(stock_als) != 0:
            stock_als['start_date'] = start_date
            stock_als['end_date'] = end_date
            stock_als = stock_als[
                ['symbol', 'start_date', 'end_date', 'report_type', 'stock_count', 'share_sum', 'market_cap_sum']]
            pd_result = pd.concat([pd_result, stock_als]).groupby(
                ['symbol', 'start_date', 'end_date', 'report_type']).sum().reset_index()

    return pd_result


def util_normalize_code(pd_data, code_col_name='code'):
    """
    通过股票code 得到股票名称
    """
    l_code_o = pd_data[code_col_name].values.tolist()
    l_norm_code = []

    for one_code in l_code_o:
        try:
            l_norm_code.append(normalize_code(one_code))
        except:
            l_norm_code.append('unknown')

    pd_data['code'] = l_norm_code
    return pd_data


def util_add_pub_year(pd_data, code_col_name='code'):
    """
    通过股票code 得到股票名称
    """

    l_code_o = pd_data[code_col_name].values.tolist()
    l_name = []

    for one_code in l_code_o:
        try:
            l_name.append(get_security_info(one_code).start_date)
        except:
            l_name.append('未知')

    pd_data['pub_date'] = l_name
    return pd_data


def util_add_name(pd_data, code_col_name='code'):
    """
    通过股票code 得到股票名称
    """

    l_code_o = pd_data[code_col_name].values.tolist()
    l_name = []

    for one_code in l_code_o:
        try:
            l_name.append(get_security_info(one_code).display_name)
        except:
            l_name.append('未知')

    pd_data['display_name'] = l_name
    return pd_data


def alpha_plus_10(data_o, N=4):
    data_o['flag'] = data_o['stock_count'].map(lambda x: 1 if x > 10 else 0)
    data_o['alpha_plus_10'] = data_o.groupby('code')['flag'].cumsum()
    data_o = data_o[data_o['alpha_plus_10'] >= N]
    return data_o[['code', 'end_date', 'alpha_plus_10']]


def util_pct_stock_count(data_o):
    data_o = data_o.sort_values(by=['code', 'end_date'])
    data_o['alpha_pct_stockCount'] = data_o.groupby(['code'])['stock_count'].pct_change()
    return data_o[['code', 'end_date', 'alpha_pct_stockCount']]


def util_get_month_K(list_code):
    close = history(security_list=list_code, unit='1d', count=365, field='close')
    close = close.resample('M').last().unstack()

    open = history(security_list=list_code, unit='1d', count=365, field='open')
    open = open.resample('M').first().unstack()

    high = history(security_list=list_code, unit='1d', count=365, field='high')
    high = high.resample('M').max().unstack()

    low = history(security_list=list_code, unit='1d', count=365, field='low')
    low = low.resample('M').min().unstack()

    result = pd.concat([open, close, high, low], axis=1).reset_index().rename(
        columns={'level_0': 'code', 'level_1': 'end_date', 0: 'open', 1: 'close', 2: 'high', 3: 'low'})
    result['end_date'] = result['end_date'].map(lambda x: x.strftime('%Y-%m-%d'))
    return result


def alpha_profit_month(data_p):
    def alpha_profit_month_inner(data_grp_o):
        price_all = data_grp_o['close'].values
        cum_profit_10 = []
        cum_profit_1 = []
        cum_profit_3 = []
        for i, one_price in enumerate(price_all):
            if i >= 1:
                cum_profit_1.append((one_price - price_all[0]) / price_all[0])
            else:
                cum_profit_1.append(0)
            if i >= 3:
                cum_profit_3.append(((one_price - price_all[i - 3]) / price_all[i - 3]))
            else:
                cum_profit_3.append(0)
            if i >= 10:
                cum_profit_10.append((one_price - price_all[i - 10]) / price_all[i - 10])
            else:
                cum_profit_10.append(0)

        pd_result = pd.DataFrame.from_dict(
            {'cum_profit_10': cum_profit_10, 'cum_profit_1': cum_profit_1, 'cum_profit_3': cum_profit_3})
        pd_result['smart_price'] = 0.3 * pd_result['cum_profit_10'] + 0.3 * pd_result['cum_profit_3'] + 0.4 * pd_result[
            'cum_profit_1']
        pd_result = pd_result.set_index(data_grp_o['end_date'])
        return pd_result

    r = data_p.groupby(['code']).apply(alpha_profit_month_inner).reset_index().drop_duplicates().set_index(
        ['end_date', 'code'])
    r = r.rename(columns={'cum_profit_10': 'alpha_profit_month_10', 'cum_profit_1': 'alpha_profit_month_1',
                          'cum_profit_3': 'alpha_profit_month_3'})
    return r


def buy_method_market(data_all, method_name, context, **kargs):
    def buy_by_sort(**kargs_inner):
        data_o = data_all.dropna()
        if kargs.get('sort') is not None:
            data_o = data_o.sort_values(**kargs.get('sort')).groupby(['end_date']).head(kargs_inner.get('N'))
        else:
            data_o = data_o.sample(frac=1).groupby(['end_date']).head(kargs_inner.get('N'))
        buy_time = MarketTiming.buy_every_fund_released([2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021])
        data_o = pd.merge(data_o, buy_time, on=['end_date'])
        return data_o

    def buy_custom_1(**kargs_inner):
        print('购买策略:buy_custom_1')
        fund_release_date = buy_every_fund_released([2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021])[
            'end_date'].values.tolist()
        dt = context.current_dt.date()
        dt_str = dt.strftime('%Y-%m-%d')

        dt_str_format = last_day_of_month(dt).strftime('%Y-%m-%d')

        if dt_str_format in fund_release_date:
            print('季末购买日：' + dt_str_format)
            data_o = data_all.dropna()
            code_now = data_o.sort_values(**kargs.get('sort_1')).head(kargs_inner.get('N_1'))[
                'code'].values.tolist()
            g.flag = 2
        else:
            data_one_date = data_all[data_all['end_date'] == dt_str_format]
            data_one_date = data_one_date[data_one_date['code'].map(lambda x: x in g.code_bought)]
            if len(data_one_date) == 0:
                g.sell_list = g.code_bought
                g.buy_list = set()
                g.hold_list = set()
                g.code_bought = set()
                print(dt_str_format + '：当月无优质标的')
                return
            if g.flag == 2:
                code_now = data_one_date.sort_values(**kargs.get('sort_2')).head(kargs_inner.get('N_2'))[
                    'code'].values.tolist()
                g.flag += 1
            elif g.flag == 3:
                code_now = data_one_date.sort_values(**kargs.get('sort_3')).head(kargs_inner.get('N_3'))[
                    'code'].values.tolist()
            else:
                raise ValueError()
        g.buy_list = set(code_now) - g.code_bought
        g.sell_list = g.code_bought - set(code_now)
        g.hold_list = set(code_now).intersection(g.code_bought)
        g.code_bought = set(code_now)

        print({dt_str: g.code_bought})

    return locals().get(method_name)(**kargs)


def buy_every_fund_released(list_year, to_str=True):
    import datetime
    result = []

    def last_day_of_month(any_day):
        next_month = any_day.replace(day=28) + datetime.timedelta(days=4)
        return next_month - datetime.timedelta(days=next_month.day)

    for year in list_year:
        for month in [1, 4, 7, 10]:
            day = last_day_of_month(datetime.date(year, month, 1))
            if to_str:
                result.append(day.strftime('%Y-%m-%d'))
            else:
                result.append(day)
    return pd.DataFrame.from_dict({'end_date': result})


def main_buy(context):
    # #### base
    #     rank_method = {'N': 4}
    #     r_tmp = buy_method_market(alpha_concat, 'buy_by_sort', **rank_method)

    # #### 方案一
    #     rank_method = {'sort': {'by': ['alpha_pct_stockCount', 'alpha_profit_month_1', 'alpha_turnOver_30'],
    #                             'ascending': [False, False, True]}, 'N': 4}
    #     r_tmp = buy_method_market(alpha_concat, 'buy_by_sort', **rank_method)

    #     # 方案二
    print('开始进入购买流程')
    rank_method = {'sort_1': {'by': ['alpha_pct_stockCount', 'alpha_profit_month_3', 'alpha_turnOver_30'],
                              'ascending': [False, False, True]}, 'N_1': 10,
                   'sort_2': {'by': ['alpha_profit_month_3', 'alpha_turnOver_30'],
                              'ascending': [True, True]}, 'N_2': 6,
                   'sort_3': {'by': ['alpha_profit_month_3', 'alpha_turnOver_30'],
                              'ascending': [True, True]}, 'N_3': 3}
    r_tmp = buy_method_market(g.alpha_data, 'buy_custom_1', context, **rank_method)

    return r_tmp


def last_day_of_month(any_day):
    import datetime
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)
    return next_month - datetime.timedelta(days=next_month.day)


def buy_with_cash_equal(context):
    print('买入：' + ','.join(list(g.buy_list)))
    cash_equal = context.portfolio.total_value / (len(g.hold_list) + len(g.buy_list))
    for one_stock in g.buy_list.union(g.hold_list):
        result = order_target_value(one_stock, cash_equal)
        if result is None:
            print('ERROR(买入失败,现金：' + str(cash_equal) + '):' + one_stock)
    print(context.portfolio)


def sell_all(context):
    print('卖出：' + ','.join(list(g.sell_list)))
    for one_stock in g.sell_list:
        result = order_target(one_stock, 0)
        if result is None:
            print('ERROR(卖出失败):' + one_stock)
            g.code_bought.add(one_stock)


def buy_with_cash_equal_2(context):
    print('买入：' + ','.join(list(g.buy_list)))
    if len(g.buy_list) == 0:
        return
    cash_equal = context.portfolio.total_value / len(g.buy_list)
    for one_stock in g.buy_list:
        result = order_value(one_stock, cash_equal)
        if result is None:
            print('ERROR(买入失败,现金：' + str(cash_equal) + '):' + one_stock)
    print(context.portfolio.positions)


def sell_all_2(context):
    print('卖出：' + ','.join(list(g.sell_list)))
    for one_stock in g.sell_list:
        result = order_target(one_stock, 0)
        if result is None:
            print('ERROR(卖出失败):' + one_stock)
            g.code_bought.add(one_stock)
    if len(g.buy_list) == 0:
        return


def get_all_candidate_stock(context):
    stock_list = get_industry_stocks('HY001') + get_industry_stocks('HY002') \
                 + get_industry_stocks('HY003') + get_industry_stocks('HY004') \
                 + get_industry_stocks('HY005') + get_industry_stocks('HY006') \
                 + get_industry_stocks('HY007') + get_industry_stocks('HY008') \
                 + get_industry_stocks('HY009') + get_industry_stocks('HY010') \
                 + get_industry_stocks('HY011')
    g.stock_list = list(set(filter_special(context, stock_list)))
    print('候选集处理完毕')


def filter_special(context, stock_list):  # 过滤器，过滤停牌，ST，科创，新股
    curr_data = get_current_data()
    stock_list = [stock for stock in stock_list if not curr_data[stock].is_st]
    stock_list = [stock for stock in stock_list if not curr_data[stock].paused]
    stock_list = [stock for stock in stock_list if '退' not in curr_data[stock].name]
    return stock_list
