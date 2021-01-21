# 导入函数库
from jqdata import *
import random
import pandas as pd


def initialize(context):
    set_const_param()
    set_variable_param()
    set_backtest()
    set_slip_fee(context)
    log.set_level('order', 'error')

    get_all_candidate_stock(context)

    run_monthly(get_all_candidate_stock, monthday=1, time='08:30', reference_security=g.index_security)
    run_daily(before_market_open, time='08:30', reference_security=g.index_security)
    run_daily(strategy_pipeline, time='every_bar', reference_security=g.index_security)
    run_daily(after_market_close, time='16:00', reference_security=g.index_security)


def set_const_param():
    g.index_security = '000300.XSHG'
    g.per_share = 1
    g.buy_num_total = 3
    g.buy_num_current = 0

    g.stort_in_period = 20
    g.short_out_period = 10
    g.short_init_maxback_thold = 2
    g.short_add_thold = 0.5
    g.short_N_num = 4

    g.long_in_period = 55
    g.long_out_period = 20
    g.long_init_maxback_thold = 2
    g.long_add_thold = 0.5
    g.long_N_num = 4

    g.his_price_position = None

    g.loss = 0.2
    g.adjust = 0.9


def set_variable_param():
    g.days = 1
    g.days_atr = 35

    g.stort_N = {}
    g.short_break_price = {}

    g.long_N = {}
    g.long_break_price = {}


def set_backtest():
    set_benchmark(g.index_security)
    set_option('use_real_price', True)


# 4 根据不同的时间段设置滑点与手续费
def set_slip_fee(context):
    # 将滑点设置为0
    set_slippage(FixedSlippage(0))
    # 根据不同的时间段设置手续费
    dt = context.current_dt

    if dt > datetime.datetime(2013, 1, 1):
        set_commission(PerTrade(buy_cost=0.0003, sell_cost=0.0013, min_cost=5))

    elif dt > datetime.datetime(2011, 1, 1):
        set_commission(PerTrade(buy_cost=0.001, sell_cost=0.002, min_cost=5))

    elif dt > datetime.datetime(2009, 1, 1):
        set_commission(PerTrade(buy_cost=0.002, sell_cost=0.003, min_cost=5))

    else:
        set_commission(PerTrade(buy_cost=0.003, sell_cost=0.004, min_cost=5))


def before_market_open(context):
    # log.info('函数运行时间(before_market_open):' + str(context.current_dt.time()))
    positions = set(context.portfolio.positions.keys())
    # adjust_cash(context)
    calculate_N(context, list(positions))


def market_add(context, break_price, N_period, thold):
    positions = set(context.portfolio.positions.keys())
    if len(positions) == 0:
        return
    # @ todo 加仓可以加任意次数
    dt = context.current_dt  # 当前日期
    current_price = get_price(list(positions), end_date=dt, count=1, frequency='1m', panel=False, fields=['close'],
                              fill_paused=True)
    d_current_price = current_price.set_index(['code']).to_dict()['close']

    for stock_code in positions:
        one_current_price = d_current_price[stock_code]
        one_N = N_period[stock_code]
        buy_price = break_price[stock_code]
        if one_current_price >= buy_price + one_N * thold:
            state = order_by_unit(context,stock_code, one_current_price, one_N,'加仓')
            if state:
                break_price[stock_code] = current_price
                g.buy_num_current += 1


def market_out(context, d_break_price):
    positions = set(context.portfolio.positions.keys())
    if (len(positions) == 0) | (g.his_price_position is None):
        return

    dt = context.current_dt
    current_price = get_price(list(positions), end_date=dt, count=1, frequency='1m', panel=False, fields=['close'],
                              fill_paused=True)

    price_all = pd.merge(current_price, g.his_price_position, on=['code'])
    price_all_break = price_all[price_all['close'] < price_all['his_close']]
    l_price_all_break = price_all_break['code'].values.tolist()
    for stock_code in l_price_all_break:
        order_status = order_target(stock_code, 0)
        if order_status is not None:
            d_break_price.pop(stock_code)
            log.info(str(context.current_dt.time())+'：平仓的股票：',stock_code)


def stop_loss(context, d_break_price, d_N_period):
    positions = set(context.portfolio.positions.keys())
    dt = context.current_dt  # 当前日期
    if len(positions) == 0:
        return
    df_current_price = get_price(list(positions), end_date=dt, count=1, frequency='1m', panel=False, fields=['close'],
                                 fill_paused=True)
    df_current_price = df_current_price.groupby(['code'])['close'].min().reset_index()
    d_current_price = df_current_price.set_index(['code']).to_dict()['close']
    for stock_code, one_current_price in d_current_price.items():
        one_N = d_N_period[stock_code]
        try:
            if one_current_price < (d_break_price[stock_code] - 2 * one_N):
                order_status = order_target(stock_code, 0)
                if order_status is not None:
                    d_break_price.pop(stock_code)
                    log.info(str(context.current_dt.time()) + '：止损的股票：', stock_code)
        except TypeError:
            log.warn('重要信息')
            log.warn(d_break_price[stock_code])
            log.warn(one_N)



def order_by_unit(context,stock_code, current_price, N_value,log_info='买进'):
    value = context.portfolio.portfolio_value
    cash = context.portfolio.cash
    dollar_volatility = g.per_share * N_value
    unit = value * 0.01 / dollar_volatility
    num_of_shares = cash / current_price
    unit = int(unit/100)*100
    if (num_of_shares >= unit) and (int(unit) >= 100):
        order_status = order(stock_code, int(unit))
        if order_status is not None:
            log.info(str(context.current_dt.time())+'：'+log_info+'：(股票,价格,数量,N_value)：', (stock_code,str(current_price),int(unit)))
            return True
    return False


# def adjust_cash(context):
#     # 当前持有的股票和现金的总价值
#     g.value = context.portfolio.portfolio_value
#     # 可花费的现金
#     g.cash = context.portfolio.cash
#     if g.value < (1 - g.loss) * context.portfolio.starting_cash:
#         g.cash *= g.adjust
#         g.value *= g.adjust


def market_in(context, in_period, break_price):
    positions = set(context.portfolio.positions.keys())
    l_not_position = list(g.stocks_exsit - positions)

    # @todo 当天购买不超过3只
    if g.buy_num_current >= g.buy_num_total:
        return
    dt = context.current_dt  # 当前日期
    prev_dt = context.previous_date  # 当前日期
    current_price = get_price(l_not_position, end_date=dt, count=1, frequency='1m', panel=False, fields=['close'],
                              fill_paused=True)
    # @todo 这里定义的是突破前N日的收盘价
    if dt.hour == 9 and dt.minute == 30:
        his_price = get_price(l_not_position, end_date=prev_dt, count=in_period, frequency='1d', panel=False,
                              fields=['close'])
        his_price = his_price.groupby(['code'])['close'].max().reset_index()
        his_price.rename(columns={'close': 'his_close'}, inplace=True)
        g.his_price = his_price

    price_all = pd.merge(current_price, g.his_price, on=['code'])
    price_all_break = price_all[price_all['close'] > price_all['his_close']]
    d_price_all_break = price_all_break.set_index(['code']).to_dict()['close']
    l_price_all_break = price_all_break['code'].values.tolist()
    # @todo 随机购买
    random.shuffle(l_price_all_break)
    # 买进时候计算
    calculate_N(context, l_price_all_break)
    for stock_code in l_price_all_break:
        current_price = d_price_all_break[stock_code]
        if in_period == g.stort_in_period:
            N_value = g.stort_N[stock_code]
        elif in_period == g.long_in_period:
            N_value = g.long_N[stock_code]
        else:
            raise ValueError
        state = order_by_unit(context,stock_code, current_price, N_value)
        if state:
            break_price[stock_code] = current_price
            g.buy_num_current += 1
    print(break_price)



def strategy_pipeline(context):
    market_in(context, in_period=g.stort_in_period, break_price=g.short_break_price)
    market_add(context, break_price=g.short_break_price,
               thold=g.short_add_thold, N_period=g.stort_N)
    market_out(context, d_break_price=g.short_break_price)
    stop_loss(context, d_break_price=g.short_break_price, d_N_period=g.stort_N)


def get_all_candidate_stock(context):
    g.stocks_exsit = get_industry_stocks('HY001') + get_industry_stocks('HY002') \
                     + get_industry_stocks('HY003') + get_industry_stocks('HY004') \
                     + get_industry_stocks('HY005') + get_industry_stocks('HY006') \
                     + get_industry_stocks('HY007') + get_industry_stocks('HY008') \
                     + get_industry_stocks('HY009') + get_industry_stocks('HY010') \
                     + get_industry_stocks('HY011')
    g.stocks_exsit = set(filter_special(context, g.stocks_exsit))


def filter_special(context, stock_list):  # 过滤器，过滤停牌，ST，科创，新股
    curr_data = get_current_data()
    stock_list = [stock for stock in stock_list if not curr_data[stock].is_st]
    stock_list = [stock for stock in stock_list if not curr_data[stock].paused]
    stock_list = [stock for stock in stock_list if '退' not in curr_data[stock].name]
    stock_list = [stock for stock in stock_list if
                  (context.current_dt.date() - get_security_info(stock).start_date).days > 150]
    return stock_list


## 收盘后运行函数
def after_market_close(context):
    dt = context.current_dt
    positions = set(context.portfolio.positions.keys())
    log.info(str('函数运行时间(after_market_close):' + str(context.current_dt.time())))
    if len(positions)==0:
        return
    his_price = get_price(list(positions), end_date=dt, count=g.short_out_period, frequency='daily',
                          panel=False,
                          fields=['close'])
    his_price = his_price.groupby(['code'])['close'].min().reset_index()
    his_price.rename(columns={'close': 'his_close'}, inplace=True)
    g.his_price_position = his_price
    log.info('一天结束,今日持仓为：')
    log.info(positions)
    log.info('##############################################################')


def calculate_N(context, cal_position):
    def cal_real_N(x, grp_key):
        values_high = x['high'].values
        values_low = x['low'].values
        values_pre_close = x['pre_close'].values
        for i in range(len(values_high[-20:])):
            h_l = values_high[i] - values_low[i]
            h_c = values_high[i] - values_pre_close[i]
            c_l = values_pre_close[i] - values_low[i]
            tr = max(h_l, h_c, c_l)
            if g.stort_N.get(x[grp_key].values[0]) is not None:
                g.stort_N[x[grp_key].values[0]] = (tr + i * g.stort_N.get(x[grp_key].values[0])) / (i + 1)
            else:
                g.stort_N[x[grp_key].values[0]] = tr

        for i in range(len(values_high)):
            h_l = values_high[i] - values_low[i]
            h_c = values_high[i] - values_pre_close[i]
            c_l = values_pre_close[i] - values_low[i]
            tr = max(h_l, h_c, c_l)
            if g.long_N.get(x[grp_key].values[0]) is not None:
                g.long_N[x[grp_key].values[0]] = (tr + i * g.long_N.get(x[grp_key].values[0])) / (i + 1)
            else:
                g.long_N[x[grp_key].values[0]] = tr

    if len(cal_position) == 0:
        return
    prev_dt = context.previous_date  # 当前日期
    current_price = get_price(cal_position, end_date=prev_dt, count=g.long_in_period, frequency='1d', panel=False,
                              fields=['high', 'low', 'pre_close'],
                              fill_paused=True)

    current_price.groupby(['code']).apply(lambda x: cal_real_N(x, 'code'))
