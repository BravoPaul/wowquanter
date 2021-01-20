# 克隆自聚宽文章：https://www.joinquant.com/post/31136
# 标题：2020年年化收益222%，回撤10%
# 作者：苦咖啡

import csv
import os
import xlrd
import math
import jqdata
import numpy as np
import pandas as pd
from six import BytesIO
from jqlib.alpha101 import *
from pandas import DataFrame, Series
from jqdata import *
from kuanke.wizard import *


# 初始化程序, 整个回测只运行一次
def initialize(context):
    g.index_security = '000300.XSHG'
    # 设定沪深300作为基准
    set_benchmark(g.index_security)
    # 开启动态复权模式(真实价格)
    set_option('use_real_price', True)
    log.info('初始函数开始运行且全局只运行一次')
    # 过滤掉order系列API产生的比error级别低的log
    log.set_level('order', 'error')
    before_market_open(context)
    g.filter_paused = True
    g.max_hold_stocknum = 3
    # 股票类每笔交易时的手续费是：买入时佣金万分之三，卖出时佣金万分之三加千分之一印花税, 每笔交易佣金最低扣5块钱
    set_order_cost(OrderCost(close_tax=0.001, open_commission=0.0003, close_commission=0.0003, min_commission=5),
                   type='stock')

    run_monthly(before_market_open, 1, time='before_open', reference_security=g.index_security)
    run_weekly(market_open, 1, time='9:35', reference_security='000300.XSHG')


def risk_control(context):
    hold_stock = list(context.portfolio.positions.keys())
    stocks_close = history(count4, '1d', 'close', hold_stock)
    for security in hold_stock:
        if max_high(security, 60, 0, 59) / stocks_close[security][-1] < 1.15:
            market_open(context)


## 开盘时运行函数
def market_open(context):
    # 卖出不在买入列表中的股票
    count4 = 180
    g.close4 = history(count4, '1d', 'close', g.stocks_exsit)
    g.buy_lists = []
    g.buy_list = []
    g.buy_list2 = []
    g.buy_list1 = []

    a = query(valuation.code).filter(valuation.code.in_(g.stocks_exsit), valuation.market_cap > 1000,
                                     valuation.pe_ratio > 15, valuation.pe_ratio < 70)
    g.buy_list2 = list(get_fundamentals(a).code)

    for security in (g.buy_list2):
        if 0.75 < max_high(security, 180, 0, 59) / max_high(security, 120, 0, 59) < 1 \
                and 0.75 < max_high(security, 120, 0, 59) / max_high(security, 60, 0, 59) < 1:
            if 0.75 < min_low(security, 180, 0, 59) / min_low(security, 120, 0, 59) < 1 \
                    and 0.75 < min_low(security, 120, 0, 59) / min_low(security, 60, 0, 59) < 1:
                if avg_volume(security, 7) / avg_volume(security, 180) < 1.5 \
                        and 0.75 > g.close4[security][count4 - 180] / g.close4[security][-1] > 0.5:
                    if max_high(security, 60, 0, 59) / g.close4[security][-1] < 1.15 \
                            and g.close4[security][-1] < 500:
                        g.buy_list1.append(security)
                        log.info('%s ：%s：%s' % (security, get_security_info(security).display_name,
                                                avg_volume(security, 7) / avg_volume(security, 180)))

    a = query(valuation.code).filter(valuation.code.in_(g.buy_list1)).order_by(
        valuation.circulating_market_cap.desc()).limit(g.max_hold_stocknum)
    g.buy_list = list(get_fundamentals(a).code)

    hold_stock = list(context.portfolio.positions.keys())
    for security in g.buy_list:
        # 买不在持股列表中的股票
        # log.info('%s ：%s：%s：%s'%(security,get_security_info(security).display_name,avg_volume(security,7)/avg_volume(security,100),max_high(security,20,0,10)))
        if security not in hold_stock:
            g.buy_lists.append(security)

    if len(g.buy_list):
        log.info('___' * 10)
        log.info('股票池：%d ' % (len(g.buy_list)))
        log.info(g.buy_list)

    sell(context, g.buy_list)
    # 买入不在持仓中的股票，按要操作的股票平均资金
    buy(context, g.buy_lists)


# 交易函数 - 买入
def buy(context, buy_lists):
    # 获取最终的 buy_lists 列表
    log.info('buy')

    Num = len(buy_lists)

    if len(buy_lists) > 0:
        cash = context.portfolio.available_cash
        amount = cash / Num
        for stock in buy_lists:
            if len(context.portfolio.positions) < g.max_hold_stocknum:
                log.info(stock + get_security_info(stock).display_name + ' 购买资金：' + str(amount) + ' 剩余资金：' + str(
                    cash) + ' 热度：' + str(
                    avg_volume(stock, 7) / avg_volume(stock, 180)))  # order(stock, amount, MarketOrderStyle())
                order_target_value(stock, amount)
    return


# 交易函数 - 出场
def sell(context, buy_lists):
    # 获取 sell_lists 列表
    log.info('sell')
    hold_stock = list(context.portfolio.positions.keys())
    for s in hold_stock:  # 卖出不在买入列表中的股票
        if s not in buy_lists:
            log.info(s + get_security_info(s).display_name + ' sell')
            order_target_value(s, 0)


def before_market_open(context):
    g.stocks_exsit = get_industry_stocks('HY001') + get_industry_stocks('HY002') \
                     + get_industry_stocks('HY003') + get_industry_stocks('HY004') \
                     + get_industry_stocks('HY005') + get_industry_stocks('HY006') \
                     + get_industry_stocks('HY007') + get_industry_stocks('HY008') \
                     + get_industry_stocks('HY009') + get_industry_stocks('HY010') \
                     + get_industry_stocks('HY011')
    g.stocks_exsit = set(filter_special(context, g.stocks_exsit))  # 当天上市的所有股票，过滤了ST等


def avg_volume(security, timeperiod):
    security_data = attribute_history(security, timeperiod, '1d', ['volume'])
    avg_volume = security_data['volume'].mean()
    return avg_volume


def max_high(security, timeperiod, start, end):
    security_data = attribute_history(security, timeperiod, '1d', ['high'])
    max_high = 0
    for i in range(start, end):
        if security_data['high'][i] > max_high:
            max_high = security_data['high'][i]
    return max_high


def min_low(security, timeperiod, start, end):
    security_data = attribute_history(security, timeperiod, '1d', ['low'])
    min_low = 0
    for i in range(start, end):
        if security_data['low'][i] > min_low:
            min_low = security_data['low'][i]
    return min_low


def filter_special(context, stock_list):  # 过滤器，过滤停牌，ST，科创，新股
    curr_data = get_current_data()
    stock_list = [stock for stock in stock_list if not curr_data[stock].is_st]
    stock_list = [stock for stock in stock_list if not curr_data[stock].paused]
    stock_list = [stock for stock in stock_list if '退' not in curr_data[stock].name]
    stock_list = [stock for stock in stock_list if
                  (context.current_dt.date() - get_security_info(stock).start_date).days > 150]

    return stock_list