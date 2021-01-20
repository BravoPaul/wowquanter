from jqdatasdk import *


auth('18201150441','5513561pkPK')


stocks = get_index_stocks('000300.XSHG')
print(stocks)

g.stort_in_period = 20
g.short_out_period = 10
g.short_init_maxback_thold = 2
g.short_add_thold = 0.5
g.short_N = 4

g.long_N = {}
g.long_days = {}
g.long_break_price = {}
g.long_stock = {}