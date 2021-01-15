from jqdatasdk import *


auth('18201150441','5513561pkPK')


stocks = get_index_stocks('000300.XSHG')
print(stocks)