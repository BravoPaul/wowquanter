import pandas as pd

di = {'haha': ['1', '1', '2', '3'], 'name': [5, 2, 2, 4]}

p = pd.DataFrame.from_dict(di)

g = {'1': 0, '2': 3}



def grp_func(x, grp_key):
    # print(x)
    # print()
    # print(x[grp_key][0])
    if g.get(x[grp_key].values[0]) is not None:
        g[x[grp_key].values[0]] = g[x[grp_key].values[0]]+sum(x['name'].tolist())


grp_p = p.groupby(['haha']).apply(lambda x:grp_func(x,grp_key='haha'))

print(g)

