#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Dec 2023

@author: Sean Sanggyu Choi
"""

import numpy as np
import pandas as pd
import datetime as dt

import yfinance as yf
import time
# QQQfirms_csv_file_path = "/Users/apple/PROJECT/Code_4_10k/QQQ_constituents.csv"
# firms_df = pd.read_csv(QQQfirms_csv_file_path)
# firms_df = firms_df.drop(['Security', 'GICS Sector', 'GICS Sub-Industry', 'Headquarters Location', 'Date added', 'Founded'], axis=1)
# firms_df['CIK'] = firms_df['CIK'].apply(lambda x: str(x).zfill(10))
# firms_dict = firms_df.set_index('Symbol')['CIK'].to_dict()
# firms_dict = {value: key for key, value in firms_dict.items()}


def vol_reader(comp, rev_firms_dict ,start_date, end_date):

    stock = rev_firms_dict[comp]
    print(f'Downloading {stock} stock data')
    time_series = yf.download(stock, 
                            start = start_date,
                            end = end_date,
                            progress = False)
    def vol_proxy(ret, proxy):
        proxies = ['sqaured return', 'realized', 'daily-range', 'return']
        assert proxy in proxies, f'proxy should be in {proxies}'
        if proxy == 'realized':
            raise 'Realized volatility proxy not yet implemented'
        elif proxy == 'daily-range':
            ran = np.log(ret['High']) - np.log(ret['Low'])
            adj_factor = 4 * np.log(2)
            return np.square(ran)/adj_factor
        elif proxy == 'return':
            def ret_fun(xt_1, xt):
                return np.log(xt/xt_1)
            return ret_fun(ret['Open'], ret['Close'])
        else:
            assert proxy == 'squared return'
            raise 'Squared return proxy not yet implemented'
        
    vol_list = []
    for p in ['daily-range', 'return']:
        vol = vol_proxy(time_series, p)

        vol_list.append(vol)
        # vol_list.append(vol.to_frame())
    
    df_vol = pd.concat(vol_list, axis=1)
    df_vol.columns = ['_vol', '_ret']
    df_vol = df_vol.reset_index()
    df_vol['_vol+1'] = df_vol['_vol'].shift(-1)
    df_vol['_ret+1'] = df_vol['_ret'].shift(-1)
    df_vol = df_vol.dropna()
    df_vol.set_index('Date', inplace=True)
    return df_vol

# def vol_reader(comp, rev_firms_dict, start_date, end_date):
#     stock = rev_firms_dict[comp]
#     print(f'Downloading {stock} stock data...')
#     start_time = time.time()
#     df = yf.download(stock, start=start_date, end=end_date, progress=False, auto_adjust=False)[['Open', 'Close', 'High', 'Low']]
#     end_time = time.time()
#     print(f'Downloaded {stock} data in {end_time - start_time:.2f} seconds')
#     # Compute daily-range volatility proxy: (log(High) - log(Low))^2 / (4 * log(2))
#     log_range = np.log(df['High'].values) - np.log(df['Low'].values)
#     daily_range_vol = (log_range ** 2) / (4 * np.log(2))

#     # Compute log return: log(Close / Open)
#     log_ret = np.log(df['Close'].values / df['Open'].values)

#     # Build final DataFrame using numpy for speed
#     data = {
#         '_vol': daily_range_vol,
#         '_ret': log_ret,
#         '_vol+1': np.roll(daily_range_vol, -1),
#         '_ret+1': np.roll(log_ret, -1)
#     }

#     df_vol = pd.DataFrame(data, index=df.index)
#     df_vol = df_vol[:-1]  # Drop last row (shifted +1 has garbage)

#     print('df_vol shape:', df_vol.shape)
#     print('df_vol', df_vol.head())
#     return df_vol

def vol_reader2(comps, rev_firms_dict, start_date, end_date, window = None, extra_end = False, extra_start = False, AR = None):
    def ret_fun(xt_1, xt): # log difference
        return np.log(xt/xt_1)

    ts = []
    empty = []
    if extra_end:
        if window:
            end_date = str(dt.datetime.strptime(end_date, '%Y-%m-%d')+dt.timedelta(days=window + 3))[:10]
        else:
            end_date = str(dt.datetime.strptime(end_date, '%Y-%m-%d')+dt.timedelta(days= 1))[:10]
    if extra_start and window:
        if AR:
            start_date = str(dt.datetime.strptime(start_date, '%Y-%m-%d')-dt.timedelta(days=window*AR + 1))[:10]
        else:
            start_date = str(dt.datetime.strptime(start_date, '%Y-%m-%d')-dt.timedelta(days=window + 1))[:10]
            
    for cc in comps:
        stock = rev_firms_dict[cc]
        print(f'Downloading {stock} stock data')
        time_series = yf.download(stock, 
                            start=start_date, 
                            end=end_date, 
                            progress=False)
        if time_series.empty:
            print(f'{stock} data is empty')
            empty.append(cc)
            continue
        ts.append(time_series)
    comps = list(set(comps) - set(empty))
        
    def vol_proxy(ret, proxy):
        proxies = ['squared return','realized','daily-range', 'return']
        assert proxy in proxies, f'proxy should be in {proxies}'
        if proxy == 'realized':
            raise 'Realized volatiliy proxy not yet implemented'
        elif proxy == 'daily-range':
            ran = np.log(ret['High']) - np.log(ret['Low'])
            adj_factor = 4*np.log(2)
            return np.square(ran)/adj_factor
        elif proxy == 'return':
            #print('Computing Open-Close log-diff returns')
            return ret_fun(ret['Open'], ret['Close'])
        else:
            assert proxy == 'squared return'
            raise 'Sqaured return proxy not yet implemented'
    
    def vol_proxy_window(ret, proxy, window):
        proxies = ['squared return', 'realized', 'daily-range', 'return']
        assert proxy in proxies, f'proxy should be in {proxies}'
        if proxy == 'return':
            t1 = ret.index + dt.timedelta(days=window-1)
            t1_adj = list(t1)
            for t in range(len(t1)):
                t_new = t1[t]
                while t_new not in ret.index:
                    t_new -= dt.timedelta(days=1)
                t1_adj[t] = t_new
            ret1 = ret.loc[t1_adj]
            ret1.index = ret.index
            remove_last = t1_adj.count(t1_adj[-1]) - 1
            if remove_last > 0:
                ret1 = ret1[:-remove_last]
                ret = ret[:-remove_last]
            return ret_fun(ret['Open'], ret1['Close'])
        elif proxy == 'realized':
            daily_ran_sq = np.square(np.log(ret['High'])-np.log(ret['Low']))/4*np.log(2)
            volvol = pd.Series(0, index=ret.index)
            for t in volvol.index:
                tt = t
                vv = 0
                N = 0
                past_date = t + dt.timedelta(days=window)
                while t < past_date:
                    if t in daily_ran_sq.index:
                        vv += daily_ran_sq.loc[t]
                        N += 1
                    t += dt.timedelta(days=1)
                volvol.loc[tt] = vv/N
            return volvol
        else:
            raise 'Proxy not introduced yet'
            
    ret_list = []
    vol_list = []
            
    if window:
        assert window > 0 and isinstance(window, int),'Incorrect window specified'
        for cc in range(len(comps)):
            ret = vol_proxy_window(ts[cc], 'return', window=window)
            # ret_list.append(ret.to_frame())
            ret_list.append(ret)
            vol = vol_proxy_window(ts[cc], 'realized', window=window)
            # vol_list.append(vol.to_frame())
            vol_list.append(vol)
    else:
        for cc in range(len(comps)):
            ret = vol_proxy(ts[cc], 'return')
            # ret_list.append(ret.to_frame())
            ret_list.append(ret)
            vol = vol_proxy(ts[cc], 'daily-range')
            # vol_list.append(vol.to_frame())
            vol_list.append(vol)
            
    df_ret = pd.concat(ret_list, axis=1)
    df_ret.columns = comps
    df_ret = df_ret.fillna(method='bfill')
    df_ret = df_ret.dropna()
    df_vol = pd.concat(vol_list, axis=1)
    df_vol.columns = comps
    df_vol = df_vol.fillna(method='bfill')
    df_vol = df_vol.dropna()
    

    return df_ret, df_vol
    

def price_reader(comps, rev_firms_dict, start_date, end_date):

    # firms_dict = {ticker : cik}
    # comps = [cik]
    ts = []
    empty = []
    for cc in comps:
        stock = rev_firms_dict[cc]
        try:
            print(f'Downloading {stock} stock data')
            time_series = yf.download(stock,
                                    start=start_date,
                                    end=end_date,
                                    progress=False)
            if time_series.empty:
                print(f'{stock} data is empty')
                empty.append(cc)
                continue
            ts.append(time_series)
        except Exception as e:
            print(f'Error: {e}')
            print(f'Could not download {stock} data')
            pass
    comps = list(set(comps) - set(empty))
    ret_list = []
    for cc in range(len(comps)):
        ret = ts[cc]['Open']
        # ret = ret/ret[0] * 100
        ret_list.append(ret)
        # ret_list.append(ret.to_frame())
        
    df_ret = pd.concat(ret_list, axis=1)
    df_ret.columns = comps
    df_ret = df_ret.fillna(method='bfill')
    df_ret = df_ret.dropna()
    
    return df_ret, comps

"""
#%% Aligning to future returns
start_date = '2012-01-01'
end_date = '2022-05-31'
comps = ['TOY','VOW','HYU','GM','FOR','NIS','HON','REN','SUZ','SAI']
first = True
for c in comps:
    x = vol_reader(c, start_date, end_date)
    x['_vol+1'] = x['_vol'].shift(-1)
    x['_ret+1'] = x['_ret'].shift(-1)
    x = x.drop(columns = ['_ret','_vol'])
    x['_stock'] = c
    if first:
        x_all = x
        first=False
    else:
        x_all = pd.concat([x_all,x],axis=0)
x_all = x_all.dropna()
new_ind1 = list(x_all['_stock'])
new_ind2 = list(x_all.index)
assert len(new_ind1) == len(new_ind2)
new_ind = []
for i in range(len(new_ind1)):
    new_ind.append(f'{new_ind1[i]}_{new_ind2[i]}')
x_all_new = x_all
x_all_new.index = new_ind

df2 = df_all
new_ind1 = list(df2['_stock'])
new_ind2 = list(df2.index)
assert len(new_ind1) == len(new_ind2)
new_ind = []
for i in range(len(new_ind1)):
    new_ind.append(f'{new_ind1[i]}_{new_ind2[i]}')
df2_new = df2
df2_new.index = new_ind

df3 = df2_new.join()


#%% Plotting ts for each company

import matplotlib.pyplot as plt
start_date = '2012-01-01'
end_date = '2022-05-31'
comps = ['TOY','VOW','HYU','GM','FOR','NIS','HON','REN','SUZ','SAI']
for c in comps:
    vv = vol_reader(c,start_date=start_date, end_date=end_date)
    fig,ax = plt.subplots()
    ax.plot(vv['_ret'])
    ax.set_ylabel('Daily Return')
    ax.set_xlabel('Date')
    fig.suptitle(c)
    plt.show()
    fig,ax = plt.subplots()
    ax.plot(vv['_vol'])
    ax.set_ylabel('Volatility')
    ax.set_xlabel('Date')
    fig.suptitle(c)
    plt.show()
"""

