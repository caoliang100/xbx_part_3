"""
《邢不行-2020新版|Python数字货币量化投资课程》
无需编程基础，助教答疑服务，专属策略网站，一旦加入，永续更新。
课程详细介绍：https://quantclass.cn/crypto/class
邢不行微信: xbx9025
本程序作者: 邢不行

# 课程内容
调用各个函数，查看策略结果
"""
import pandas as pd
from datetime import timedelta
from multiprocessing import Pool, Manager
from datetime import datetime

from Signals import *
from Position import *
from Evaluate import *
import glob

pd.set_option('expand_frame_repr', False)  # 当列太多时不换行

# =====手工设定策略参数
# para = [200, 2, 0.05]

symbol = 'BTC-USDT_5m'
face_value = 0.01  # btc是0.01，不同的币种要进行不同的替换
c_rate = 5 / 10000  # 手续费，commission fees，默认为万分之5。不同市场手续费的收取方法不同，对结果有影响。比如和股票就不一样。
slippage = 1 / 1000  # 滑点 ，可以用百分比，也可以用固定值。建议币圈用百分比，股票用固定值
leverage_rate = 3
min_margin_ratio = 1 / 100  # 最低保证金率，低于就会爆仓
rule_type = '5T'
drop_days = 10  # 币种刚刚上线10天内不交易

# =====读入数据
df = pd.read_hdf('./data/%s.h5' % symbol, key='df')
# 任何原始数据读入都进行一下排序、去重，以防万一
df.sort_values(by=['candle_begin_time'], inplace=True)
df.drop_duplicates(subset=['candle_begin_time'], inplace=True)
df.reset_index(inplace=True, drop=True)

# =====转换为其他分钟数据
# rule_type = '15T'
# period_df = df.resample(rule=rule_type, on='candle_begin_time', label='left', closed='left').agg(
#     {'open': 'first',
#      'high': 'max',
#      'low': 'min',
#      'close': 'last',
#      'volume': 'sum',
#      # 'quote_volume': 'sum',
#      })
# period_df.dropna(subset=['open'], inplace=True)  # 去除一天都没有交易的周期
# period_df = period_df[period_df['volume'] > 0]  # 去除成交量为0的交易周期
# period_df.reset_index(inplace=True)
# df = period_df[['candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'quote_volume']]
# df = period_df[['candle_begin_time', 'open', 'high', 'low', 'close', 'volume']]
df = df[df['candle_begin_time'] >= pd.to_datetime('2017-01-01')]
df.reset_index(inplace=True, drop=True)

# 共享的计数器
manager = Manager()
total_counter = manager.Value('i', 0)


def calculate_by_one_loop(para):
    global total_counter
    total_counter.value += 1

    _df = df.copy()
    # =====计算交易信号
    _df = signal_simple_bolling(_df, para=para)

    # -----------------------------

    # =====计算实际持仓
    _df = position_for_OKEx_future(_df)
    # 计算资金曲线

    # =====计算资金曲线
    # 选取相关时间。币种上线10天之后的日期
    t = _df.iloc[0]['candle_begin_time'] + timedelta(days=drop_days)
    _df = _df[_df['candle_begin_time'] > t]
    _df = equity_curve_for_OKEx_USDT_future_next_open(_df, slippage=slippage, c_rate=c_rate,
                                                      leverage_rate=leverage_rate,
                                                      face_value=face_value, min_margin_ratio=min_margin_ratio)
    # 计算收益
    rtn = pd.DataFrame()
    rtn.loc[0, 'para'] = str(para)
    r = _df.iloc[-1]['equity_curve']
    rtn.loc[0, 'equity_curve'] = r
    print(para, '策略最终收益：', r, '参数总数：', sum, '当前完成：', total_counter.value)
    return rtn


# =====获取策略参数组合
para_list = signal_simple_bolling_para_list()
# para_list = [[230, 3.8, 0.008], [190, 3.0, 0.043], [160, 2.6, 0.03], [30, 3.4, 0.005], [20, 4.9, 0.011],
#              [20, 4.9, 0.01], [30, 3.3, 0.05], [40, 2.3, 0.031], [70, 3.2, 0.007], [20, 3.4, 0.017]]
sum = len(para_list)
num = 0
# # =====并行提速
start_time = datetime.now()  # 标记开始时间


# with Pool(processes=8) as pool:  # or whatever your hardware can support
#     # 使用并行批量获得data frame的一个列表
#     df_list = pool.map(calculate_by_one_loop, para_list)
#     print('读入完成, 开始合并', datetime.now() - start_time)
#     # 合并为一个大的DataFrame
#     para_curve_df = pd.concat(df_list, ignore_index=True)
#
# # =====输出
# para_curve_df.sort_values(by='equity_curve', ascending=False, inplace=True)
# print(para_curve_df)
# para_curve_df.to_csv('./data/%s_equity.csv' % symbol, index=False)


def split_list(lst, num=10):
    length = len(lst)
    size = length // num
    return [lst[i * size:(i + 1) * size] for i in range(num)]


split_param_list = split_list(para_list)
for index, _param_list in enumerate(split_param_list):
    with Pool(processes=10) as pool:  # or whatever your hardware can support
        # 使用并行批量获得data frame的一个列表
        df_list = pool.map(calculate_by_one_loop, _param_list)
        print('读入完成, 开始合并', datetime.now() - start_time)
        # 合并为一个大的DataFrame
        para_curve_df = pd.concat(df_list, ignore_index=True)

    # =====输出
    para_curve_df.sort_values(by='equity_curve', ascending=False, inplace=True)
    print(para_curve_df)
    para_curve_df.to_csv('./data/{}_{}_equity.csv'.format(index, symbol), index=False)

path_list = glob.glob("./data/*_{}_equity.csv".format(symbol))  # python自带的库，获得某文件夹中所有csv文件的路径
equity_list = []
for path in path_list:
    print(path)
    _equity_list = pd.read_csv(path, encoding="GBK")
    equity_list.append(_equity_list)
# print(equity_list)
# exit()
data = pd.concat(equity_list, ignore_index=True)
data['equity_curve'] = data['equity_curve'].round(4)
data.sort_values(by='equity_curve', ascending=False, inplace=True)
data.to_csv('./data/{}_equity.csv'.format(symbol), index=False)
