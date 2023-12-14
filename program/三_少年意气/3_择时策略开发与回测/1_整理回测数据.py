"""
《邢不行-2020新版|Python数字货币量化投资课程》
无需编程基础，助教答疑服务，专属策略网站，一旦加入，永续更新。
课程详细介绍：https://quantclass.cn/crypto/class
邢不行微信: xbx9025
本程序作者: 邢不行

# 课程内容
介绍如何批量导入一个文件夹中的所有数据
"""
import pandas as pd
import glob
# import pytables
pd.set_option('expand_frame_repr', False)  # 当列太多时不换行


# 获取数据的路径
path = '/Users/caoliang/PycharmProjects/xbx_part_2/data/history_candle_data/binance/spot'  # 改成电脑本地的地址
path_list = glob.glob(path + "/*/*.csv")  # python自带的库，获得某文件夹中所有csv文件的路径

# 筛选出指定币种和指定时间
symbolList =[ 'BTC-USDT_5m']
# symbolList =[ 'SOL-USDT_5m','BTC-USDT_5m','ETH-USDT_5m','AVAX-USDT_5m']
# symbol =
# symbol =
# symbol =

for symbol in symbolList:
    tempList = list(filter(lambda x: symbol in x, path_list))

    # 导入数据
    df_list = []
    for path in sorted(tempList):
        print(path)
        # df = pd.read_csv(
        #     # 该参数为数据在电脑中的路径，
        #     # 要注意字符串转义符号 \ ，可以使用加r变为raw string或者每一个进行\\转义
        #     filepath_or_buffer=path,
        #     # 编码格式，不同的文件有不同的编码方式，一般文件中有中文的，编码是gbk，默认是utf8
        #     # ** 大家不用去特意记住很多编码，我们常用的就是gbk和utf8，切换一下看一下程序不报错就好了
        #     encoding='gbk',
        #     # 该参数代表数据的分隔符，csv文件默认是逗号。其他常见的是'\t'
        #     sep=',',
        #     # 该参数代表跳过数据文件的的第1行不读入
        #     skiprows=1,
        #     # nrows，只读取前n行数据，若不指定，读入全部的数据。一般在初步查看数据时使用
        #     nrows=15,
        #     # 将指定列的数据识别为日期格式。若不指定，时间数据将会以字符串形式读入。一开始先不用。
        #     # parse_dates=['candle_begin_time'],
        #     # 将指定列设置为index。若不指定，index默认为0, 1, 2, 3, 4...
        #     # index_col=['candle_begin_time'],
        #     # 读取指定的这几列数据，其他数据不读取。若不指定，读入全部列
        #     # usecols=['candle_begin_time', 'close'],
        #     # 当某行数据有问题时，报错。设定为False时即不报错，直接跳过该行。当数据比较脏乱的时候用这个。
        #     # error_bad_lines=False,
        #     # 将数据中的null识别为空值
        #     # na_values='NULL',
        #
        #     # 更多其他参数，请直接在搜索引擎搜索"pandas read_csv"，要去逐个查看一下。比较重要的，header等
        # )
        # print(df)
        # exit()
        df = pd.read_csv(path,  encoding="GBK", parse_dates=['candle_begin_time'])
        df = df[['candle_begin_time', 'open', 'high', 'low', 'close', 'volume']]
        # df = df[['candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'quote_volume']]
        df_list.append(df)
        print(df.head(5))

    # 整理完整数据
    print(df_list)
    data = pd.concat(df_list, ignore_index=True)
    data.sort_values(by='candle_begin_time', inplace=False)
    data.reset_index(drop=False, inplace=False)

    # 导出完整数据
    data.to_hdf('/Users/caoliang/PycharmProjects/xbx_part_3/data/%s.h5' % symbol, key='df', mode='w')
    print(data)

