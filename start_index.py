import os
import sys
_ = os.path.abspath(os.path.dirname(__file__))  # 返回当前文件路径
_ = os.path.abspath(os.path.join(_, '..'))  # 返回根目录文件夹
sys.path.append(_)  # _ 表示上级绝对目录，系统中添加上级目录，可以解决导入config不存的问题
sys.path.append('..')  # '..' 表示上级相对目录，系统中添加上级目录，可以解决导入config不存的问题
import ccxt
import time
import random
import traceback
from datetime import datetime
import pandas as pd
import warnings
warnings.filterwarnings('ignore')
pd.set_option('display.max_rows', 1000)
pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option('display.unicode.ambiguous_as_wide', True)  # 设置命令行输出时的列对齐功能
pd.set_option('display.unicode.east_asian_width', True)
from api.market import fetch_all_binance_swap_candle_data, load_market
from utils.functions import save_data, judge_first_run_and_adjust_index, create_finish_flag, get_coin_list_for_select_coin
from utils.index_functions import cal_index, cal_factor_and_select_coin
from utils.notifications import send_wechat_work_msg
from utils.commons import sleep_until_run_time, remedy_until_run_time
from config import black_list, get_kline_num, index_config, min_kline_size, data_path, flag_path, utc_offset, min_time, max_time, proxy, is_ahead


def run():
    while True:
        # =====配置exchange，用于获取交易对数据和k线数据
        exchange = ccxt.binance({'proxies': proxy})

        # =====加载所有交易对的信息：交易对的最小下单量、最小下单金额
        symbol_list, min_qty, price_precision, min_notional = load_market(exchange, black_list)
        # symbol_list = symbol_list[:20]  # 测试的时候可以减少获取币种数量，加快请求速度

        # =====sleep直到下一个整点小时
        random_time = random.randint(min_time, max_time) if is_ahead else 0  # 指数生成随机提前3-7分钟。不建议使用负数
        run_time = sleep_until_run_time('1h', if_sleep=True, cheat_seconds=random_time)
        # run_time = datetime.strptime('2023-08-24 00:00:00', "%Y-%m-%d %H:%M:%S")  # 测试代码，测试的时候可以使用

        # =====判断是否首次运行、是否指数换仓；确定要获取数据的币种
        # 判断是否首次运行、是否指数换仓
        is_first_run, is_adjust_index = judge_first_run_and_adjust_index(index_config, run_time, data_path, utc_offset)
        is_update_all = is_first_run or is_adjust_index  # 设置全部更新标识。首次运行或者指数需要换仓，都是需要全部更新数据的

        # 确定要获取数据的币种
        if is_update_all:  # 如果需要全部更新指数和选币，需要获取全量的symbol_list
            coin_list = symbol_list
        else:  # 如果不需要全部更新指数和选币，那么就直接获取指数的成分币种即可，减少请求，加快速度
            # 这里会获取最近3个周期的选币数据，理论上只要一个，为了做一点冗余
            coin_list = get_coin_list_for_select_coin(index_config, data_path)  # 此处同时会更新index_config

        # =====获取指定币种的1小时K线
        limit = get_kline_num if is_first_run else 399  # 首次运行k线数量设置为1500根。399: 两个持仓周期时间k线24*7*2=336，多取一点设置为399
        s_time = datetime.now()
        # 更新coin_list中所有币种的1小时K线数据
        symbol_candle_data = fetch_all_binance_swap_candle_data(exchange, coin_list, run_time, limit)  # 串行获取
        print('完成获取K线，花费时间：', datetime.now() - s_time)

        # =====根据获取的数据，进行选币
        if is_update_all:  # 需要全部更新的时候，才会进行数据整理选币
            s_time = datetime.now()
            # 整理最近limit根K线，每个指数的选币结果。
            index_data = cal_factor_and_select_coin(symbol_candle_data, index_config, min_kline_size)
            print('完成选币数据整理 & 选币，花费时间：', datetime.now() - s_time)
        else:  # 不需要全部更新的时候，其他情况直接copy我们之前读到的数据
            index_data = index_config.copy()

        # =====遍历构建指数
        s_time = datetime.now()
        for _index in index_data.keys():
            # ===根据每个周期的选币结果，计算指数，并保存。在计算指数的时候，会去除最先的1个周期。
            index_df = cal_index(symbol_candle_data, index_data[_index])  # 根据指数的成分币信息，计算指数
            index_file_path = os.path.join(data_path, f'{_index}.csv')  # 构建保存指数文件路径
            save_data(index_df, index_file_path)  # 保存当前计算的所有指数文件。如果本地不存在择全部存，存在的话append最近5行。
            print('保存指数成功：', _index)

            # ===保存选币数据
            # 判断是否需要保存选币数据
            if is_update_all:  # 全部更新的时候，才会更新选币数据
                select_coin = index_data[_index]['select_coin']  # 需要保存的选币结果
                select_coin_file_path = os.path.join(data_path, f'{_index}_select_coin.csv')  # 构建保存选币文件路径
                save_data(select_coin, select_coin_file_path, keep_new=True)  # 保存最新的选币结果。如果本地不存在择全部存。

        print('完成构建指数，花费时间：', datetime.now() - s_time)

        # =====生成指数完成标识文件。如果标记文件过多，会删除7天之前的数据
        create_finish_flag(flag_path, run_time)

        # =====本次循环结束
        print('-' * 20, '本次循环结束，%f秒后进入下一次循环' % 20, '-' * 20)
        print('\n')
        time.sleep(20)

        # 补偿当前时间与run_time之间的差值
        remedy_until_run_time(run_time)


if __name__ == '__main__':
    while True:
        try:
            run()
        except KeyboardInterrupt:
            print('退出')
            exit()
        except Exception as err:
            msg = '系统出错，10s之后重新运行，出错原因: ' + str(err)
            print(msg)
            print(traceback.format_exc())
            send_wechat_work_msg(msg)
            time.sleep(10)
