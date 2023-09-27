import os
import sys
_ = os.path.abspath(os.path.dirname(__file__))  # 返回当前文件路径
_ = os.path.abspath(os.path.join(_, '..'))  # 返回根目录文件夹
sys.path.append(_)  # _ 表示上级绝对目录，系统中添加上级目录，可以解决导入config不存的问题
sys.path.append('..')  # '..' 表示上级相对目录，系统中添加上级目录，可以解决导入config不存的问题
import ccxt
import time
import traceback
import numpy as np
import pandas as pd
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')
pd.set_option('display.max_rows', 1000)
pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option('display.unicode.ambiguous_as_wide', True)  # 设置命令行输出时的列对齐功能
pd.set_option('display.unicode.east_asian_width', True)
from api.market import load_market, fetch_binance_ticker_data, reset_leverage, check_position_side, check_multi_assets_margin
from api.trade import place_order, get_twap_symbol_info_list, simple_order
from config import utc_offset, black_list, leverage, max_one_order_amount, twap_interval, account_config, index_config, data_path, flag_path, max_time, proxy, is_ahead
from utils.functions import cal_order_amount, get_current_offset, calc_target_amount, cal_signal, import_index_data, import_select_coin, update_all_account_info, save_index_equity, send_img_for_signal
from utils.notifications import send_msg_for_position, send_wechat_work_msg
from utils.commons import sleep_until_run_time, remedy_until_run_time


def run():
    while True:
        # =====加载所有交易对的信息：下单精度、最小下单量等...
        exchange = ccxt.binance({'proxies': proxy})  # 创建交易所对象
        symbol_list, min_qty, price_precision, min_notional = load_market(exchange, black_list)

        # =====更新所有账户的净值和持仓币种
        # 函数注意点：如果其中一个账户出现问题，del这个账户避免影响其他账户操作；这里将账户配置拷贝一份，失败的账户可以在下个小时再重新尝试一下
        account_info = update_all_account_info(account_config.copy())
        # 判断是否没能成功读取任一账户
        if not len(account_info.keys()):  # 如果account_info数据为空，表示更新账户信息失败
            print('所有账户更新信息失败')
            continue

        # =====企业微信发送账户净值和持仓信息
        send_msg_for_position(account_info)

        # =====企业微信发送指标的信息
        send_img_for_signal(account_info, data_path)

        # =====sleep直到下一个整点小时
        random_time = max_time if is_ahead else 0  # 提前时间比index要快一点，这样可以在文件生成之后，立马下单
        run_time = sleep_until_run_time('1h', if_sleep=True, cheat_seconds=random_time)
        # run_time = datetime.strptime('2023-08-10 11:00:00', "%Y-%m-%d %H:%M:%S")  # 以下代码可以测试的时候使用

        # =====判断指数的本地文件有没有更新成功。
        index_file_path = os.path.join(flag_path, f"{run_time.strftime('%Y-%m-%d_%H')}.flag")  # 构建本地flag文件地址
        count = 0  # 计数器，用来统计检测文件存在的次数
        while True:
            # 判断该flag文件是否存在
            if os.path.exists(index_file_path):
                count += 1  # 计数器+1：表示检测到文件存在次数+1。
                if count >= 2:  # 若flag文件存在，并且检测到2次以上文件存在时，表示指数已经更新成功，可以进行后续的操作
                    break

        # =====遍历每个账户，进行下单
        for account in account_info:
            # ===获取与当前账号相关的配置信息
            # =获取指数相关信息
            index_name = account_info[account]['index']  # 获取指数名称例如：index_name是"低价币指数"，详情可参考config中的account_config中的index配置
            index_df = import_index_data(os.path.join(data_path, f'{index_name}.csv'))  # 从本地指数文件中读取数据，并且去重
            select_coin = import_select_coin(os.path.join(data_path, f'{index_name}_select_coin.csv'))  # 从本地指数文件中读取数据

            # =获取当前账户配置
            strategy = account_info[account]['strategy']  # 获取当前账户的策略配置，详情可参考config中的account_config中的strategy配置
            position_df = account_info[account]['position_df']  # 获取当前账户的当前仓位
            equity = account_info[account]['equity']  # 获取当前账户的净值（不含未实现盈亏），例如：equity是'99999'
            current_exchange = account_info[account]['exchange']  # 获取当前账户的交易所对象

            # ===计算交易信号
            # 计算择时信号s ignal以及当前持仓now_pos，期间可能会读取开仓时指数的净值，用来判断是否止损。
            signal, now_pos = cal_signal(index_df, strategy, position_df, data_path, account)
            # signal = 1  # 测试的时候可以设置为 1(做多) 0(平仓) -1(做空) None(无信号)
            print(f'账户: {account}, 当前择时交易信号: ', signal)

            # 判断是否需要下单
            cond1 = signal is None or np.isnan(signal)  # 满足没有择时信号
            cond2 = run_time.hour == utc_offset % 24  # 满足当前时间是utc0点（utc0点对应国内时间是北京时间早上8点）
            cond3 = index_config[index_name]['offset'] == get_current_offset(run_time, utc_offset, index_config[index_name])  # 满足当前offset等于当前指数的offset
            is_change_index = cond2 and cond3  # 是否是成分币换仓
            # 判断是否需要跳过后续下单操作
            if cond1 and not is_change_index:  # 当前没有择时信号 并且 不是指数成分换仓时间，跳过下单；如果存在交易信号，就进行后续的下单操作
                continue

            # ===计算当前目标持仓
            # =设置开仓的方向
            # 如果当前需要开仓
            open_pos = signal  # 设置开仓方向为择时信号
            # 如果当前需要更换指数成分
            if cond1:  # 如果择时交易信号不存在，则表示当前是指数成分换仓才需要去下单，因此开仓信号直接保持与原持仓方向一致即可
                open_pos = now_pos
            # 赋值开仓方向
            select_coin['方向'] = open_pos

            # =计算目标持仓：根据最新一期的选币，并且计算其当前比例
            target_amount_info = calc_target_amount(current_exchange, select_coin, equity, leverage, now_pos, has_signal=not cond1, is_change_index=is_change_index)
            print('目标持仓信息：\n', target_amount_info)

            # ===开始计算具体下单信息
            symbol_order = cal_order_amount(current_exchange, position_df, target_amount_info)
            print('下单信息：\n', symbol_order)
            if symbol_order.empty:
                continue

            # ===使用twap算法拆分订单
            twap_symbol_info_list = get_twap_symbol_info_list(symbol_order, max_one_order_amount)

            # ===遍历下单
            try:
                error_order_list = []
                for i in range(len(twap_symbol_info_list)):
                    # 获取币种的最新价格
                    symbol_last_price = fetch_binance_ticker_data(current_exchange)
                    # 逐批下单
                    error_orders = place_order(current_exchange, twap_symbol_info_list[i], symbol_last_price, min_qty, price_precision, min_notional)
                    error_order_list.extend(error_orders)  # 添加失败的订单
                    # 下单间隔
                    print(f'等待 {twap_interval}s 后继续下单')
                    time.sleep(twap_interval)
                account_info[account]['error_orders'] = error_order_list
            except BaseException as e:
                msg = f'账户: {account}，出现问题，下单失败'
                print(msg)
                send_wechat_work_msg(msg)
                print(traceback.format_exc())
                continue

            # ===更新下单时的指数净值
            if not cond1:  # 如果当前存在择时信号，更新本地指数净值数据，用于计算止损
                save_index_equity(open_pos, run_time, data_path, account, index_df)

            # ===发送交易信号
            send_wechat_work_msg(f'账户: {account}, 当前择时交易信号: {signal}, 是否成分股换仓【{is_change_index}】')

            # ===停顿一下，防止权重超标
            time.sleep(3)

        # =====遍历每个账户，容错补单：对失败的订单进行重试
        now = datetime.now()  # 获取当前时间
        # 重试补单30分钟
        while now > datetime.now() - pd.to_timedelta('30min'):
            _temp_error_orders = []  # 临时存储一下所有账号的失败订单，用于判断下次是否需要继续补单操作
            # ===遍历账号进行补单
            for account in account_info:
                # =检查是否有错误订单信息字段
                if 'error_orders' not in account_info[account]:  # 当前账号不存在错误订单信息，跳过
                    continue

                # =获取一下需要补单的信息
                account_error_orders = account_info[account]['error_orders']  # 获取当前账号需要补单的信息
                _temp_error_orders.extend(account_error_orders)  # 追加当前账号的失败订单

                error_orders = []  # 定义一个空的补单数据
                # =遍历错误的订单信息，进行补单
                for order_info in account_error_orders:
                    # 进行单币下单(容错次数：5)
                    simple_order_info = simple_order(exchange, order_info, price_precision)
                    # 判断是否需要添加失败订单列表
                    if simple_order_info:  # 若果存在信息返回，则表示5次单币下单都失败了
                        error_orders.append(simple_order_info)

                # 更新失败的订单信息
                account_info[account]['error_orders'] = error_orders

            # ===所有失败订单都成功之后，退出循环
            if not _temp_error_orders:
                break

            # ===停顿一下，防止权重超标
            time.sleep(5)

        # =====清理数据
        del exchange, symbol_list, min_qty, price_precision, min_notional, account_info
        # 本次循环结束
        print('-' * 20, '本次循环结束，%f秒后进入下一次循环' % 20, '-' * 20)
        print('\n')
        time.sleep(20)

        # 补偿当前时间与run_time之间的差值
        remedy_until_run_time(run_time)


if __name__ == '__main__':
    # =====遍历初始化账户设置
    for _account in account_config:
        # ===设置一下页面最大杠杆
        reset_leverage(account_config[_account]['exchange'], 3)  # 3倍杠杆
        # ===检查并且设置持仓模式：单向持仓
        check_position_side(account_config[_account]['exchange'])
        # ===检查联合保证金模式
        check_multi_assets_margin(account_config[_account]['exchange'])
        # ===停顿一下，防止权重超标
        time.sleep(5)

    while True:
        try:
            run()
        except KeyboardInterrupt:  # 手动终止程序的错误，直接打印'退出'，并退出程序
            print('退出')
            exit()
        except Exception as err:
            msg = '系统出错，10s之后重新运行，出错原因: ' + str(err)
            print(msg)
            print(traceback.format_exc())
            send_wechat_work_msg(msg)
            time.sleep(10)
