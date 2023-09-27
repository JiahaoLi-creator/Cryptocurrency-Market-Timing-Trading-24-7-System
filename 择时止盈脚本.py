import os
import sys
_ = os.path.abspath(os.path.dirname(__file__))  # 返回当前文件路径
_ = os.path.abspath(os.path.join(_, '..'))  # 返回根目录文件夹
sys.path.append(_)  # _ 表示上级绝对目录，系统中添加上级目录
sys.path.append('..')  # '..' 表示上级相对目录，系统中添加上级目录
import ccxt
import time
import traceback
import pandas as pd
import warnings
warnings.filterwarnings('ignore')
pd.set_option('display.max_rows', 1000)
pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option('display.unicode.ambiguous_as_wide', True)  # 设置命令行输出时的列对齐功能
pd.set_option('display.unicode.east_asian_width', True)
from api.market import load_market, fetch_binance_ticker_data
from api.trade import place_order, get_twap_symbol_info_list
from config import max_one_order_amount, twap_interval, account_config, proxy
from utils.functions import update_all_account_info
from utils.notifications import send_wechat_work_msg


# ===监控间隔(单位: 秒)
monitor_time = 90  # 10：表示每10秒监测一次

# ===止盈配置
stop_profit_config = {
    'MinPrice_MA': {  # 与account_config账户配置的账号名称保持一致
        'stop_profit': 500,  # 500: 表示账户总净值(包含未实现盈亏)超过500阈值，触发止盈操作
        'stop_profit_rate': 0.5,  # 0.5: 表示止盈操作，需要平掉当前50%的仓位
    },
    # 'Alpha95_Bolling': {  # 与account_config账户配置的账号名称保持一致
    #     'stop_profit': 6666,
    #     'stop_profit_rate': 0.2,
    # },
}


def run():
    # =====加载所有交易对的信息：下单精度、最小下单量等...
    exchange = ccxt.binance({'proxies': proxy})  # 创建交易所对象
    symbol_list, min_qty, price_precision, min_notional = load_market(exchange)

    while True:
        # =====更新所有账户的净值和持仓币种
        account_info = update_all_account_info(account_config)
        account_list = list(account_info.keys())  # 获取账户列表
        # 判断是否没能成功读取任一账户
        if not len(account_list):  # 如果account_info数据为空，表示更新账户信息失败
            print('所有账户信息列表为空，账号全部止盈或更新信息失败')
            time.sleep(monitor_time)
            continue

        # =====遍历每个账户，进行下单
        for account in account_list:
            # ===获取账号配置
            # =获取当前账户配置
            position_df = account_info[account]['position_df']  # 获取当前账户的当前仓位
            equity = account_info[account]['equity']  # 获取当前账户的净值（不含未实现盈亏）
            current_exchange = account_info[account]['exchange']  # 获取当前账户的交易所对象
            # =获取止盈配置
            stop_profit = stop_profit_config[account]['stop_profit']  # 获取当前账户的止盈阈值
            stop_profit_rate = stop_profit_config[account]['stop_profit_rate']  # 获取当前账户的平仓比例

            # ===检查止盈配置
            if stop_profit_rate >= 1:  # 止盈不支持超过100%平仓，会影响策略正常下单
                print('止盈比例配置错误，不支持 1 以上的参数配置，会影响策略正常下单')
                exit()

            # ===判断当前是否有仓位
            if position_df.empty:  # 当前没有仓位直接跳过
                print(f'账户: {account}，当前没有持仓')
                continue

            # ===判断是否需要止盈
            total_equity = equity + position_df['持仓盈亏'].sum()  # 计算当前账户总净值（含未实现盈亏）
            print(f'账户: {account}，当前账户资金: {total_equity}，止盈金额: {stop_profit}')

            # 判断当前账户净值是否达到止盈阈值
            if total_equity < stop_profit:  # 若当前账户净没有达到止盈阈值，打印信息，并跳过当前账号
                continue

            # ===计算下单信息
            position_df['实际下单量'] = position_df['当前持仓量'] * stop_profit_rate * -1  # 直接通过当前持仓进行计算，方向要去反
            position_df['实际下单资金'] = position_df['实际下单量'] * position_df['当前标记价格']  # 计算实际下单资金，用于后续拆单
            position_df['交易模式'] = '减仓'  # 设置交易模式
            position_df.rename_axis('index', inplace=True)  # 对index进行重命名
            position_df = position_df[abs(position_df['实际下单量']) > 0]  # 保留实际下单量 > 0 的数据
            print('下单信息：\n', position_df)

            # 判断是否需要有下单信息
            if position_df.empty:
                continue

            # ===使用twap算法拆分订单
            twap_symbol_info_list = get_twap_symbol_info_list(position_df, max_one_order_amount)

            # ===遍历下单
            try:
                for i in range(len(twap_symbol_info_list)):
                    # 获取币种的最新价格
                    symbol_last_price = fetch_binance_ticker_data(current_exchange)
                    # 逐批下单
                    place_order(current_exchange, twap_symbol_info_list[i], symbol_last_price, min_qty, price_precision, min_notional)
                    # 下单间隔
                    print(f'等待 {twap_interval}s 后继续下单')
                    time.sleep(twap_interval)
            except BaseException as e:
                msg = f'账户: {account}，出现问题，下单失败'
                print(msg)
                send_wechat_work_msg(msg)
                print(traceback.format_exc())
                continue

            # ===止盈成功之后，移除当前账号
            del account_info[account]

            # ===发送信息推送
            send_wechat_work_msg(f'账户: {account}, 成功减仓完毕')

            # ===停顿一下，防止权重超标
            time.sleep(3)

        # 本次循环结束
        print('-' * 20, '本次监测结束，%f秒后进入下一次监测' % monitor_time, '-' * 20)
        print('\n')
        time.sleep(monitor_time)


if __name__ == '__main__':
    while True:
        try:
            run()
        except KeyboardInterrupt:  # 手动终止程序的错误，直接打印'退出'，并退出程序
            print('退出')
            exit()
        except Exception as err:
            msg = '择时止盈脚本出错，10s之后重新运行，出错原因: ' + str(err)
            print(msg)
            print(traceback.format_exc())
            send_wechat_work_msg(msg)
            time.sleep(10)
