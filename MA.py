def signal(df, para=[20, 40]):
    # ===获取指标的计算参数
    ma_short = para[0]  # 短期均线
    ma_long = para[1]  # 长期均线

    # ===计算指标
    df['ma_short'] = df['close'].rolling(1300, min_periods=2).apply(lambda x: x.ewm(span=ma_short, adjust=False).mean().to_list()[-1])
    df['ma_long'] = df['close'].rolling(1300, min_periods=2).apply(lambda x: x.ewm(span=ma_long, adjust=False).mean().to_list()[-1])

    # ===找出做多信号
    condition1 = df['ma_short'] > df['ma_long']  # 短期均线 > 长期均线
    condition2 = df['ma_short'].shift(1) <= df['ma_long'].shift(1)  # 上一周期的短期均线 <= 长期均线
    df.loc[condition1 & condition2, 'signal_long'] = 1  # 将产生做多信号的那根K线的signal设置为1，1代表做多

    # ===找出做空信号
    condition1 = df['ma_short'] < df['ma_long']  # 短期均线 < 长期均线
    condition2 = df['ma_short'].shift(1) >= df['ma_long'].shift(1)  # 上一周期的短期均线 >= 长期均线
    df.loc[condition1 & condition2, 'signal_short'] = -1  # 将产生平仓信号当天的signal设置为0，0代表平仓

    # ===合并做多做空信号
    df['signal'] = df[['signal_long', 'signal_short']].sum(axis=1, min_count=1, skipna=True)

    # 配置需要保存的因子列
    factor_columns = ['ma_short', 'ma_long']
    # ===信号去重
    temp = df[df['signal'].notnull()][['signal']]
    temp = temp[temp['signal'] != temp['signal'].shift(1)]
    df['signal'] = temp['signal']

    return df, factor_columns
