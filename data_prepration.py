import pandas as pd 
import numpy as np 
import matplotlib.pyplot as plt 
import datetime
from pybit import usdt_perpetual
import talib as ta
import yfinance as yf 

def will_frac(df: pd.DataFrame, period: int = 2): #-> Tuple[pd.Series, pd.Series]:
    """Indicate bearish and bullish fractal patterns using shifted Series.

    :param df: OHLC data
    :param period: number of lower (or higher) points on each side of a high (or low)
    :return: tuple of boolean Series (bearish, bullish) where True marks a fractal pattern
    """

    periods = [p for p in range(-period, period + 1) if p != 0] # default [-2, -1, 1, 2]

    highs = [df['high'] > df['high'].shift(p) for p in periods]
    bears = pd.Series(np.logical_and.reduce(highs), index=df.index)

    lows = [df['low'] < df['low'].shift(p) for p in periods]
    bulls = pd.Series(np.logical_and.reduce(lows), index=df.index)

    return bears, bulls

def adx(df, len):

    df['dist1'] = df['high'] - df['low']
    df['dist2'] = abs(df['high'] - df['close'].shift(1))
    df['dist3'] = abs(df['low'] - df['close'].shift(1))

    df.dropna(inplace = True)
    df.reset_index(inplace = True, drop = True)

    df['dist1dist2'] = df[["dist1", "dist2"]].max(axis=1)
    df['truerange'] = df[["dist1dist2", "dist3"]].max(axis=1)
    df.drop(columns=['dist1', 'dist2', 'dist3', 'dist1dist2'], inplace = True)

    df['smooth_truerange'] = 0
    len = 14
    for i in range(1,df.shape[0]):    
        df['smooth_truerange'].iloc[i] =  df['smooth_truerange'].iloc[i-1] - (df['smooth_truerange'].iloc[i-1]/len) + df['truerange'].iloc[i]

    df['k'] = (df['high'] - df['high'].shift(1))
    df['j'] = (df['low'].shift(1) - df['low'])
    df['DirectionalMovementPlus'] = np.where(df['k'] > df['j'], df['high'] - df['high'].shift(1), 0)
    df['DirectionalMovementPlus'] = np.where(df['DirectionalMovementPlus'] > 0, df['DirectionalMovementPlus'], 0)
    df['smooth_DirectionalMovementPlus'] = 0
    len = 14
    for i in range(1,df.shape[0]):    
        df['smooth_DirectionalMovementPlus'].iloc[i] =  df['smooth_DirectionalMovementPlus'].iloc[i-1] - (df['smooth_DirectionalMovementPlus'].iloc[i-1]/len) + df['DirectionalMovementPlus'].iloc[i]
    df.drop(columns=['DirectionalMovementPlus', 'k', 'j'], inplace = True)

    df['k'] = (df['low'].shift(1) - df['low'])
    df['j'] = (df['high'] - df['high'].shift(1))
    df['DirectionalMovementMinus'] = np.where(df['k'] > df['j'], df['low'].shift(1) - df['low'], 0)
    df['DirectionalMovementMinus'] = np.where(df['DirectionalMovementMinus'] > 0, df['DirectionalMovementMinus'], 0)
    df['smooth_DirectionalMovementMinus'] = 0
    len = 14
    for i in range(1,df.shape[0]):    
        df['smooth_DirectionalMovementMinus'].iloc[i] =  df['smooth_DirectionalMovementMinus'].iloc[i-1] - (df['smooth_DirectionalMovementMinus'].iloc[i-1]/len) + df['DirectionalMovementMinus'].iloc[i]
    df.drop(columns=['DirectionalMovementMinus', 'k', 'j'], inplace = True)

    df['DIPlus']  = (df['smooth_DirectionalMovementPlus'] / df['smooth_truerange']) * 100
    df['DIMinus'] = (df['smooth_DirectionalMovementMinus'] / df['smooth_truerange']) * 100
    df['DX'] = (abs(df['DIPlus'] - df['DIMinus']) / (df['DIPlus'] + df['DIMinus'])) * 100
    df['ADX'] = ta.SMA(df['DX'], timeperiod = len)
    df.dropna(inplace=True)
    df.reset_index(inplace=True, drop=True)

    df.drop(columns=['truerange', 'smooth_truerange', 'smooth_DirectionalMovementPlus', 'smooth_DirectionalMovementMinus', 'DIPlus', 'DIMinus', 'DX'], inplace = True)

    return df

def ichi(df, tenkan_period = 20, kijun_period = 60, senkou_period = 120, shift_period = 29):
    #Tenkan-sen (Conversion Line): (9-period high + 9-period low)/2))
    nine_period_high  = df['high'].rolling(window= tenkan_period).max()
    nine_period_low   = df['low'].rolling(window= tenkan_period).min()
    df['tenkan_sen']  = (nine_period_high + nine_period_low) /2

    #Kijun-sen (Base Line): (26-period high + 26-period low)/2))
    period26_high   = df['high'].rolling(window=kijun_period).max()
    period26_low    = df['low'].rolling(window=kijun_period).min()
    df['kijun_sen'] = (period26_high + period26_low) / 2

    #Senkou Span A (Leading Span A): (Conversion Line + Base Line)/2))
    df['senkou_span_a'] = ((df['tenkan_sen'] + df['kijun_sen']) / 2).shift(shift_period)

    df['f_senkou_span_a'] = (df['tenkan_sen'] + df['kijun_sen'])/2

    #Senkou Span B (Leading Span B): (52-period high + 52-period low)/2))
    period52_high = df['high'].rolling(window=senkou_period).max()
    period52_low  = df['low'].rolling(window=senkou_period).min()
    df['senkou_span_b'] = ((period52_high + period52_low)/2).shift(shift_period)

    df['f_senkou_span_b'] = (period52_high + period52_low)/2

    #chikou
    df['chikou'] = df['close'].shift(shift_period)

    return df

def bolinger(df):
    df['upperband'], df['middleband'], df['lowerband'] = ta.BBANDS(df['close'], timeperiod=24, nbdevup=2, nbdevdn=2, matype=0)
    return df

def atr(df, n):
    df['atr'] = ta.ATR(df['high'], df['low'], df['close'], timeperiod=n)
    return df

def sp500(start, end):
    start = start
    end   = end
    start = datetime.datetime.fromtimestamp(start)
    end   = datetime.datetime.fromtimestamp(end)
    df    = yf.download('^GSPC', start = start, end = end, interval = '1d')
    # df_w  = yf.download('^GSPC', start = start, end = end, interval = '1wk')
    # print(df)
    # print(df_w)

    df.reset_index(inplace=True)
    # df_w.reset_index(inplace=True)
    df['Date'] = df[['Date']].apply(lambda x: x[0].timestamp(), axis=1).astype(int)
    # df_w['Date'] = df_w[['Date']].apply(lambda x: x[0].timestamp(), axis=1).astype(int)
    del df['Close'], df['Volume']
    # del df_w['Close'], df_w['Volume']
    df.columns = ['open_time', 'open', 'high', 'low', 'close']
    # df_w.columns = ['open_time', 'open', 'high', 'low', 'close']

    # df_w = ichi(df_w)
    df   = ichi(df)

    # print(df)
    # print(df_w)

    # future_kumo = pd.DataFrame()
    # future_kumo = df_w[['open_time', 'f_senkou_span_a', 'f_senkou_span_b']]

    df = df.drop(columns=['tenkan_sen', 'kijun_sen', 'senkou_span_a', 'senkou_span_b', 'chikou'])

    # final = pd.DataFrame()
    # final = pd.merge(df, future_kumo, on = 'open_time', how = 'left')
    # final.fillna(method = 'ffill', inplace = True)

    df.dropna(inplace = True)
    df.reset_index(inplace=True, drop=True)
    print(df)

    df.to_csv('/home/farzan/robot/ILT22_backtest/csv/sp500.csv', index = False, sep = ',')

def bitcoin(start, end):

    symbol    = 'BTCUSDT'
    limit     = 200
    df_d      = pd.DataFrame()
    session   = usdt_perpetual.HTTP(
                    endpoint   = 'https://api.bybit.com', 
                    api_key    = 'AlNAPQLUTywTCD70XV',
                    api_secret = '6qzDdbG1U4fVgpBwEaoyEFRPHJfZiM6U7PvE',)


    #daily candles
    for i in range(start, end, limit*86400): #86400
        dataframe1 = pd.DataFrame.from_records(session.query_kline(symbol = symbol, interval = 'D', limit = limit, from_time = i)['result'])
        df_d       = pd.concat([df_d, dataframe1], axis = 0, ignore_index = True)

    df_d = df_d.reset_index(drop = True)
    df_d = df_d.iloc[:,5:11]
    #df_d = ichi(df_d)
    # df_d = atr(df_d, 14)
    # df_d.drop('volume', axis = 1, inplace = True)
    #df_d = df_d.drop(columns=['volume','tenkan_sen', 'kijun_sen', 'senkou_span_a', 'senkou_span_b', 'chikou'])
    # df_d.columns = ['open_time', 'd_open', 'd_high', 'd_low', 'd_close', 'd_tenkan_sen', 'd_kijun_sen', 'd_senkou_span_a', 'd_f_senkou_span_a', 'd_senkou_span_b', 'd_f_senkou_span_b', 'd_chikou', 'd_atr']
    # df_d['open_time'] = df_d['open_time'] + 86400   #86400

    # future_kumo = pd.DataFrame()
    # future_kumo = df_d[['open_time', 'd_f_senkou_span_a', 'd_f_senkou_span_b']]
    # print(future_kumo)

    df_d.dropna(inplace = True)
    df_d.reset_index(inplace=True, drop=True)


    # final = pd.merge(df, future_kumo, on = 'open_time', how = 'left')
    # final.fillna(method = 'ffill', inplace=True)
    # final.dropna(inplace = True)
    # final.reset_index(inplace=True, drop=True)

    # final['open_time'] = final['open_time'] * 1000

    print(df_d)

    df_d.to_csv('/home/farzan/robot/ILT22_backtest/csv_html/bitcoin.csv', index=False, sep=',')

def data_generator(start, end, timeframe, symbol):

    if timeframe == 'h4':
        steps    = 3600*4
        interval = '240'

    if timeframe == 'h1':
        steps    = 3600
        interval = '60'

    symbol    = symbol
    limit     = 200
    df_d      = pd.DataFrame()
    session   = usdt_perpetual.HTTP(
                    endpoint   = 'https://api.bybit.com', 
                    api_key    = 'AlNAPQLUTywTCD70XV',
                    api_secret = '6qzDdbG1U4fVgpBwEaoyEFRPHJfZiM6U7PvE',)


    #daily candles
    for i in range(start, end, limit*steps):
        dataframe1 = pd.DataFrame.from_records(session.query_kline(symbol = symbol, interval = interval, limit = limit, from_time = i)['result'])
        df_d       = pd.concat([df_d, dataframe1], axis = 0, ignore_index = True)

    df_d = df_d.reset_index(drop = True)
    df_d = df_d.iloc[:,5:11]
    df_d['top'], df_d['bot'] = will_frac(df_d, period = 9)
    
    df_d['top'] = np.where(df_d['top'] == True, 1, 0)
    df_d['bot'] = np.where(df_d['bot'] == True, 1, 0)

    df_d = df_d.drop(columns=['volume'])

    df_d.dropna(inplace = True)
    df_d.reset_index(inplace=True, drop=True)

    print(df_d)

    df_d.to_csv(f'/home/farzan/robot/IMS22/csv_html/{symbol}_{timeframe}.csv', index=False, sep=',')

def euro_daily():

    df = yf.download('EURUSD=X', start = '2012-01-01', end = '2022-07-14')
    df.drop(columns=['Adj Close', 'Volume'], inplace = True)
    df = df.reset_index()
    df['Date'] = df[['Date']].apply(lambda x: x[0].timestamp(), axis=1).astype(int)
    df.columns = ['timestamp', 'open', 'high', 'low', 'close']
    df = ichi(df)
    df = adx(df, 14)
    df = df.drop(columns=['tenkan_sen', 'kijun_sen', 'senkou_span_a', 'senkou_span_b', 'chikou'])
    df.dropna(inplace = True)
    df.reset_index(inplace=True, drop=True)

    df.to_csv('/home/farzan/robot/Backtest/csv_html/euro_daily.csv', index=False, sep=',')

def euro_h4():

    df = pd.read_csv('/home/farzan/robot/ILT22_backtest/csv_html/eurousd.csv')
    df = ichi(df)
    df = adx(df, 14)
    df = df.drop(columns=['tenkan_sen', 'kijun_sen', 'senkou_span_a', 'senkou_span_b', 'chikou'])
    df.dropna(inplace = True)
    df.reset_index(inplace=True, drop=True)

    print(df)

    df.to_csv('/home/farzan/robot/ILT22_backtest/csv_html/eurousd.csv', index = False, sep = ',')




if __name__ == '__main__':
    data_generator(1577880000, 1659484800, 'h4', 'BTCUSDT')
    # euro_h4()
    # euro_daily()

    # sp500(1420070400, 1652832000)
    # bitcoin_h4(1577836800, 1656633600)
    #euro()

