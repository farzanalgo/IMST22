from logging import setLoggerClass
import os
import datetime
import backtrader as bt
from backtrader_plotting import Bokeh
from backtrader_plotting.schemes import Tradimo
import pandas as pd 
import numpy as np 
import time

class trade_list(bt.Analyzer):

    def get_analysis(self):

        return self.trades


    def __init__(self):

        self.trades = []
        self.cumprofit = 0.0


    def notify_trade(self, trade):

        if trade.isclosed:

            brokervalue = self.strategy.broker.getvalue()

            dir = 'short'
            if trade.history[0].event.size > 0: 
                dir = 'long'

            pricein  = trade.history[len(trade.history)-1].status.price
            priceout = trade.history[len(trade.history)-1].event.price
            datein   = (bt.num2date(trade.history[0].status.dt))
            dateout  = (bt.num2date(trade.history[len(trade.history)-1].status.dt))
            ts_in     = (bt.num2date(trade.history[0].status.dt)).timestamp()*1000
            ts_out    = (bt.num2date(trade.history[len(trade.history)-1].status.dt)).timestamp()*1000
            if trade.data._timeframe >= bt.TimeFrame.Days:
                datein  = datein.date()
                dateout = dateout.date()

            pcntchange = 100 * priceout / pricein - 100
            pnl        = trade.history[len(trade.history)-1].status.pnlcomm
            pnlpcnt    = 100 * pnl / brokervalue
            barlen     = trade.history[len(trade.history)-1].status.barlen
            pbar       = pnl / barlen
            self.cumprofit += pnl

            size = value = 0.0
            for record in trade.history:
                if abs(size) < abs(record.status.size):
                    size  = record.status.size
                    value = record.status.value

            highest_in_trade = max(trade.data.high.get(ago=0, size=barlen+1))
            lowest_in_trade  = min(trade.data.low.get(ago=0, size=barlen+1))
            hp = 100 * (highest_in_trade - pricein) / pricein
            lp = 100 * (lowest_in_trade - pricein) / pricein
            if dir == 'long':
                mfe = hp
                mae = lp
            if dir == 'short':
                mfe = -lp
                mae = -hp

            self.trades.append({'ref': trade.ref, 'ticker': trade.data._name, 'dir': dir,
                'datein': datein, 'pricein': pricein, 'dateout': dateout, 'priceout': priceout,
                'chng%': round(pcntchange, 2), 'pnl': pnl, 'pnl%': round(pnlpcnt, 2),
                'size': size, 'value': value, 'cumpnl': self.cumprofit,
                'nbars': barlen, 'pnl/bar': round(pbar, 2),
                'mfe%': round(mfe, 2), 'mae%': round(mae, 2), 'ts_in': ts_in, 'ts_out': ts_out})


def backtest():
    starttime    = datetime.datetime.now()
    BASE_DIR     = os.path.dirname('/home/farzan/robot/IMS22/csv_html/')
    GammaCSV_DIR = os.path.join(BASE_DIR, "BTCUSDT_h4.csv")
    plot_DIR     = os.path.join(BASE_DIR, "BTCUSDT_h4.html")

    bokehPlot   = False
    matPlotLib  = False
    printlog    = False
    cash        = 100000
    commission  = 0.0004
    size        = (False  ,1)
    percent     = (True , 95)
    whole_time  = True
    from_time   = datetime.datetime.strptime('2020-09-28 00:00:00', '%Y-%m-%d %H:%M:%S')                                      
    to_time     = datetime.datetime.strptime('2022-07-20 10:00:00', '%Y-%m-%d %H:%M:%S')


    class GenericCSV_IDF22(bt.feeds.GenericCSVData):
        lines  = ('top', 'bot')
        params =(   ('open_time'    , 0),
                    ('open'         , 1),
                    ('high'         , 2),
                    ('low'          , 3),
                    ('close'        , 4),
                    ('volume'       ,-1),
                    ('openinterest' ,-1),
                    ('top'          , 5),
                    ('bot'          , 6),
                    )



    class IDF22(bt.Strategy):
        params = (  ('h4_entry',    1),
                    ('h4_stop',     0.5),    
                    ('printlog',    printlog),
                    ('atr_period',  14),
                    ('tenkan_period'   , 20),
                    ('kijun_period'    , 60),
                    ('senkou_period'   , 120),
                    ('shift_period'    , 29),
                    ('kumo_trail'      , 1.5),
                    )

        def log(self, txt, dt=None, doprint=False):
            dt = dt or self.data.datetime[0]
            if self.params.printlog or doprint:
                if isinstance(dt, float):
                    dt = bt.num2date(dt)
                print('%s, %s' % (dt.isoformat(), txt))                                                   

        def __init__(self):

            #h4 candles
            self.open       = self.datas[0].open
            self.high       = self.datas[0].high
            self.low        = self.datas[0].low
            self.clos       = self.datas[0].close

            self.open_d     = self.datas[1].open
            self.high_d     = self.datas[1].high
            self.low_d      = self.datas[1].low
            self.clos_d     = self.datas[1].close

            self.top = self.datas[0].top
            self.bot = self.datas[0].bot

            #h4_ichi
            self.ichi                 = bt.ind.Ichimoku(
                                                        self.datas[0],
                                                        tenkan      = self.p.tenkan_period,
                                                        kijun       = self.p.kijun_period,
                                                        senkou      = self.p.senkou_period,
                                                        senkou_lead = self.p.shift_period,
                                                        chikou      = self.p.shift_period,
                                                        )
                                                        
            self.cross_clos_tenkan    = bt.ind.CrossOver(self.clos, self.ichi.tenkan_sen)
            self.cross_clos_kijun     = bt.ind.CrossOver(self.clos, self.ichi.kijun_sen)
            self.cross_clos_senkou_a  = bt.ind.CrossOver(self.clos, self.ichi.senkou_span_a)
            self.cross_clos_senkou_b  = bt.ind.CrossOver(self.clos, self.ichi.senkou_span_b)
            self.cross_tenkan_kijun   = bt.ind.CrossOver(self.ichi.tenkan_sen, self.ichi.kijun_sen)
            self.cross_kijun_senkou_b = bt.ind.CrossOver(self.ichi.kijun_sen, self.ichi.senkou_span_b)
            self.cross_kijun_senkou_a = bt.ind.CrossOver(self.ichi.kijun_sen, self.ichi.senkou_span_a)


            #daily_ichi
            self.ichi_daily           = bt.ind.Ichimoku(
                                                        self.datas[1],
                                                        tenkan      = 10,
                                                        kijun       = 34,
                                                        senkou      = 55,
                                                        senkou_lead = 30,
                                                        chikou      = 30,
                                                        )

            self.exit_dly = bt.ind.CrossOver(self.clos, self.ichi_daily.kijun_sen)

            #indicators
            self.atr   = bt.ind.ATR(self.datas[0], period = self.p.atr_period)
            #self.adx   = bt.ind.AverageDirectionalMovementIndex(self.datas[0], period = self.p.adx_period)

            self.order = None

            self.sell_list = []
            self.buy_list  = []

            self.long_stop  = []
            self.short_stop = []

            self.min_dist     = []
            self.new_stoploss = []

            self.one_hund_stop    = []
            self.fifty_stop       = [] 
            self.kijun_dist_stop  = []

            self.exit    = []
            self.pnl_100 = []
            self.pnl_50  = []

            self.top_list = []
            self.bot_list = []

            self.higher_low = []
            self.lower_high = []

            self.stoploss_list = []



        def cancel_open_orders(self):
            [self.cancel(order) for order in self.broker.orders if order.status < 4]

        def notify_order(self, order):
            side  = lambda order:   "BUY" if order.size > 0 else "SELL"
            otype = lambda order:   "  MARKET    " if order.exectype==0 else \
                                    "  CLOSE     " if order.exectype==1 else \
                                    "  LIMIT     " if order.exectype==2 else \
                                    "  STOP      " if order.exectype==3 else \
                                    "  STOP Trail" if order.exectype==5 else \
                                    "  STOPLIMIT " 

            k = str(order.Status[order.status])
            def statement(order):
                txt=side(order)+ \
                    otype(order)+ \
                    f'{k}'\
                    f' Size: {order.executed.size}' \
                    f' Price: {order.executed.price:.2f}' \
                    f' Commission: {order.executed.comm:.2f}' 
                return txt
                

            if order.status in [bt.Order.Submitted, bt.Order.Accepted]:
                return

            else:
                txt = statement(order)
                self.log(txt)


            if order.status is order.Completed:
                if order.exectype==0:          
                    self.broker.setcommission(0.0002)
                else: 
                    self.broker.setcommission(0.0004)


            self.order = None



        def notify_trade(self, trade):

            # if trade.isopen:
            #     print(trade.pnl)
            #     if self.getposition(self.data).size > 0 and trade.pnl > self.min_pnl:
            #         self.cancel_open_orders()
            #         updated_stop = self.sell()


            if trade.isclosed:
                if self.getposition(self.data).size == 0:
                    self.cancel_open_orders()

                self.sell_list = []
                self.buy_list  = []

                self.long_stop  = []
                self.short_stop = []

                self.one_hund_stop   = []
                self.fifty_stop      = []
                self.kijun_dist_stop = []

                self.exit    = []
                self.pnl_100 = []
                self.pnl_50  = []

                self.higher_low = []
                self.lower_high = []


                txt = 'TRADE PNL        Gross {}, Net {}'.format(
                                        round(trade.pnl,2),
                                        round(trade.pnlcomm,2))
                self.log(txt)


        def next(self):

            if self.top == 1:
                self.top_list.append(self.high)

            if self.bot == 1:
                self.bot_list.append(self.low)


            #pnl exit
            if (self.getposition(self.data).size > 0):
                pnl = (self.clos - self.getposition(self.data).price)/self.getposition(self.data).price
                if pnl > 1 and len(self.pnl_100) == 0:
                    self.pnl_100.append(pnl)
                if pnl > 0.5 and len(self.pnl_50) == 0:
                    self.pnl_50.append(pnl)

                
                if (self.clos - self.ichi.kijun_sen) > 4*self.atr or (self.clos - self.ichi.tenkan_sen) > 3*self.atr and ((self.ichi.kijun_sen - 0.5*self.atr) > self.long_stop[0]):
                    self.cancel_open_orders()
                    self.long_stop = []
                    self.close(price = self.ichi.kijun_sen - 0.5*self.atr, exectype = bt.Order.Stop)
                    self.long_stop.append(self.ichi.kijun_sen - 0.5*self.atr)
                    
                    print(bt.num2date(self.datas[0].datetime[0]), '4 atr distance stop below kijun', self.ichi.kijun_sen - 0.5*self.atr)


                if len(self.pnl_100) != 0 and (self.clos < (self.ichi.tenkan_sen - 1*self.atr)):
                    self.cancel_open_orders()
                    one_hund_stop = self.close()
                    print(bt.num2date(self.datas[0].datetime[0]), 'updating stoploss 100%', 'price:', one_hund_stop.price)
                    return

                if len(self.pnl_50) != 0 and (self.clos < (self.ichi.kijun_sen - 1*self.atr)):
                    self.cancel_open_orders()
                    fifty_stop = self.close()
                    print(bt.num2date(self.datas[0].datetime[0]), 'updating stoploss 50%', 'price:', fifty_stop.price)
                    return

                if self.bot == 1 and ((self.low - 1*self.atr) > self.long_stop[0]):
                    self.cancel_open_orders()
                    self.long_stop = []
                    self.close(price = self.low - 1*self.atr, exectype = bt.Order.Stop)
                    self.long_stop.append(self.low - 1*self.atr)
                    print(bt.num2date(self.datas[0].datetime[0]), 'fractal stoploss placed@', self.low - 1*self.atr)


            if (self.getposition(self.data).size < 0):
                pnl = (self.getposition(self.data).price - self.clos)/self.getposition(self.data).price
                if pnl > 1 and len(self.pnl_100) == 0:
                    self.pnl_100.append(pnl)
                if pnl > 0.5 and len(self.pnl_50) == 0:
                    self.pnl_50.append(pnl)

                if (self.ichi.kijun_sen - self.clos) > 4*self.atr or (self.ichi.tenkan_sen - self.clos) > 3*self.atr and (self.ichi.kijun_sen + 0.5*self.atr) < self.short_stop[0]:
                    self.cancel_open_orders()
                    self.short_stop = []
                    self.close(price = self.ichi.kijun_sen + 0.5*self.atr, exectype = bt.Order.Stop)
                    self.short_stop.append(self.ichi.kijun_sen + 0.5*self.atr)
                    print(bt.num2date(self.datas[0].datetime[0]), '4 atr distance stop above kijun', self.ichi.kijun_sen + 0.5*self.atr)

                if len(self.pnl_100) != 0 and (self.clos > (self.ichi.tenkan_sen + 1*self.atr)):
                    self.cancel_open_orders()
                    one_hund_stop = self.close()
                    print(bt.num2date(self.datas[0].datetime[0]), 'updating stoploss 100%', 'price:', one_hund_stop.price)
                    return

                if len(self.pnl_50) != 0 and (self.clos > (self.ichi.kijun_sen + 1*self.atr)):
                    self.cancel_open_orders()
                    fifty_stop = self.close()
                    print(bt.num2date(self.datas[0].datetime[0]), 'updating stoploss 50%', 'price:', fifty_stop.price)
                    return

                if self.top == 1 and (self.high + 1*self.atr) < self.short_stop[0]:
                    self.cancel_open_orders()
                    self.close(price = self.high + 1*self.atr, exectype = bt.Order.Stop)
                    print(bt.num2date(self.datas[0].datetime[0]), 'fractal stoploss placed@', self.high + 1*self.atr)


            #updating stoploss 
            if self.getposition(self.data).size > 0 and (len(self.min_dist) != 0):
                if (self.clos - self.getposition(self.data).price) > self.min_dist[0]:
                    self.min_dist = []
                    self.long_stop = []
                    self.cancel_open_orders()
                    new_stop = self.close(price = self.new_stoploss[0], exectype = bt.Order.Stop)
                    self.long_stop.append(self.new_stoploss[0])

                    print(bt.num2date(self.datas[0].datetime[0]), 'updating stoploss', 'price:', self.new_stoploss[0])
                    self.new_stoploss = []

            if self.getposition(self.data).size < 0 and (len(self.min_dist) != 0):
                if (self.getposition(self.data).price - self.clos) > self.min_dist[0]:
                    self.min_dist = []
                    self.short_stop = []
                    self.cancel_open_orders()
                    new_stop = self.close(price = self.new_stoploss[0], exectype = bt.Order.Stop)
                    self.short_stop.append(self.new_stoploss[0])

                    print(bt.num2date(self.datas[0].datetime[0]), 'updating stoploss', 'price:', self.new_stoploss[0])
                    self.new_stoploss = []


            #open orders check
            b1 = self.clos > self.ichi_daily.kijun_sen and self.clos > self.ichi.senkou_span_a and self.ichi.senkou_span_a > self.ichi.senkou_span_b and self.ichi.tenkan_sen >= self.ichi.kijun_sen and self.ichi.kijun_sen >= self.ichi.senkou_span_b and self.clos > self.high[-(self.p.shift_period - 1)]
            b2 = self.clos > self.ichi_daily.kijun_sen and self.clos > self.ichi.senkou_span_b and self.ichi.senkou_span_a < self.ichi.senkou_span_b and self.ichi.tenkan_sen >= self.ichi.kijun_sen and self.ichi.kijun_sen >= self.ichi.senkou_span_a and self.clos > self.high[-(self.p.shift_period - 1)]

            s1 = self.clos < self.ichi_daily.kijun_sen and self.clos < self.ichi.senkou_span_b and self.ichi.senkou_span_a > self.ichi.senkou_span_b and self.ichi.tenkan_sen <= self.ichi.kijun_sen and self.ichi.kijun_sen <= self.ichi.senkou_span_a and self.clos < self.low[-(self.p.shift_period - 1)]
            s2 = self.clos < self.ichi_daily.kijun_sen and self.clos < self.ichi.senkou_span_a and self.ichi.senkou_span_a < self.ichi.senkou_span_b and self.ichi.tenkan_sen <= self.ichi.kijun_sen and self.ichi.kijun_sen <= self.ichi.senkou_span_b and self.clos < self.low[-(self.p.shift_period - 1)]


            if (self.getposition(self.data).size == 0) and len(self.buy_list) == 1:
                if b1 or b2:
                    pass
                else: 
                    self.cancel_open_orders()
                    self.buy_list = []

                    print(bt.num2date(self.datas[0].datetime[0]), 'precondition changed, cancel open order')

            if (self.getposition(self.data).size == 0) and len(self.sell_list) == 1:
                if s1 or s2:
                    pass
                else: 
                    self.cancel_open_orders()
                    self.sell_list = []

                    print(bt.num2date(self.datas[0].datetime[0]), 'precondition changed, cancel open order')



            if (len(self.buy_list) > 1) or (len(self.sell_list) > 1):
                print('******************************** 2 open order simultanously *******************************************')


            if self.order:
                return


            entry_price = self.high + self.p.h4_entry*self.atr
            #H4 signal
            if (self.getposition(self.data).size == 0) and (self.clos > self.ichi_daily.kijun_sen) and len(self.buy_list) == 0 and (self.atr/self.atr[-1] < 1.5):
                #h4 long
                if (self.clos > self.ichi.senkou_span_a) and (self.clos > self.ichi.senkou_span_b) and (self.clos > self.high[-(self.p.shift_period - 1)]) and (self.ichi.tenkan_sen >= self.ichi.kijun_sen) and (self.ichi.kijun_sen >= np.where(self.ichi.senkou_span_a < self.ichi.senkou_span_b, self.ichi.senkou_span_a, self.ichi.senkou_span_b)[0]) and (self.clos >= self.ichi.tenkan_sen) and (self.cross_clos_kijun == +1):
                    self.min_dist = []
                    self.new_stoploss = []
                    self.cancel_open_orders()
                    buy  = self.buy(price = self.high + self.p.h4_entry*self.atr, exectype = bt.Order.Stop, transmit=False)
                    if abs(entry_price - self.ichi.kijun_sen) > 4.8*self.atr:
                        stop = self.sell(price = self.ichi.tenkan_sen - 1*self.atr, exectype = bt.Order.Stop, parent = buy, transmit=True)
                    else:
                        stop = self.sell(price = self.low - 1*self.atr, exectype = bt.Order.Stop, parent = buy, transmit=True)

                    self.buy_list.append(buy)
                    self.long_stop.append(stop.price)
                    self.min_dist.append(buy.price - stop.price)
                    self.new_stoploss.append(self.high[0])

                    print(bt.num2date(self.datas[0].datetime[0]), 'stoploss amount on 1 BTC:', self.min_dist[0])
                    print(bt.num2date(self.datas[0].datetime[0]), 'tf:H4 side:long signal: clos/kijun')

                    return

                elif (self.clos > self.ichi.senkou_span_a) and (self.clos > self.ichi.senkou_span_b) and (self.ichi.tenkan_sen >= self.ichi.kijun_sen) and (self.clos >= self.ichi.tenkan_sen) and (self.ichi.kijun_sen >= np.where(self.ichi.senkou_span_a < self.ichi.senkou_span_b, self.ichi.senkou_span_a, self.ichi.senkou_span_b)[0]) and (self.clos[-2] < self.high[-self.p.shift_period - 1]) and (self.clos[-1] < self.high[-self.p.shift_period]) and (self.clos > self.high[-(self.p.shift_period - 1)]):
                    self.min_dist = []
                    self.new_stoploss = []
                    self.cancel_open_orders()
                    buy  = self.buy(price = self.high + self.p.h4_entry*self.atr, exectype = bt.Order.Stop, transmit = False)
                    a = np.where(self.ichi.senkou_span_a > self.ichi.senkou_span_b, self.ichi.senkou_span_a, self.ichi.senkou_span_b)
                    if abs(entry_price - self.ichi.kijun_sen) > 4.8*self.atr:
                        stop = self.sell(price = self.ichi.tenkan_sen - 1*self.atr, exectype = bt.Order.Stop, parent = buy, transmit=True)
                    else:
                        stop = self.sell(price = self.ichi.kijun_sen - 1*self.atr, exectype = bt.Order.Stop, parent = buy, transmit=True)
                    
                    self.buy_list.append(buy)
                    self.long_stop.append(stop.price)
                    self.min_dist.append(buy.price - stop.price)
                    self.new_stoploss.append(self.high[0])

                    print(bt.num2date(self.datas[0].datetime[0]), 'stoploss amount on 1 BTC:', self.min_dist[0])
                    print(bt.num2date(self.datas[0].datetime[0]), 'tf:H4 side:long signal: high/chikou')

                    return

                elif (self.clos > self.ichi.senkou_span_a) and (self.clos > self.ichi.senkou_span_b) and (self.clos > self.high[-(self.p.shift_period - 1)]) and (self.ichi.kijun_sen >= np.where(self.ichi.senkou_span_a < self.ichi.senkou_span_b, self.ichi.senkou_span_a, self.ichi.senkou_span_b)[0]) and (self.clos >= self.ichi.tenkan_sen) and (self.ichi.tenkan_sen[-1] < self.ichi.kijun_sen[-1]) and (self.ichi.tenkan_sen >= self.ichi.kijun_sen):
                    self.min_dist = []
                    self.new_stoploss = []
                    self.cancel_open_orders()
                    buy  = self.buy(price = self.high + self.p.h4_entry*self.atr, exectype = bt.Order.Stop, transmit = False)
                    if abs(entry_price - self.ichi.kijun_sen) > 4.8*self.atr:
                        stop = self.sell(price = self.ichi.tenkan_sen - 1*self.atr, exectype = bt.Order.Stop, parent = buy, transmit=True)
                    else:
                        stop = self.sell(price = self.ichi.kijun_sen  - self.p.h4_stop*self.atr, exectype = bt.Order.Stop, parent = buy, transmit = True)
                    
                    self.buy_list.append(buy)
                    self.long_stop.append(stop.price)
                    self.min_dist.append(buy.price - stop.price)
                    self.new_stoploss.append(buy.price)

                    print(bt.num2date(self.datas[0].datetime[0]), 'stoploss amount on 1 BTC:', self.min_dist[0])
                    print(bt.num2date(self.datas[0].datetime[0]), 'tf:H4 side:long signal: tenkan/kijun')

                    return

                elif  (self.clos > self.high[-(self.p.shift_period - 1)]) and (self.ichi.tenkan_sen >= self.ichi.kijun_sen) and (self.ichi.kijun_sen >= np.where(self.ichi.senkou_span_a < self.ichi.senkou_span_b, self.ichi.senkou_span_a, self.ichi.senkou_span_b)[0]) and (self.clos >= self.ichi.tenkan_sen) and (self.ichi.senkou_span_a > self.ichi.senkou_span_b) and (self.cross_clos_senkou_a == 1):
                    self.min_dist = []
                    self.new_stoploss = []
                    self.cancel_open_orders()
                    buy  = self.buy(price = self.high + self.p.h4_entry*self.atr, exectype = bt.Order.Stop, transmit = False)
                    if abs(entry_price - self.ichi.kijun_sen) > 4.8*self.atr:
                        stop = self.sell(price = self.ichi.tenkan_sen - 1*self.atr, exectype = bt.Order.Stop, parent = buy, transmit=True)
                    else:
                        stop = self.sell(price = self.low - 1*self.atr, exectype = bt.Order.Stop, parent = buy, transmit = True)
                    
                    self.buy_list.append(buy)
                    self.long_stop.append(stop.price)
                    self.min_dist.append(buy.price - stop.price)
                    self.new_stoploss.append(buy.price)

                    print(bt.num2date(self.datas[0].datetime[0]), 'stoploss amount on 1 BTC:', self.min_dist[0])
                    print(bt.num2date(self.datas[0].datetime[0]), 'tf:H4 side:long signal: clos/kumo')

                    return

                elif  (self.clos > self.high[-(self.p.shift_period - 1)]) and (self.ichi.tenkan_sen >= self.ichi.kijun_sen) and (self.ichi.kijun_sen >= np.where(self.ichi.senkou_span_a < self.ichi.senkou_span_b, self.ichi.senkou_span_a, self.ichi.senkou_span_b)[0]) and (self.clos >= self.ichi.tenkan_sen) and (self.ichi.senkou_span_a < self.ichi.senkou_span_b) and (self.cross_clos_senkou_b == 1):
                    self.min_dist = []
                    self.new_stoploss = []
                    self.cancel_open_orders()
                    buy  = self.buy(price = self.high + self.p.h4_entry*self.atr, exectype = bt.Order.Stop, transmit = False)
                    if abs(entry_price - self.ichi.kijun_sen) > 4.8*self.atr:
                        stop = self.sell(price = self.ichi.tenkan_sen - 1*self.atr, exectype = bt.Order.Stop, parent = buy, transmit=True)
                    else:
                        stop = self.sell(price = self.low - 1*self.atr, exectype = bt.Order.Stop, parent = buy, transmit = True)
                    
                    self.buy_list.append(buy)
                    self.long_stop.append(stop.price)
                    self.min_dist.append(buy.price - stop.price)
                    self.new_stoploss.append(buy.price)

                    print(bt.num2date(self.datas[0].datetime[0]), 'stoploss amount on 1 BTC:', self.min_dist[0])
                    print(bt.num2date(self.datas[0].datetime[0]), 'tf:H4 side:long signal: clos/kumo')

                    return

                elif (self.clos > self.ichi.senkou_span_a) and (self.clos > self.ichi.senkou_span_b) and (self.clos > self.high[-(self.p.shift_period - 1)]) and (self.ichi.tenkan_sen >= self.ichi.kijun_sen) and (self.clos >= self.ichi.tenkan_sen) and (((self.ichi.senkou_span_a > self.ichi.senkou_span_b) and (self.cross_kijun_senkou_b == 1)) or ((self.ichi.senkou_span_a < self.ichi.senkou_span_b) and (self.cross_kijun_senkou_a == 1))):
                    self.min_dist = []
                    self.new_stoploss = []
                    self.cancel_open_orders()
                    buy  = self.buy(price = self.high + self.p.h4_entry*self.atr, exectype = bt.Order.Stop, transmit = False)
                    if abs(entry_price - self.ichi.kijun_sen) > 4.8*self.atr:
                        stop = self.sell(price = self.ichi.tenkan_sen - 1*self.atr, exectype = bt.Order.Stop, parent = buy, transmit=True)
                    else:
                        stop = self.sell(price = self.ichi.kijun_sen - self.p.h4_stop*self.atr, exectype = bt.Order.Stop, parent = buy, transmit = True)
                    
                    self.buy_list.append(buy)
                    self.long_stop.append(stop.price)
                    self.min_dist.append(buy.price - stop.price)
                    self.new_stoploss.append(buy.price)

                    print(bt.num2date(self.datas[0].datetime[0]), 'stoploss amount on 1 BTC:', self.min_dist[0])
                    print(bt.num2date(self.datas[0].datetime[0]), 'tf:H4 side:long signal: kijun/kumo')

                    return


            entry_price = self.low - self.p.h4_entry*self.atr
            if (self.getposition(self.data).size == 0) and (self.clos < self.ichi_daily.kijun_sen) and len(self.sell_list) == 0 and (self.atr/self.atr[-1] < 1.5):
                #h4 short
                if (self.clos < self.ichi.senkou_span_a) and (self.clos < self.ichi.senkou_span_b) and (self.clos < self.low[-(self.p.shift_period - 1)]) and (self.ichi.tenkan_sen <= self.ichi.kijun_sen) and (self.ichi.kijun_sen <= np.where(self.ichi.senkou_span_a > self.ichi.senkou_span_b, self.ichi.senkou_span_a, self.ichi.senkou_span_b)[0]) and (self.clos <= self.ichi.tenkan_sen) and (self.cross_clos_kijun == -1):
                    self.min_dist = []
                    self.new_stoploss = []
                    self.cancel_open_orders()
                    sell = self.sell(price = self.low - self.p.h4_entry*self.atr, exectype = bt.Order.Stop, transmit=False)
                    if abs(entry_price - self.ichi.kijun_sen) > 4.8*self.atr:
                        stop = self.buy(price = self.ichi.tenkan_sen + 1*self.atr, exectype = bt.Order.Stop, parent = sell, transmit=True)
                    else:
                        stop = self.buy(price = self.high + 1*self.atr, exectype = bt.Order.Stop, parent = sell, transmit=True)
                    
                    self.sell_list.append(sell)
                    self.short_stop.append(stop.price)
                    self.min_dist.append(stop.price - sell.price)
                    self.new_stoploss.append(sell.price)

                    print(bt.num2date(self.datas[0].datetime[0]), 'stoploss amount on 1 BTC:', self.min_dist[0])
                    print(bt.num2date(self.datas[0].datetime[0]), 'tf:H4 side:short signal: clos/kijun')

                    return

                elif (self.clos < self.ichi.senkou_span_a) and (self.clos < self.ichi.senkou_span_b) and (self.ichi.tenkan_sen <= self.ichi.kijun_sen) and (self.clos <= self.ichi.tenkan_sen) and (self.ichi.kijun_sen <= np.where(self.ichi.senkou_span_a > self.ichi.senkou_span_b, self.ichi.senkou_span_a, self.ichi.senkou_span_b)[0]) and (self.clos[-2] > self.low[-self.p.shift_period - 1]) and (self.clos[-1] > self.low[-self.p.shift_period]) and (self.clos < self.low[-(self.p.shift_period - 1)]):
                    self.min_dist = []
                    self.new_stoploss = []
                    self.cancel_open_orders()
                    sell = self.sell(price = self.low - self.p.h4_entry*self.atr, exectype = bt.Order.Stop, transmit=False)
                    if abs(entry_price - self.ichi.kijun_sen) > 4.8*self.atr:
                        stop = self.buy(price = self.ichi.tenkan_sen + 1*self.atr, exectype = bt.Order.Stop, parent = sell, transmit=True)
                    else:
                        stop = self.buy(price = self.ichi.kijun_sen + 1*self.atr, exectype = bt.Order.Stop, parent = sell, transmit=True)
                    
                    self.sell_list.append(sell)
                    self.short_stop.append(stop.price)
                    self.min_dist.append(stop.price - sell.price)
                    self.new_stoploss.append(self.low[0])

                    print(bt.num2date(self.datas[0].datetime[0]), 'stoploss amount on 1 BTC:', self.min_dist[0])
                    print(bt.num2date(self.datas[0].datetime[0]), 'tf:H4 side:short signal: low/chikou')

                    return

                elif (self.clos < self.ichi.senkou_span_a) and (self.clos < self.ichi.senkou_span_b) and (self.clos < self.low[-(self.p.shift_period - 1)]) and (self.ichi.kijun_sen <= np.where(self.ichi.senkou_span_a > self.ichi.senkou_span_b, self.ichi.senkou_span_a, self.ichi.senkou_span_b)[0]) and (self.clos <= self.ichi.tenkan_sen) and (self.ichi.tenkan_sen[-1] > self.ichi.kijun_sen[-1]) and (self.ichi.tenkan_sen <= self.ichi.kijun_sen):
                    self.min_dist = []
                    self.new_stoploss = []
                    self.cancel_open_orders()
                    sell = self.sell(price = self.low - self.p.h4_entry*self.atr, exectype = bt.Order.Stop, transmit=False)
                    if abs(entry_price - self.ichi.kijun_sen) > 4.8*self.atr:
                        stop = self.buy(price = self.ichi.tenkan_sen + 1*self.atr, exectype = bt.Order.Stop, parent = sell, transmit=True)
                    else:
                        stop = self.buy(price = self.ichi.kijun_sen + self.p.h4_stop*self.atr, exectype = bt.Order.Stop, parent = sell, transmit=True)
                    
                    self.sell_list.append(sell)
                    self.short_stop.append(stop.price)
                    self.min_dist.append(stop.price - sell.price)
                    self.new_stoploss.append(sell.price)

                    print(bt.num2date(self.datas[0].datetime[0]), 'stoploss amount on 1 BTC:', self.min_dist[0])
                    print(bt.num2date(self.datas[0].datetime[0]), 'tf:H4 side:short signal: tenkan/kijun')

                    return

                elif  (self.clos < self.low[-(self.p.shift_period - 1)]) and (self.ichi.tenkan_sen <= self.ichi.kijun_sen) and (self.ichi.kijun_sen <= np.where(self.ichi.senkou_span_a > self.ichi.senkou_span_b, self.ichi.senkou_span_a, self.ichi.senkou_span_b)[0]) and (self.clos <= self.ichi.tenkan_sen) and (self.ichi.senkou_span_a < self.ichi.senkou_span_b) and (self.cross_clos_senkou_a == -1):
                    self.min_dist = []
                    self.new_stoploss = []
                    self.cancel_open_orders()
                    sell = self.sell(price = self.low - self.p.h4_entry*self.atr, exectype = bt.Order.Stop, transmit=False)
                    if abs(entry_price - self.ichi.kijun_sen) > 4.8*self.atr:
                        stop = self.buy(price = self.ichi.tenkan_sen + 1*self.atr, exectype = bt.Order.Stop, parent = sell, transmit=True)
                    else:
                        stop = self.buy(price = self.high + 1*self.atr, exectype = bt.Order.Stop, parent = sell, transmit=True)
                    
                    self.sell_list.append(sell)
                    self.short_stop.append(stop.price)
                    self.min_dist.append(stop.price - sell.price)
                    self.new_stoploss.append(sell.price)

                    print(bt.num2date(self.datas[0].datetime[0]), 'stoploss amount on 1 BTC:', self.min_dist[0])
                    print(bt.num2date(self.datas[0].datetime[0]), 'tf:H4 side:short signal: clos/kumo')

                    return

                elif  (self.clos < self.low[-(self.p.shift_period - 1)]) and (self.ichi.tenkan_sen <= self.ichi.kijun_sen) and (self.ichi.kijun_sen <= np.where(self.ichi.senkou_span_a > self.ichi.senkou_span_b, self.ichi.senkou_span_a, self.ichi.senkou_span_b)[0]) and (self.clos <= self.ichi.tenkan_sen) and (self.ichi.senkou_span_a > self.ichi.senkou_span_b) and (self.cross_clos_senkou_b == -1):
                    self.min_dist = []
                    self.new_stoploss = []
                    self.cancel_open_orders()
                    sell = self.sell(price = self.low - self.p.h4_entry*self.atr, exectype = bt.Order.Stop, transmit=False)
                    if abs(entry_price - self.ichi.kijun_sen) > 4.8*self.atr:
                        stop = self.buy(price = self.ichi.tenkan_sen + 1*self.atr, exectype = bt.Order.Stop, parent = sell, transmit=True)
                    else:
                        stop = self.buy(price = self.high + 1*self.atr, exectype = bt.Order.Stop, parent = sell, transmit=True)
                    
                    self.sell_list.append(sell)
                    self.short_stop.append(stop.price)
                    self.min_dist.append(stop.price - sell.price)
                    self.new_stoploss.append(sell.price)

                    print(bt.num2date(self.datas[0].datetime[0]), 'stoploss amount on 1 BTC:', self.min_dist[0])
                    print(bt.num2date(self.datas[0].datetime[0]), 'tf:H4 side:short signal: clos/kumo')

                    return

                elif (self.clos < self.ichi.senkou_span_a) and (self.clos < self.ichi.senkou_span_b) and (self.clos < self.low[-(self.p.shift_period - 1)]) and (self.ichi.tenkan_sen <= self.ichi.kijun_sen) and (self.clos <= self.ichi.tenkan_sen) and (((self.ichi.senkou_span_a > self.ichi.senkou_span_b) and (self.cross_kijun_senkou_a == -1)) or ((self.ichi.senkou_span_a < self.ichi.senkou_span_b) and (self.cross_kijun_senkou_b == -1))):
                    self.min_dist = []
                    self.new_stoploss = []
                    self.cancel_open_orders()
                    sell = self.sell(price = self.low - self.p.h4_entry*self.atr, exectype = bt.Order.Stop, transmit=False)
                    if abs(entry_price - self.ichi.kijun_sen) > 4.8*self.atr:
                        stop = self.buy(price = self.ichi.tenkan_sen + 1*self.atr, exectype = bt.Order.Stop, parent = sell, transmit=True)
                    else:
                        stop = self.buy(price = self.ichi.kijun_sen + self.p.h4_stop*self.atr, exectype = bt.Order.Stop, parent = sell, transmit=True)
                    
                    self.sell_list.append(sell)
                    self.short_stop.append(stop.price)
                    self.min_dist.append(stop.price - sell.price)
                    self.new_stoploss.append(sell.price)

                    print(bt.num2date(self.datas[0].datetime[0]), 'stoploss amount on 1 BTC:', self.min_dist[0])
                    print(bt.num2date(self.datas[0].datetime[0]), 'tf:H4 side:short signal: kijun/kumo')

                    return


            #exit
            #TRAILING STOPLOSS
            if (self.getposition(self.data).size > 0) and (self.low < (np.where(self.ichi.senkou_span_a < self.ichi.senkou_span_b, self.ichi.senkou_span_a, self.ichi.senkou_span_b)[0] - self.p.kumo_trail*self.atr)):
                self.close()
                self.cancel_open_orders()

                print(bt.num2date(self.datas[0].datetime[0]), 'close by trailing kumo')

            elif (self.getposition(self.data).size < 0) and (self.high > (np.where(self.ichi.senkou_span_a > self.ichi.senkou_span_b, self.ichi.senkou_span_a, self.ichi.senkou_span_b)[0] + self.p.kumo_trail*self.atr)):
                self.close()
                self.cancel_open_orders()

                print(bt.num2date(self.datas[0].datetime[0]), 'close by trailing kumo')


            #usual exit
            # elif (self.getposition(self.data).size > 0) and (self.ichi.senkou_span_a < self.ichi.senkou_span_b) and (self.cross_clos_senkou_a == -1) and (len(self.exit) == 0):
            #     self.cancel_open_orders()
            #     exitloss = self.close(price = self.low - 1*self.atr, exectype = bt.Order.Stop)
            #     self.exit.append(exitloss)

            #     print(bt.num2date(self.datas[0].datetime[0]), 'close by kumo')

            # elif (self.getposition(self.data).size > 0) and (self.ichi.senkou_span_a > self.ichi.senkou_span_b) and (self.cross_clos_senkou_b == -1) and (len(self.exit) == 0):
            #     self.cancel_open_orders()
            #     exitloss = self.close(price = self.low - 1*self.atr, exectype = bt.Order.Stop)
            #     self.exit.append(exitloss)

            #     print(bt.num2date(self.datas[0].datetime[0]), 'close by kumo')

            elif (self.getposition(self.data).size > 0) and self.clos < (self.ichi_daily.kijun_sen - self.atr):
                self.cancel_open_orders()
                self.close()
                # exitloss = self.close(price = self.low - 1*self.atr, exectype = bt.Order.Stop)
                # self.exit.append(exitloss)

                print(bt.num2date(self.datas[0].datetime[0]), 'close by dly kijun cross')

            # elif (self.getposition(self.data).size < 0) and (self.ichi.senkou_span_a > self.ichi.senkou_span_b) and (self.cross_clos_senkou_a == +1) and (len(self.exit) == 0):
            #     self.cancel_open_orders()
            #     exitloss = self.close(price = self.high + 1*self.atr, exectype = bt.Order.Stop)
            #     self.exit.append(exitloss)

            #     print(bt.num2date(self.datas[0].datetime[0]), 'close by kumo')

            # elif (self.getposition(self.data).size < 0) and (self.ichi.senkou_span_a < self.ichi.senkou_span_b) and (self.cross_clos_senkou_b == +1) and (len(self.exit) == 0):
            #     self.cancel_open_orders()
            #     exitloss = self.close(price = self.high + 1*self.atr, exectype = bt.Order.Stop)
            #     self.exit.append(exitloss)

            #     print(bt.num2date(self.datas[0].datetime[0]), 'close by kumo')

            elif (self.getposition(self.data).size < 0) and self.clos > (self.ichi_daily.kijun_sen + self.atr):
                self.cancel_open_orders()
                self.close()
                # exitloss = self.close(price = self.high + 1*self.atr, exectype = bt.Order.Stop)
                # self.exit.append(exitloss)

                print(bt.num2date(self.datas[0].datetime[0]), 'close by dly kijun cross')


        global fvalue 
        def stop(self):
            global fvalue                                                                                 
            txt =   f'Initial cash:{self.broker.startingcash}'\
                    f'Final cash:{self.broker.getvalue()}'
            print(txt)
            fvalue = self.broker.getvalue()



    cerebro = bt.Cerebro(cheat_on_open = True)  #cheat_on_open = True
    cerebro.broker.setcash(cash)                                                                        
    cerebro.broker.setcommission(commission = commission)                                                 

    if whole_time:
        data = GenericCSV_IDF22(
                                dataname     = GammaCSV_DIR, 
                                dtformat     = 2,
                                timeframe    = bt.TimeFrame.Minutes,
                                compression  = 240,
                                )
    else:
        data = GenericCSV_IDF22(
                            dataname    = GammaCSV_DIR, 
                            dtformat    = 2,
                            fromdate    = from_time,
                            todate      = to_time, 
                            timeframe   = bt.TimeFrame.Minutes,
                            compression = 240)

    codename = '510300'
    cerebro.adddata(data, name=codename)
    cerebro.resampledata(data, timeframe=bt.TimeFrame.Days, compression=1, name=codename)


    cerebro.addstrategy(IDF22)

    if size[0]:
        cerebro.addsizer(bt.sizers.FixedSize,   stake    = size[1])  
    elif percent[0]: 
        cerebro.addsizer(bt.sizers.PercentSizer,percents = percent[1])  

    cerebro.addanalyzer(trade_list, _name='trade_list')
    # cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='TradeAnalyzer')
    # cerebro.addanalyzer(bt.analyzers.SharpeRatio, 
    #                     timeframe       = bt.TimeFrame.Minutes, 
    #                     compression     = 24*60, 
    #                     riskfreerate    = 0.05,
    #                     #convertrate     = False, 
    #                     annualize       = True, 
    #                     factor          = 365,
    #                     _name           ='SharpeRatio')                   
    # cerebro.addanalyzer(bt.analyzers.DrawDown, _name='DrawDown')

    cerebroResponse = cerebro.run(tradehistory=True) #tradehistory=True)
    response        = cerebroResponse[0] 
    trades_list     = response.analyzers.trade_list.get_analysis()
    
    if matPlotLib:
        cerebro.plot(style              = 'candlestick',
                    barup               = 'green',
                    volume              = False)
    if bokehPlot:
        b = Bokeh(  style               = 'bar', 
                    plot_mode           = 'single', 
                    scheme              = Tradimo(), 
                    legend_text_color   = '#000000', 
                    filename            = plot_DIR,
                    volume              = False)
        cerebro.plot(b)

    endtime = datetime.datetime.now()
    print('Process time duration:',endtime-starttime)

    return pd.DataFrame(trades_list)


if __name__ == '__main__':
    backtest()
    # df = backtest()
    # df.to_csv('/home/farzan/robot/IMS22/trades(IMTS22).csv', index = False)

# df = backtest(1, 1, 1.5)
# df.to_csv('/home/farzan/robot/ILT22_backtest/csv_html/tradelist.csv')