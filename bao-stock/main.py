import dailyk as dk
import index as idx
import info
from datetime import date
import backtrader as bt
import pandas as pd

class TestStrategy(bt.Strategy):
    params = (
        ('maperiod', 15),
    )

    def log(self, txt, dt=None):
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        self.dataopen = self.datas[0].open
        self.dataclose = self.datas[0].close
        self.order = None
        self.buyprice = None
        self.buycomm = None

        self.sma = bt.indicators.SimpleMovingAverage(
            self.datas[0], 
            period=self.params.maperiod
        )

        bt.indicators.ExponentialMovingAverage(self.datas[0], period=25)
        bt.indicators.WeightedMovingAverage(self.datas[0], period=25, subplot=True)
        bt.indicators.StochasticSlow(self.datas[0], safediv=True)
        bt.indicators.MACDHisto(self.datas[0])
        rsi = bt.indicators.RSI(self.datas[0])
        bt.indicators.SmoothedMovingAverage(rsi, period=10)
        bt.indicators.ATR(self.datas[0], plot=False)

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return

        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(f'BUY EXECUTED, Price: {order.executed.price}, Cost: {order.executed.value}, Comm {order.executed.comm}')

                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
            else:
                self.log(f'SELL EXECUTED, Price: {order.executed.price}, Cost: {order.executed.value}, Comm {order.executed.comm}')
            
            self.bar_executed = len(self)
        
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')

        self.order = None
    
    def notify_trade(self, trade):
        if not trade.isclosed:
            return
        
        self.log(f'OPERATION PROFIT, GROSS {trade.pnl}, NET {trade.pnlcomm}')

    def next(self):
        # self.log('Open: %.2f, Close: %.2f' % (self.dataopen[0], self.dataclose[0]))

        if self.order:
            return 

        if not self.position:
            if self.dataclose[0] > self.sma[0]:                
                self.log(f'BUY CREATE, {self.dataclose[0]}')
                self.order = self.buy()
        else:
            if self.dataclose[0] < self.sma[0]:
                self.log(f'SELL CREATE, {self.dataclose[0]}')
                self.order = self.sell()

cerebro = bt.Cerebro()
cerebro.addstrategy(TestStrategy)

dataframe = dk.getDailyK('sh', '601888', '2019-01-01')

data = bt.feeds.PandasData(dataname=dataframe)

cerebro.adddata(data)
cerebro.broker.setcash(100000.0)
cerebro.addsizer(bt.sizers.FixedSize, stake=10)
cerebro.broker.setcommission(commission=0.0)

print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())

cerebro.run()

print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
cerebro.plot()

# dk.syncExistingStocks()
# idx.updateAllIndexes()
# idx.updateIndex('sz50')
# print(info.getInfo('sh', '600240'))

# print(type(info.getIpoDate('sh', '600298')))
# dk.syncDailyK('sh', '600690')
# data = dk.getDailyK('sh', '600298')
# print(data[data["date"] > date(2021, 5, 31)])
# for indx in idx.indexes:
    # stocks = idx.getIndex(indx)
    # for type, exchange, code, name, updateDate in stocks.to_numpy():
        # dk.syncDailyK(exchange, code)
        # None
