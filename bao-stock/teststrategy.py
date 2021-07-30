import backtrader as bt
import backtrader.indicators as btind
import backtrader.feeds as btfeeds

class TestStrategy(bt.Strategy):
    params = dict(period=20)

    def __init__(self):
        self.movav = btind.SimpleMovingAverage(self.data, period=self.p.period)

    def next(self):
        if self.movav.sma[0] > self.data.close[0]:
            print('Simple Moving Average is greater than the closing price')

def runTest(dataframe):
    cerebro = bt.Cerebro()
    cerebro.addstrategy(TestStrategy);

    data = btfeeds.PandasData(dataname=dataframe)

    cerebro.adddata(data)
    cerebro.broker.setcash(100000.0)
    cerebro.addsizer(bt.sizers.FixedSize, stake=10)
    cerebro.broker.setcommission(commission=0.0)

    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())

    cerebro.run(maxcpus=1)

    # print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
    cerebro.plot()