import backtrader as bt
import backtrader.indicators as btind
import backtrader.feeds as btfeeds

class TestStrategy(bt.Strategy):
    params = dict(period=20)

    def __init__(self):
        self.sma = sma = btind.SimpleMovingAverage(self.data, period=20)

        close_over_sma = self.data.close > sma
        self.sma_dist_to_high = self.data.high - sma

        sma_dist_small = self.sma_dist_to_high < 3.55

        self.sell_sig = bt.And(close_over_sma, sma_dist_small)

    def next(self):
        if self.sma > 30.0:
            print('sma is greater than 30.0')

        if self.sma > self.data.close:
            print('sma is above the close price')

        if self.sell_sig: 
            print('sell isg is True')
        else:
            print('sell sig is False')

        if self.sma_dist_to_high > 5.0:
            print('distance from sma to high is greater than 5.0')

def runTest(dataframe):
    cerebro = bt.Cerebro()
    cerebro.addstrategy(TestStrategy);

    data = btfeeds.PandasData(dataname=dataframe)

    cerebro.adddata(data)
    cerebro.broker.setcash(100000.0)
    cerebro.addsizer(bt.sizers.FixedSize, stake=10)
    cerebro.broker.setcommission(commission=0.0)

    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())

    cerebro.run(maxcpus=1, stdstats=False)

    # print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
    cerebro.plot()