import backtrader as bt
import backtrader.indicators as btind
import backtrader.feeds as btfeeds
import argparse

class SMAStrategy(bt.Strategy):
    params = (
        ('period', 10),
        ('onlydaily', False),
    )

    def __init__(self):
        self.sma = btind.SMA(self.data, period=self.p.period)

    def start(self):
        self.counter = 0

    def prenext(self):
        self.counter += 1
        print('prenext len %d - counter %d' % (len(self), self.counter))

    def next(self):
        self.counter += 1
        print('---next len %d - counter %d' % (len(self), self.counter))

def runstrat(dataframe):
    args = parse_args()
    cerebro = bt.Cerebro(stdstats=False)

    cerebro.addstrategy(
        SMAStrategy,
        period=args.period,
    )

    data = btfeeds.PandasData(dataname=dataframe)
    cerebro.adddata(data)

    tframes = dict(
        daily=bt.TimeFrame.Days,
        weekly=bt.TimeFrame.Weeks,
        monthly=bt.TimeFrame.Months
    )

    cerebro.replaydata(data, timeframe=tframes[args.timeframe], compression=args.compression)

    cerebro.run()
    cerebro.plot(style='bar')

def parse_args():
    parser = argparse.ArgumentParser(description='Multitimeframe test')

    parser.add_argument('--timeframe', default='weekly', required=False, choices=['daily', 'weekly', 'monthly'], help='Timeframe to resample to')
    parser.add_argument('--compression', default=1, required=False, type=int, help='Compression n bars in to 1')
    parser.add_argument('--period', default=10, required=False, type=int, help='Period to apply to indicator')

    return parser.parse_args()

