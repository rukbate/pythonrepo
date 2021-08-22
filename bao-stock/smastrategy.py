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
        self.sma_small_tf = btind.SMA(self.data, period=self.p.period)
        if not self.p.onlydaily:
            self.sma_large_tf = btind.SMA(self.data1, period=self.p.period)

    def nextstart(self):
        print('----------------------------------')
        print('nextstart called when len', len(self))
        print('----------------------------------')

        super(SMAStrategy, self).nextstart()

def runstrat(dataframe):
    args = parse_args()
    cerebro = bt.Cerebro(stdstats=False)

    if not args.indicators:
        cerebro.addstrategy(bt.Strategy)
    else:
        cerebro.addstrategy(
            SMAStrategy,
            period=args.period,
            onlydaily=args.onlydaily,
        )

    data = btfeeds.PandasData(dataname=dataframe)
    cerebro.adddata(data)

    tframes = dict(
        daily=bt.TimeFrame.Days,
        weekly=bt.TimeFrame.Weeks,
        monthly=bt.TimeFrame.Months
    )

    cerebro.resampledata(data, timeframe=tframes[args.timeframe], compression=args.compression)

    cerebro.run()
    cerebro.plot(style='bar')

def parse_args():
    parser = argparse.ArgumentParser(description='Multitimeframe test')

    parser.add_argument('--timeframe', default='weekly', required=False, choices=['daily', 'weekly', 'monthly'], help='Timeframe to resample to')
    parser.add_argument('--compression', default=1, required=False, type=int, help='Compression n bars in to 1')
    parser.add_argument('--indicators', action='store_true', help='Whether to apply Strategy with indicators')
    parser.add_argument('--onlydaily', action='store_true', help='Indicator only to be applied to daily timeframe')
    parser.add_argument('--period', default=10, required=False, type=int, help='Period to apply to indicator')

    return parser.parse_args()

