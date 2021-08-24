import backtrader as bt
import backtrader.indicators as btind
import backtrader.feeds as btfeeds
import datetime
import argparse
import collections

MAINSIGNALS = collections.OrderedDict(
    (('longshort', bt.SIGNAL_LONGSHORT),
     ('longonly', bt.SIGNAL_LONG),
     ('shortonly', bt.SIGNAL_SHORT),)
)

EXITSIGNALS = {
    'longexit': bt.SIGNAL_LONGEXIT,
    'shortexit': bt.SIGNAL_SHORTEXIT,
}

class SMACloseSignal(bt.Indicator):
    lines = ('signal',)
    params = (('period', 30),)

    def __init__(self):
        self.lines.signal = self.data - bt.indicators.SMA(period=self.p.period)

class SMAExitSignal(bt.Indicator):
    lines = ('signal',)
    params = (('p1', 5), ('p2', 30),)

    def __init__(self):
        sma1 = bt.indicators.SMA(period=self.p.p1)
        sma2 = bt.indicators.SMA(period=self.p.p2)
        self.lines.signal = sma1 - sma2

def runTest(dataframe, args=None):
    args = parse_args(args)

    cerebro = bt.Cerebro()
    cerebro.broker.setcash(args.cash)

    data = btfeeds.PandasData(dataname=dataframe)

    cerebro.adddata(data)
    cerebro.add_signal(MAINSIGNALS[args.signal], SMACloseSignal, period=args.smaperiod)
    
    if args.exitsignal is not None:
        cerebro.add_signal(
            EXITSIGNALS[args.exitsignal],
            SMAExitSignal,
            p1=args.exitperiod,
            p2=args.smaperiod
        )

    cerebro.run()

    if args.plot:
        pkwargs = dict(style='bar')
        if args.plot is not True:
            npkwargs = eval('dict(' + args.plot + ')')
            pkwargs.update(npkwargs)
        
        cerebro.plot(**pkwargs)

def parse_args(pargs=None):
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description='Sample for Signal concepts'
    )

    parser.add_argument('--cash', required=False, action='store', type=float, default=500000, help=('Cash to start with'))
    parser.add_argument('--smaperiod', required=False, action='store', type=int, default=30, help=('Period for the moving average'))
    parser.add_argument('--exitperiod', required=False, action='store', type=int, default=5, help=('Period for the exit control SMA'))
    parser.add_argument('--signal', required=False, action='store', default=list(MAINSIGNALS.keys())[0], choices=MAINSIGNALS, help=('Signal type to use for the main signal'))
    parser.add_argument('--exitsignal', required=False, action='store', default=None, choices=EXITSIGNALS, help=('Signal type to use for the exit signal'))
    parser.add_argument('--plot', '-p', nargs='?', required=False, metavar='kwargs', const=True, help=('Plot the read data applying any kwargs passed'))

    if pargs is not None:
        return parser.parse_args(pargs)

    return parser.parse_args()