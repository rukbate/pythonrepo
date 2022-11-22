import dailyk as dk
import index as idx
from datetime import date
import pandas as pd
import teststrategy as ts
import smastrategy as ss
import testsignal

# testsignal.runTest(dataframe=dk.getDailyK('sh', '601888', '2020-01-01', '2020-12-31'))
# ss.runstrat(dataframe=dk.getDailyK('sh', '601888', '2020-01-01', '2020-12-31'))
# ts.runTest(dataframe = dk.getDailyK('sh', '601888', '2020-01-01'))

# idx.updateAllIndexes()
dk.syncExistingStocks()

# idx.updateIndex('sz50')
# print(info.getInfo('sh', '600240'))

# print(type(info.getIpoDate('sh', '600298')))
