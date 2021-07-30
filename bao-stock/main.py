import dailyk as dk
import index as idx
from datetime import date
import pandas as pd
import teststrategy as ts

ts.runTest(dataframe = dk.getDailyK('sh', '601888', '2015-01-01'))

# dk.syncExistingStocks()
# idx.updateAllIndexes()
# idx.updateIndex('sz50')
# print(info.getInfo('sh', '600240'))

# print(type(info.getIpoDate('sh', '600298')))
