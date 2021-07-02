import dailyk as dk
import index as idx
import info
from datetime import date

dk.syncExistingStocks()
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
