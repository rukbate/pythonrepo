import dailyk as dk
import index as idx
import info
from datetime import date

# dk.syncExistingStocks()
# idx.updateAllIndexes()
# idx.updateIndex('sz50')
# print(info.getInfo('sh', '600240'))

# print(type(info.getIpoDate('sh', '600298')))
# dk.syncDailyK('sh', '600298')
data = dk.getDailyK('sh', '600298')
print(data[data["date"] > date(2021, 5, 31)])
# print(idx.getIndex('zz500'))