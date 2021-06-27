import dailyk as dk
import index as idx
import info

# dk.syncExistingStocks()
idx.updateAllIndexes()
# idx.updateIndex('sz50')
# print(info.getInfo('sh', '600240'))

# print(type(info.getIpoDate('sh', '600298')))
dk.syncDailyK('sh', '600298')