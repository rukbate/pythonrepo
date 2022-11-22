from repository import index as dx
from repository import dailyk as dk

# testsignal.runTest(dataframe=dk.retrieve_daily_k('sh', '601888', '2020-01-01', '2020-12-31'))
# ss.runstrat(dataframe=dk.retrieve_daily_k('sh', '601888', '2020-01-01', '2020-12-31'))
# ts.runTest(dataframe = dk.retrieve_daily_k('sh', '601888', '2020-01-01'))

dx.update_indexes()
dk.sync_all_stocks()
