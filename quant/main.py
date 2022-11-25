from repository.data import index as dx
from repository.data import dailyk as dk
from repository.data import basic as bc
from repository.data import profit as pf
from market import market

# testsignal.runTest(dataframe=dk.retrieve_daily_k('sh', '601888', '2020-01-01', '2020-12-31'))
# ss.runstrat(dataframe=dk.retrieve_daily_k('sh', '601888', '2020-01-01', '2020-12-31'))
# ts.runTest(dataframe = dk.retrieve_daily_k('sh', '601888', '2020-01-01'))

# dx.update_indexes()
# dk.sync_all_stocks()
dk.sync_index_stocks()

# pf.persist_profit(market.profit('sh.601888', 2022, 1))
# pf.update_all_profit()
# pf.update_profit('sh.600139')
# print(bc.ipo_date('sh.600139'))

