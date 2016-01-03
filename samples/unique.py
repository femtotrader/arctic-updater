import time
import logging
from collections import OrderedDict
import pandas as pd
pd.set_option('max_rows', 10)
pd.set_option('expand_frame_repr', False)
pd.set_option('max_columns', 6)

from arctic_updater.updaters.truefx import TrueFXUpdater
logging.Formatter.converter = time.gmtime
logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)
logger = logging.getLogger(__name__)

my_updater = TrueFXUpdater()

symbols = ['EURGBP', 'EURUSD', 'USDCHF', 'USDJPY']
d_df = OrderedDict()

year, month = 2015, 11

#for symbol in symbols:
#    filename = my_updater._filename(symbol, year, month, '.h5')
#    df = pd.read_hdf(filename, "data")
#    assert df.index.is_unique

for symbol in symbols:
    logger.info(symbol)
    df = my_updater._read_one_month(symbol, year, month)
    
    logger.info("build unique index")
    df = df.sort_index()
    df = df.reset_index()
    df['us'] = (df['Date'].groupby((df['Date'].diff() != pd.to_timedelta(0)).cumsum()).cumcount()).values.astype('timedelta64[us]')
    df['Date'] = df['Date'] + df['us']
    #remove helper column
    df = df.drop(['us', 'Symbol'], axis=1)
    #set column Date as index
    df = df.set_index('Date', verify_integrity=True)
    
    # Save to HDF5
    filename = my_updater._filename(symbol, year, month, '.h5')
    logger.info("save to %s" % filename)
    df.to_hdf(filename, "data", mode='w', complevel=5, complib='zlib')

    d_df[symbol] = df

logger.info("concatenate")
df_all = pd.concat(d_df, axis=1)
print(df_all)
df_all = df_all.swaplevel(0, 1, axis=1)
d = {}
filename = "all-panel-%s-%04d-%2d.h5" % ('ask', year, month)
for col in ['Bid', 'Ask']:
    d[col] = df_all[col]
panel = pd.Panel.from_dict(d)
panel.to_hdf(filename, "data", mode='w', complevel=5, complib='zlib')

#filename = "all-%s-%04d-%2d.h5" % ('bid', year, month)
#logger.info("save to %s" % filename)
#df_all['Bid'].to_hdf(filename, "data", mode='w', complevel=5, complib='zlib')

#filename = "all-%s-%04d-%2d.h5" % ('ask', year, month)
#logger.info("save to %s" % filename)
#df_all['Ask'].to_hdf(filename, "data", mode='w', complevel=5, complib='zlib')
