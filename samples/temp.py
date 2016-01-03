import logging
import pandas as pd
pd.set_option('max_rows', 10)
pd.set_option('expand_frame_repr', False)
pd.set_option('max_columns', 6)

from arctic_updater.updaters.truefx import TrueFXUpdater
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

my_updater = TrueFXUpdater()
symbol = 'EURUSD'
year, month = 2015, 11
%time df = my_updater._read_one_month(symbol, year, month)

# Arctic (MongoDB)
from arctic import Arctic
store = Arctic('localhost')
library_name = 'test'
store.initialize_library(library_name)
library = store[library_name]

%time library.write(symbol, df)
%time df_retrieved = library.read(symbol).data

# HDF5
filename = my_updater._filename(symbol, year, month, '.h5')
%time df.to_hdf(filename, "data", mode='w', complevel=0, complib='zlib', format='table')
%time df_retrieved = pd.read_hdf(filename)

# Make unique index
# http://stackoverflow.com/questions/34575126/create-a-dataframe-with-datetimeindex-with-unique-values-by-adding-a-timedelta/34576154#34576154
df = df.reset_index()

%time df['us'] =  (df['Date'].groupby((df['Date'] != df['Date'].shift()).cumsum()).cumcount()).values.astype('timedelta64[us]')
#or
%time df['us'] =  (df['Date'].groupby((df['Date'].diff() != pd.to_timedelta(0)).cumsum()).cumcount()).values.astype('timedelta64[us]')

df['Date'] = df['Date'] + df['us']

