import pandas as pd

dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%fZ')

data = pd.read_csv('BITMEX_SPOT_BTC_USD_1MIN.csv',index_col=['Date'], parse_dates=['Date'], date_parser=dateparse)
data = data.resample('900S',how={'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last',
                   'Volume': 'sum'})
data.to_csv('BITMEX_SPOT_BTC_USD_15MIN14.csv',date_format='%Y-%m-%dT%H:%M:%S.%fZ')