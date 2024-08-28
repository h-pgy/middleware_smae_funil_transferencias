import time
import pandas as pd

def agora_unix_timestamp():

    return int(time.time())

def dmy_series_to_datetime(series:pd.Series)->pd.Series:
    
    #erros serao colocados como NaN
    return pd.to_datetime(series, format=r'%d/%m/%Y', errors='coerce')