import sys
from pathlib import Path
import numpy as np
import pandas as pd


############ 1. code to parse .scid file into py dataframe: ############
BCOLS = ['Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Trades', 'BidVolume','AskVolume']

def get_scid_df(filename, limitsize=sys.maxsize):
    f = Path(filename)
    assert f.exists(), "file not found"
    stat = f.stat()
    offset = 56 if stat.st_size < limitsize else stat.st_size - (
        (limitsize // 40) * 40)
    rectype = np.dtype([
        (BCOLS[0], '<u8'), (BCOLS[1], '<f4'), (BCOLS[2], '<f4'),
        (BCOLS[3], '<f4'), (BCOLS[4], '<f4'), (BCOLS[6], '<i4'),
        (BCOLS[5], '<i4'), (BCOLS[7], '<i4'), (BCOLS[8], '<i4')
    ])
    pd.set_option('display.max_columns', 9)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.width', 300)
    df_tmp = pd.DataFrame(data=np.memmap(f, dtype=rectype, offset=offset, mode="r"), copy=False)
    df_tmp.dropna(inplace=True)
    df_tmp["Time"] = df_tmp["Time"] - 2209161600000000
    df_tmp.drop(df_tmp[(df_tmp.Time < 1) | (df_tmp.Time > 2705466561000000)].index, inplace=True)
    df_tmp.set_index("Time", inplace=True)
    df_tmp.index = pd.to_datetime(df_tmp.index, unit='us')
    df_tmp.index = df_tmp.index.tz_localize(tz="utc")
    df_tmp.index = df_tmp.index.tz_convert('America/Chicago')
    return df_tmp