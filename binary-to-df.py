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
    df_tmp = pd.DataFrame(data=np.memmap(f, dtype=rectype, offset=offset, mode="r"), copy=False)
    df_tmp.dropna(inplace=True)
    df_tmp["Time"] = df_tmp["Time"] - 2209161600000000
    df_tmp.drop(df_tmp[(df_tmp.Time < 1) | (df_tmp.Time > 2705466561000000)].index, inplace=True)
    df_tmp.set_index("Time", inplace=True)
    df_tmp.index = pd.to_datetime(df_tmp.index, unit='us')
    df_tmp.index = df_tmp.index.tz_localize(tz="utc")
    df_tmp.index = df_tmp.index.tz_convert('America/Chicago')
    return df_tmp



############ 2. code to resample df to desired timeframe ############
startTime = pd.Timestamp("08:00:00").time()
endTime = pd.Timestamp("14:59:59").time()

def scidToDfAndResampleHelper():
    df_tmp = get_scid_df('F.US.EPM24.scid')
    index_time = df_tmp.index.time
    mask = (index_time >= startTime) & (index_time <= endTime)
    df_tmp = df_tmp.loc[mask]

    mask = None

    # You can then use pandas resample to get any timeframe. for example, for 30 minutes:
    # df_tmp = (
    #     df_tmp.resample("30MIN")
    #     .agg(
    #         {
    #             "Open": "first",
    #             "High": "max",
    #             "Low": "min",
    #             "Close": "last",
    #             "Volume": "sum",
    #             "Trades": "sum",
    #             "BidVolume": "sum",
    #             "AskVolume": "sum",
    #         }
    #     )
    #     .assign(Delta=lambda x: x["AskVolume"] - x["BidVolume"])  # add new column
    #     # .ffill()
    # )
    # or for 1 second:
    df_tmp = (
        df_tmp.resample("1S")
        .agg(
            {
                "Open": "first",
                "High": "max",
                "Low": "min",
                "Close": "last",
                "Volume": "sum",
                "Trades": "sum",
                "BidVolume": "sum",
                "AskVolume": "sum",
            }
        )
        .assign(Delta=lambda x: x["AskVolume"] - x["BidVolume"])  # add new column
        # .ffill()
    )
    df_tmp['Open'] = df_tmp['Close'].shift(1)
    df_tmp.dropna(inplace=True)
    
    return df_tmp