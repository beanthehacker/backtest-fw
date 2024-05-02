import sys
import pandas as pd
from scidReader import get_scid_df
from contract_dates_constants import CONTRACT_START_DATES, CONTRACT_END_DATES

def scidToDfAndResampleHelper(contract, startTime=pd.Timestamp("04:30:00").time(), endTime=pd.Timestamp("10:01:00").time(), resample="1S", limitsize=sys.maxsize):
    df_tmp = get_scid_df('F.US.EP'+ contract + '.scid', limitsize)
    index_time = df_tmp.index.time
    mask = (index_time >= startTime) & (index_time <= endTime)
    df_tmp = df_tmp.loc[mask]
    mask = None

    startDate = pd.Timestamp(CONTRACT_START_DATES.get(contract)).date()
    endDate = pd.Timestamp(CONTRACT_END_DATES.get(contract)).date()
    index_date = df_tmp.index.date
    mask = (index_date >= startDate) & (index_date <= endDate)
    df_tmp = df_tmp.loc[mask]
    mask = None

    if resample == "None":
       return df_tmp

    elif resample:
      df_tmp = (
          df_tmp.resample(resample)
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
    # print(df_tmp)
    return df_tmp


def scidToDfAndResampleHelperWithoutTimeFilter(contract, resample="1S", limitsize=sys.maxsize):
    df_tmp = get_scid_df('D:\\SierraChart-daytrading\\Data\\F.US.EP'+ contract + '.scid', limitsize)

    endTime=pd.Timestamp("10:00:00").time()
    mask = df_tmp.index.time <= endTime
    df_tmp = df_tmp.loc[mask]
    mask = None

    startDate = pd.Timestamp(CONTRACT_START_DATES.get(contract)).date()
    endDate = pd.Timestamp(CONTRACT_END_DATES.get(contract)).date()
    index_date = df_tmp.index.date
    mask = (index_date >= startDate) & (index_date <= endDate)
    df_tmp = df_tmp.loc[mask]
    mask = None

    if resample == "None":
       return df_tmp

    elif resample:
      df_tmp = (
          df_tmp.resample(resample)
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
    # print(df_tmp)
    return df_tmp


def dfResampleHelper(df, resample):
    endTime=pd.Timestamp("10:00:00").time()
    mask = df_tmp.index.time <= endTime
    df_tmp = df_tmp.loc[mask]
    mask = None
    
    if resample:
      df_tmp = (
          df_tmp.resample(resample)
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
    # print(df_tmp)
    return df_tmp