import sys
from pathlib import Path
import json
import numpy as np
import pandas as pd
import datetime


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


############ 3. Slope class and helper functions if needed. You can refactor/rewrite if needed: ############
class Slope:
    def __init__(self, curr_pos_slope, curr_neg_slope, max_pos_slope, max_neg_slope, max_slope_time, max_slope_extreme_price, max_slope_bid_volume_before, max_slope_bid_volume_during, 
                 max_slope_bid_volume_after, max_slope_ask_volume_before, max_slope_ask_volume_during, max_slope_ask_volume_after):
        self.curr_pos_slope = curr_pos_slope
        self.curr_neg_slope = curr_neg_slope
        self.max_pos_slope = max_pos_slope
        self.max_neg_slope = max_neg_slope
        self.max_slope_time = max_slope_time
        self.max_slope_extreme_price = max_slope_extreme_price
        self.max_slope_bid_volume_before = max_slope_bid_volume_before
        self.max_slope_bid_volume_during = max_slope_bid_volume_during
        self.max_slope_bid_volume_after = max_slope_bid_volume_after
        self.max_slope_ask_volume_before = max_slope_ask_volume_before
        self.max_slope_ask_volume_during = max_slope_ask_volume_during
        self.max_slope_ask_volume_after = max_slope_ask_volume_after
        
def calc_slope_metrics(df, idx, position_size, slope):
    if position_size == 0:
        return

    slope.curr_pos_slope = df['High'].iloc[idx] - df['Low'].iloc[idx-10]
    slope.curr_neg_slope = df['Low'].iloc[idx] - df['High'].iloc[idx-10]

    # for longs:
    if position_size > 0:
        if slope.curr_pos_slope > slope.max_pos_slope:
            slope.max_pos_slope = slope.curr_pos_slope
            slope.max_neg_slope = 0
            slope.max_slope_time = df.index[idx]
            slope.max_slope_extreme_price = df['High'].iloc[idx]
            slope.max_slope_bid_volume_before = df['BidVolume'].iloc[idx - 1]
            slope.max_slope_bid_volume_during = df['BidVolume'].iloc[idx]
            slope.max_slope_bid_volume_after = df['BidVolume'].iloc[idx + 1]
            slope.max_slope_ask_volume_before = df['AskVolume'].iloc[idx - 1]
            slope.max_slope_ask_volume_during = df['AskVolume'].iloc[idx]
            slope.max_slope_ask_volume_after = df['AskVolume'].iloc[idx + 1]
    
    # for shorts
    elif position_size < 0:
        if slope.curr_neg_slope < slope.max_neg_slope:
            slope.max_pos_slope = 0
            slope.max_neg_slope = slope.curr_neg_slope
            slope.max_slope_time = df.index[idx]
            slope.max_slope_extreme_price = df['Low'].iloc[idx]
            slope.max_slope_bid_volume_before = df['BidVolume'].iloc[idx - 1]
            slope.max_slope_bid_volume_during = df['BidVolume'].iloc[idx]
            slope.max_slope_bid_volume_after = df['BidVolume'].iloc[idx + 1]
            slope.max_slope_ask_volume_before = df['AskVolume'].iloc[idx - 1]
            slope.max_slope_ask_volume_during = df['AskVolume'].iloc[idx]
            slope.max_slope_ask_volume_after = df['AskVolume'].iloc[idx + 1]


############ 4. long and short entry functions. You can refactor/rewrite if needed: ############
def enter_long_position(price, entry_time, stop_loss_price_incoming, trades, slope, entry_slope):
    global entry_price, stop_loss_price, target_price, peak
    global isLiqGrabRangeWithinFlushRange, flushRange, grabRange, grabResponseExceededFlushStartPrice
    position_size = 1
    pnl = 0
    peak = -np.inf
    entry_price = price
    stop_loss_price = stop_loss_price_incoming # price - stop_loss
    target_price = price + target
    slope = Slope(-np.inf,np.inf,-np.inf,np.inf,0,0,0,0,0,0,0,0)
    trades.append(('Long', entry_time, 0, entry_price, 0, entry_slope, 0, 0, 0, 0, 
                   isLiqGrabRangeWithinFlushRange, flushRange, grabRange, grabResponseExceededFlushStartPrice,
                   slope.max_pos_slope, slope.max_slope_time, slope.max_slope_extreme_price, slope.max_slope_bid_volume_before, slope.max_slope_bid_volume_during,
                   slope.max_slope_bid_volume_after, slope.max_slope_ask_volume_before, slope.max_slope_ask_volume_during, slope.max_slope_ask_volume_after))
    return trades, position_size, 0, 0, 0 # also returning mae, mfe, drawdown

def enter_short_position(price, entry_time, stop_loss_price_incoming, trades, slope, entry_slope):
    global entry_price, stop_loss_price, target_price, pnl, peak, trough
    global isLiqGrabRangeWithinFlushRange, flushRange, grabRange, grabResponseExceededFlushStartPrice
    position_size = -1
    pnl = 0
    trough = np.inf
    entry_price = price
    stop_loss_price = stop_loss_price_incoming # price - stop_loss
    target_price = price - target
    slope = Slope(-np.inf,np.inf,-np.inf,np.inf,0,0,0,0,0,0,0,0)
    trades.append(('Short', entry_time, 0, entry_price, 0, entry_slope, 0, 0, 0, 0, 
                   isLiqGrabRangeWithinFlushRange, flushRange, grabRange, grabResponseExceededFlushStartPrice,
                   slope.max_neg_slope, slope.max_slope_time, slope.max_slope_extreme_price, slope.max_slope_bid_volume_before, slope.max_slope_bid_volume_during,
                   slope.max_slope_bid_volume_after, slope.max_slope_ask_volume_before, slope.max_slope_ask_volume_during, slope.max_slope_ask_volume_after))
    return trades, position_size, 0, 0, 0 # also returning mae, mfe, drawdown


############ 5. MAE, MFE, Drawdown calculations: ############
def calc_mae_mfe_drawdown(high_price, low_price, position_size, mae, mfe, drawdown):
    global peak, trough
    
    # for longs:
    if position_size > 0:
        # calc drawdown
        if high_price > peak:
                peak = high_price
        else:
            dd_local = peak - low_price
            if dd_local > drawdown:
                drawdown = dd_local
        trade_profit = (high_price - entry_price)
        if trade_profit > 0:
            mfe = max(mfe, trade_profit)
        trade_loss = (low_price - entry_price)
        if trade_loss < 0:
            mae = min(mae, trade_loss)
    
    # for shorts
    elif position_size < 0:
        # calc drawdown
        if low_price < trough:
                trough = low_price
        else:
            dd_local = high_price - trough
            if dd_local > drawdown:
                drawdown = dd_local
        trade_profit = (entry_price - low_price)
        if trade_profit > 0:
            mfe = max(mfe, trade_profit)
        trade_loss = (entry_price - high_price)
        if trade_loss < 0:
            mae = min(mae, trade_loss)
    return mae, mfe, drawdown



############ 6. helper function ############
def isLiqGrabRangeWithinFlushRangeFn(df, idx):
    global isLiqGrabRangeWithinFlushRange, flushRange, grabRange
    if df['Low'].iloc[idx] <= df['Low'].iloc[idx+1] and \
        df['Low'].iloc[idx] <= df['Low'].iloc[idx+2] and \
        df['High'].iloc[idx] >= df['High'].iloc[idx+1] and \
        df['High'].iloc[idx] >= df['High'].iloc[idx+2]:
        isLiqGrabRangeWithinFlushRange = True
    else:
        isLiqGrabRangeWithinFlushRange = False



############ 7. runAlgo() ############
def runAlgo(df, target, stop_loss, be_offset, trades):
    global entry_price, stop_loss_price, target_price, pnl, peak
    global isLiqGrabRangeWithinFlushRange, flushRange, grabRange, grabResponseExceededFlushStartPrice, exit_condition_slopeAndVolume
    global total_winning_profit, total_losing_loss
    curr_contract_pnl, num_winning_trades, num_losing_trades, position_size, mae, mfe, drawdown = 0, 0, 0, 0, 0, 0, 0
    entry_time = None
    slope = Slope(-np.inf,np.inf,-np.inf,np.inf,0,0,0,0,0,0,0,0)
    # Iterate over each row in the dataframe
    for i in range(len(df)-2):

        if(df.index[i].time() < datetime.time(8, 32)):
            continue

        if i > 10 and position_size !=0 :        
            calc_slope_metrics(df, i, position_size, slope)

        # ********** FILTERS HERE: **********
        # Check if current row meets entry conditions for long . Sample filters:
        if df['BidVolume'].iloc[i] > 100 and \
        df['BidVolume'].iloc[i+1] < 100 and \
        df['AskVolume'].iloc[i] > 100 and \
        df['AskVolume'].iloc[i+1] > 100 and \
        position_size == 0 and df['Close'].iloc[i-1] > df['Close'].iloc[i]:
            # check if liq grab bars' range is within the flush bars' range
            isLiqGrabRangeWithinFlushRangeFn(df, i)
            if df['High'].iloc[i] < max(df['High'].iloc[i+1], df['High'].iloc[i+2]):
                grabResponseExceededFlushStartPrice = True
            else: grabResponseExceededFlushStartPrice = False
            
            # Enter long position if no existing position
            if position_size == 0:
                entry_time = df.index[i]
                trades, position_size, mae, mfe, drawdown = enter_long_position(df['Close'].iloc[i+2], entry_time, df['Close'].iloc[i+2] - stop_loss, trades, slope, df['Low'].iloc[i] - df['High'].iloc[i-10])
                i = (i+2)
        
        # Check if current row meets entry conditions for short position
        elif df['AskVolume'].iloc[i] > 100 and \
        df['AskVolume'].iloc[i+1] < 100 and \
        df['BidVolume'].iloc[i] > 100 and \
        df['BidVolume'].iloc[i+1] > 100 and \
        position_size == 0 and df['Close'].iloc[i-1] < df['Close'].iloc[i]:
            # check if liq grab bars' range is within the flush bars' range
            isLiqGrabRangeWithinFlushRangeFn(df, i)
            if df['Low'].iloc[i] > min(df['Low'].iloc[i+1], df['Low'].iloc[i+2]):
                grabResponseExceededFlushStartPrice = True
            else: grabResponseExceededFlushStartPrice = False
            # Enter short position if no existing position
            if position_size == 0:
                entry_time = df.index[i]
                trades, position_size, mae, mfe, drawdown = enter_short_position(df['Close'].iloc[i+2], entry_time, df['Close'].iloc[i+2] + stop_loss, trades, slope, df['High'].iloc[i] - df['Low'].iloc[i-10])
                i = (i+2)

        
        ### position mgmt logic: ###
        if position_size != 0:
            
            mae, mfe, drawdown = calc_mae_mfe_drawdown(df['High'].iloc[i], df['Low'].iloc[i], position_size, mae, mfe, drawdown)

            # if time in position is > 1hr, then flatten
            if (df.index[i] - entry_time >= pd.Timedelta('59 minutes')):
                exit_price = df['Close'][i]
                pnl = (exit_price - entry_price)*position_size
                curr_contract_pnl += pnl
                position_size = 0
                trades[-1] = (trades[-1][0], trades[-1][1], df.index[i], trades[-1][3], exit_price, trades[-1][5], pnl, mae, mfe, drawdown,
                               isLiqGrabRangeWithinFlushRange, flushRange, grabRange, grabResponseExceededFlushStartPrice,
                               slope.max_pos_slope, slope.max_slope_time, slope.max_slope_extreme_price, slope.max_slope_bid_volume_before, slope.max_slope_bid_volume_during,
                               slope.max_slope_bid_volume_after, slope.max_slope_ask_volume_before, slope.max_slope_ask_volume_during, slope.max_slope_ask_volume_after)
                slope = Slope(-np.inf,np.inf,-np.inf,np.inf,0,0,0,0,0,0,0,0)
                if pnl < 0:
                    num_losing_trades += 1
                if pnl > 0:
                    num_winning_trades += 1

            # if time > 14:45 i.e. closing time, then flatten
            if(df.index[i].time() >= datetime.time(14, 45)):
                exit_price = df['Close'][i]
                pnl = (exit_price - entry_price)*position_size
                curr_contract_pnl += pnl
                position_size = 0
                trades[-1] = (trades[-1][0], trades[-1][1], df.index[i], trades[-1][3], exit_price, trades[-1][5], pnl, mae, mfe, drawdown,
                               isLiqGrabRangeWithinFlushRange, flushRange, grabRange, grabResponseExceededFlushStartPrice,
                               slope.max_pos_slope, slope.max_slope_time, slope.max_slope_extreme_price, slope.max_slope_bid_volume_before, slope.max_slope_bid_volume_during,
                               slope.max_slope_bid_volume_after, slope.max_slope_ask_volume_before, slope.max_slope_ask_volume_during, slope.max_slope_ask_volume_after)
                slope = Slope(-np.inf,np.inf,-np.inf,np.inf,0,0,0,0,0,0,0,0)
                if pnl < 0:
                    num_losing_trades += 1
                if pnl > 0:
                    num_winning_trades += 1


        # Check if stop loss or target price hit on long position
        if  position_size > 0:
            if df['Low'].iloc[i] <= stop_loss_price:
                exit_price = stop_loss_price
                pnl = stop_loss_price - entry_price
                curr_contract_pnl += pnl
                position_size = 0
                trades[-1] = ('Long', trades[-1][1], df.index[i], trades[-1][3], exit_price, trades[-1][5], pnl, mae, mfe, drawdown,
                               isLiqGrabRangeWithinFlushRange, flushRange, grabRange, grabResponseExceededFlushStartPrice,
                               slope.max_pos_slope, slope.max_slope_time, slope.max_slope_extreme_price, slope.max_slope_bid_volume_before, slope.max_slope_bid_volume_during,
                               slope.max_slope_bid_volume_after, slope.max_slope_ask_volume_before, slope.max_slope_ask_volume_during, slope.max_slope_ask_volume_after)
                slope = Slope(-np.inf,np.inf,-np.inf,np.inf,0,0,0,0,0,0,0,0)
                if pnl < 0:
                    num_losing_trades += 1
            elif df['High'].iloc[i] >= target_price:
                exit_price = target_price
                pnl = target
                curr_contract_pnl += pnl
                position_size = 0
                trades[-1] = ('Long', trades[-1][1], df.index[i], trades[-1][3], exit_price, trades[-1][5], pnl, mae, mfe, drawdown,
                              isLiqGrabRangeWithinFlushRange, flushRange, grabRange, grabResponseExceededFlushStartPrice,
                              slope.max_pos_slope, slope.max_slope_time, slope.max_slope_extreme_price, slope.max_slope_bid_volume_before, slope.max_slope_bid_volume_during,
                              slope.max_slope_bid_volume_after, slope.max_slope_ask_volume_before, slope.max_slope_ask_volume_during, slope.max_slope_ask_volume_after)
                slope = Slope(-np.inf,np.inf,-np.inf,np.inf,0,0,0,0,0,0,0,0)
                num_winning_trades += 1
            elif df['High'].iloc[i] >= entry_price + be_offset and stop_loss_price != entry_price:
                stop_loss_price = entry_price
        
        # Check if stop loss or target price hit on short position
        elif position_size < 0:
            if df['High'].iloc[i] >= stop_loss_price:
                exit_price = stop_loss_price
                pnl = entry_price - stop_loss_price
                curr_contract_pnl += pnl
                position_size = 0
                trades[-1] = ('Short', trades[-1][1], df.index[i], trades[-1][3], exit_price, trades[-1][5], pnl, mae, mfe, drawdown,
                              isLiqGrabRangeWithinFlushRange, flushRange, grabRange, grabResponseExceededFlushStartPrice,
                              slope.max_neg_slope, slope.max_slope_time, slope.max_slope_extreme_price, slope.max_slope_bid_volume_before, slope.max_slope_bid_volume_during,
                              slope.max_slope_bid_volume_after, slope.max_slope_ask_volume_before, slope.max_slope_ask_volume_during, slope.max_slope_ask_volume_after)
                slope = Slope(-np.inf,np.inf,-np.inf,np.inf,0,0,0,0,0,0,0,0)
                if pnl < 0:
                    num_losing_trades += 1
            elif df['Low'].iloc[i] <= target_price:
                exit_price = target_price
                pnl = target
                curr_contract_pnl += pnl
                position_size = 0
                trades[-1] = ('Short', trades[-1][1], df.index[i], trades[-1][3], exit_price, trades[-1][5], pnl, mae, mfe, drawdown,
                              isLiqGrabRangeWithinFlushRange, flushRange, grabRange, grabResponseExceededFlushStartPrice,
                              slope.max_neg_slope, slope.max_slope_time, slope.max_slope_extreme_price, slope.max_slope_bid_volume_before, slope.max_slope_bid_volume_during,
                              slope.max_slope_bid_volume_after, slope.max_slope_ask_volume_before, slope.max_slope_ask_volume_during, slope.max_slope_ask_volume_after)
                slope = Slope(-np.inf,np.inf,-np.inf,np.inf,0,0,0,0,0,0,0,0)
                num_winning_trades += 1
            elif df['Low'].iloc[i] <= entry_price - be_offset and stop_loss_price != entry_price:
                stop_loss_price = entry_price
    return curr_contract_pnl, trades


contracts = ["M24"]

contracts_dfs = []
for contract in contracts:
    print('reading scid file:' + str(contract))
    contract_df_tmp = scidToDfAndResampleHelper(contract)
    contracts_dfs.append(contract_df_tmp)

total_profit = 0
i = 10

### this part needs to be refactored ###
while i >= 10: # target
    total_profit = 0
    j = 3.25
    while j >= 3.25: # stop loss
        total_pnl = 0
        num_winning_trades = 0
        num_losing_trades = 0
        target = i
        stop_loss = j
        be_offset = j

        # Get the current date and time
        current_datetime = datetime.datetime.now()
        # Format the date and time as desired
        filename = ("M24-target" + str(target) + "-stoploss" + str(stop_loss) + "-"
        + current_datetime.strftime("%d%b%Y-%I%p") + ".csv")

        with open(filename, 'w') as f:
            # f.write('Type, Entry Time, Exit Time, Entry Price, Exit Price, PnL, Mae, Mfe, Drawdown, isLiqGrabRangeWithinFlushRange, flushRange, grabRange, grabResponseExceededFlushStartPrice \n')
            header = (
                'Type, Entry Time, Exit Time, Entry Price, Exit Price, Entry Slope, PnL, Mae, Mfe, Drawdown, '
                'isLiqGrabRangeWithinFlushRange, flushRange, grabRange, grabResponseExceededFlushStartPrice, '
                'ExtremeSlopeValue, MaxSlopeTime, MaxSlopeExtremePrice, BidVolumeBeforeMaxSlope, BidVolumeDuringMaxSlope, '
                'BidVolumeAfterMaxSlope, AskVolumeBeforeMaxSlope, AskVolumeDuringMaxSlope, AskVolumeAfterMaxSlope '
                )
            f.write(header + "\n")
            for contract_df in contracts_dfs:
                trades = []
                print('calling runAlgo() with target:' + str(target) + ' and stoploss:' + str(stop_loss))
                curr_contract_pnl, trades = runAlgo(contract_df, target, stop_loss, be_offset, trades)

                total_pnl += curr_contract_pnl
                print('Current contract PnL: ' + str(curr_contract_pnl))
                # Write trades to CSV file
                for trade in trades:
                    f.write(', '.join(str(x) for x in trade) + '\n')

            f.write('Total PnL: ' + str(total_pnl))
            print('Total PnL: ' + str(total_pnl))

        j -= 0.25
    i -= 1
