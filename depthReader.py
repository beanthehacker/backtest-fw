import struct
from datetime import datetime, timedelta
import pandas as pd

def convert_to_datetime(microseconds_since_epoch):
    epoch_start = datetime(1899, 12, 30)  # SCDateTime epoch start
    # return epoch_start + timedelta(microseconds=microseconds_since_epoch)
    timestamp = epoch_start + timedelta(microseconds=microseconds_since_epoch)
    return pd.Timestamp(timestamp).tz_localize('UTC').tz_convert('America/Chicago')

def read_depth_file(file_path):
    rows = []
    with open(file_path, 'rb') as input_file:
        header = input_file.read(64)
        headersrc = struct.unpack('4I48s', header)
        print(headersrc)
        while True:
            d = {}
            tick = input_file.read(24)
            if not tick:
                break
            src = struct.unpack('qbbhfII', tick)
            d['datetime'] = convert_to_datetime(src[0])
            d['command'] = src[1]
            d['flag'] = src[2]
            d['orders'] = src[3]
            d['price'] = src[4]
            d['quantity'] = src[5]
            rows.append(d)
    return rows
file_path = 'F.US.EPM24.2024-04-16.depth'
rows = read_depth_file(file_path)

print(rows[0])
print(rows[1])
print(rows[2])

# for row in rows:
#   print(row)
