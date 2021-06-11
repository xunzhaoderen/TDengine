import datetime
your_timestamp = 1623254400123456789
date = datetime.datetime.fromtimestamp(your_timestamp / 1e9)
print(date)