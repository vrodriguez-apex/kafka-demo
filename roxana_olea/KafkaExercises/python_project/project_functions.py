import numpy as np

def strategy(data):
    data['short_ma'] = data['close'].rolling(window=10).mean()
    data['long_ma'] = data['close'].rolling(window=50).mean()
    data['signal'] = 0
    data['signal'][10:] = np.where(data['short_ma'][10:] > data['long_ma'][10:], 1, 0)
    data['position'] = data['signal'].diff()
    return data