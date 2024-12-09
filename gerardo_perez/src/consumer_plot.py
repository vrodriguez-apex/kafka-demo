import sys
from src.consumer_functions import create_consumer, handling_message
from itertools import count
import matplotlib.pyplot as plt

topic = 'BTCUSDT_avgPrice'
consumer = create_consumer(topic, offset_reset='earliest')
index = count()
y = []
x = []

try:
    for message in consumer:
        json_value = handling_message(message)
        print(json_value['price'])
        x.append(next(index))
        y.append(float(json_value['price']))
        plt.plot(x, y)
        plt.show(block=False)
        plt.pause(0.5)
        plt.clf()
        plt.cla()

except KeyboardInterrupt:
    print('Cerrando consumer...')
    sys.exit()
