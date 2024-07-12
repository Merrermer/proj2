import numpy as np

def message_to_orderbookd(data):
    b = data.get('bids')
    a = data.get('asks')
    if b is None or a is None:
        print('Error')
        return
    d = dict()
    d['asks'] = dict(np.array(a, dtype=float))
    d['bids'] = dict(np.array(b, dtype=float))
    return d

def update_orderbook(orderbook, data):
    a = dict(np.array(data.get('a'), dtype=float))
    b = dict(np.array(data.get('b'), dtype=float))
    orderbook['asks'].update(a)
    orderbook['bids'].update(b)
    orderbook = clean(orderbook)
    return orderbook

def clean(orderbook):
    d = orderbook.copy()
    d['asks'] = {k: v for k, v in orderbook['asks'].items() if v != 0}
    d['bids'] = {k: v for k, v in orderbook['bids'].items() if v != 0}
    print('cleaned')
    return d