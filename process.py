import torch
import json
import numpy as np
import time
device_gpu = torch.device("cuda")

def to_tensor(l):
    return torch.tensor(l, dtype = torch.float32, device=device_gpu)

def process_message(data: dict, symbol: str):
    start = time.time()
    #bids_vol, asks_vol = ask_bid_amount(data)
    wvap_asks, wvap_bids = wvap(data)
    buy_simulation(orderbook=data, investment = 10_000_000)
    end = time.time()
    #print(f'symbol:{symbol}, new_bids_amount:{bids_vol}, new_asks_amount:{asks_vol}', end = '\r')
    print(f'symbol:{symbol}, wvap_bids:{wvap_bids}, wvap_asks:{wvap_asks}', end = '\t')
    print(f'processed in {end-start:.6f} seconds', end = '\t')
    return

def clean(d):
    for key, value, in d.items():
        if value == 0:
            d.remove(key)
    return d

def ask_bid_amount(orderbook):
    bids_vol = torch.dot(to_tensor(list(orderbook['bids'].keys())), to_tensor(list(orderbook['bids'].values())))
    asks_vol = torch.dot(to_tensor(list(orderbook['asks'].keys())), to_tensor(list(orderbook['asks'].values())))
    return asks_vol, bids_vol

def wvap(orderbook):
    ask_prices = to_tensor(list(orderbook['asks'].keys()))
    ask_quantities = to_tensor(list(orderbook['asks'].values()))
    
    bid_prices = to_tensor(list(orderbook['bids'].keys()))
    bid_quantities = to_tensor(list(orderbook['bids'].values()))
    
    total_value_asks = torch.dot(ask_prices, ask_quantities)
    total_quantity_asks = torch.sum(ask_quantities)
    
    vwap_asks = total_value_asks / total_quantity_asks if total_quantity_asks != 0 else 0
    
    total_value_bids = torch.dot(bid_prices, bid_quantities)
    total_quantity_bids = torch.sum(bid_quantities)
    
    vwap_bids = total_value_bids / total_quantity_bids if total_quantity_bids != 0 else 0
    
    return vwap_asks.item(), vwap_bids.item()

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


def buy_simulation(orderbook, investment):
    all_orders = sorted(orderbook['asks'].items())
    
    remaining_investment = investment
    total_shares = 0

    for price, quantity in all_orders:
        amount = price * quantity
        
        if remaining_investment <= amount:
            remaining_quantity = remaining_investment / price
            total_shares += remaining_quantity
            print(f'Total shares bought= {total_shares}, price is raised to {price}')
            # orderbook['asks'][price] -= remaining_quantity
            # if orderbook['asks'][price] == 0:
            #     del orderbook['asks'][price]
            return price, total_shares
        

        # del orderbook['asks'][price]
        remaining_investment -= amount
        total_shares += quantity
    return

def sell_simulation(orderbook, sell_quantity):
    sorted_bids = sorted(orderbook['bids'].items(), reverse=True)
    
    remaining_quantity = sell_quantity
    total_amount = 0

    for price, quantity in sorted_bids:
        if remaining_quantity <= quantity:

            total_amount += remaining_quantity * price
            # orderbook['bids'][price] -= remaining_quantity
            # if orderbook['bids'][price] == 0:
            #     del orderbook['bids'][price]
            print(f'Total amount received: {total_amount}, the price is reduced to {price}')
            return price, total_amount, orderbook
        
        remaining_quantity -= quantity
        total_amount += quantity * price
        # del orderbook['bids'][price]

    return





