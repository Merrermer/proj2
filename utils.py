import torch
import json
import numpy as np
import time
import threading

device_gpu = torch.device("cuda")

def to_tensor(l):
    return torch.tensor(l, dtype = torch.float32, device=device_gpu)

def process_message(data: dict, symbol: str):
    start = time.time()
    m_0 = str(f'symbol: {symbol},')
    vwap_asks, vwap_bids, m_1 = vwap(data)
    price, total_shares, m_2 = buy_simulation(orderbook=data, investment = 1_000_000)
    end = time.time()
    print(m_0+m_1+m_2+f'processed in {end-start:.6f} seconds')
    return

def ask_bid_amount(orderbook):
    bids_vol = torch.dot(to_tensor(list(orderbook['bids'].keys())), to_tensor(list(orderbook['bids'].values()))) 
    asks_vol = torch.dot(to_tensor(list(orderbook['asks'].keys())), to_tensor(list(orderbook['asks'].values())))
    logging = str(f'new_bids_amount:{bids_vol}, new_asks_amount:{asks_vol}, ')
    return asks_vol, bids_vol, logging

def vwap(orderbook):
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
    
    logging = str(f'wvap_bids:{vwap_bids}, wvap_asks:{vwap_asks},')
    return vwap_asks, vwap_bids, logging


def buy_simulation(orderbook, investment):
    all_orders = sorted(orderbook['asks'].items())
    
    remaining_investment = investment
    total_shares = 0

    for price, quantity in all_orders:
        amount = price * quantity
        
        if remaining_investment <= amount:
            remaining_quantity = remaining_investment / price
            total_shares += remaining_quantity
            logging = str(f'Total shares bought: {total_shares}, the price is raised to {price}. ')

            orderbook['asks'][price] -= remaining_quantity # this part is to apply your simulated buy order to the local orderbook 
            if orderbook['asks'][price] == 0:
                del orderbook['asks'][price]

            return price, total_shares, logging
        
        del orderbook['asks'][price]
        remaining_investment -= amount
        total_shares += quantity
    logging = str(f'You have traded all the buy orders in the order book, and the highest transaction price is {price}. ')
    return price, total_shares, logging

def sell_simulation(orderbook, sell_quantity):
    sorted_bids = sorted(orderbook['bids'].items(), reverse=True)
    
    remaining_quantity = sell_quantity
    total_amount = 0

    for price, quantity in sorted_bids:
        if remaining_quantity <= quantity:
            total_amount += remaining_quantity * price

            orderbook['bids'][price] -= remaining_quantity
            if orderbook['bids'][price] == 0:
                del orderbook['bids'][price] # this part is to apply your simulated sell order to the local orderbook 
            logging = str(f'Total amount received: {total_amount}, the price is reduced to {price}. ')
            return price, total_amount, orderbook, logging
        
        remaining_quantity -= quantity
        total_amount += quantity * price
        del orderbook['bids'][price]
    logging = str(f'You have traded all the sell orders in the order book, and the lowest transaction price is {price}. ')
    return price, total_amount, orderbook, logging