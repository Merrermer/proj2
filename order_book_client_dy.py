import time
import requests
import websocket
import threading
from typing import List, Dict
import queue
import json
from typing import List, Callable
import utils
import dictionarize
import multiprocessing


class OrderBookClient:
    def __init__(self, symbols: List[str], message_callback: Callable[[str], None]):
        self.__base_uri = "wss://stream.binance.com:9443/ws"
        self.__symbols = symbols
        self.__message_callback = message_callback
        self.__ws = websocket.WebSocketApp(self.__base_uri,
                                           on_message=self.__on_message,
                                           on_error=self.__on_error,
                                           on_close=self.__on_close)
        self.__ws.on_open = self.__on_open
        self.__lookup_snapshot_id: Dict[str, int] = dict()
        self.__lookup_update_id: Dict[str, int] = dict()
        self.__orderbooks = dict()
        self.__closed = False
        self.__lock = threading.Lock()

        self.__message_queue = queue.Queue()
        self.__message_thread = threading.Thread(target=self.__process_messages)
        self.__message_thread.daemon = True

    def __connect(self):
        self.__message_thread.start()
        self.__ws.run_forever()
        self.__closed = True

    def __on_message(self, _ws, message):
        self.__message_queue.put(message)

    def __process_messages(self):
        while not self.__closed:
            try:
                message = self.__message_queue.get(timeout=1)
                self.__handle_message(message)
            except queue.Empty:
                continue

    def __handle_message(self, message):
        __s = time.time()
        data = json.loads(message)
        update_id_low = data.get("U")
        update_id_upp = data.get("u")


        if update_id_low is None:       #the first message contains no information, so we just skip and return nothing
            __e = time.time()
            print(f'handletime:{__e-__s:8f}')
            return
        
        
        symbol = data.get("s")
        snapshot_id = self.__lookup_snapshot_id.get(symbol)
        if snapshot_id is None: #If we never got a snapshot of the orderbook, we request one
            d = self.get_snapshot(symbol)
            self.__orderbooks[symbol] = dictionarize.message_to_orderbookd(d)
            self.__message_callback(self.__orderbooks[symbol], symbol) # do computations based on the orderbook
            __e = time.time()
            print(f'handletime:{__e-__s:8f}')
            return 
        elif update_id_upp < snapshot_id: # The snapshot is newer than the latest message, which means some messages must be obmitted.
            return
        
        self.__lock.acquire()
        self.__orderbooks[symbol] = dictionarize.update_orderbook(self.__orderbooks[symbol], data) # update the latest diff message on orderbook
        self.__message_callback(self.__orderbooks[symbol], symbol)
        __e = time.time()
        print(f'handletime:{__e-__s:8f}')
        self.__lock.release()

        prev_update_id = self.__lookup_update_id.get(symbol)
        
        if prev_update_id is None: # assert the messages are continuous
            assert update_id_low <= snapshot_id <= update_id_upp +1
        else:
            assert update_id_low == prev_update_id + 1

        self.__lookup_update_id[symbol] = update_id_upp # update the latest id

    def __on_error(self, _ws, error):
        print(f"Encountered error: {error}")

    def __on_close(self, _ws, _close_status_code, _close_msg):
        print("Connection closed")

    def __on_open(self, _ws):
        print("Connection opened")
        for symbol in self.__symbols:
            subscribe_message = {
                "method": "SUBSCRIBE",
                "params": [
                    f"{symbol.lower()}@depth"
                ],
                "id": 1
            }
            _ws.send(json.dumps(subscribe_message))

    def __log_message(self, msg: str) -> None:
        print(msg)

    def get_snapshot(self, symbol: str):
        snapshot_url = f"https://api.binance.com/api/v3/depth?symbol={symbol}&limit=5000"
        x = requests.get(snapshot_url)
        content = x.content.decode("utf-8")
        data = json.loads(content)
        self.__lookup_snapshot_id[symbol] = data["lastUpdateId"]
        #self.__log_message(content)
        return data

    def get_snapshot_trades(self, symbol: str):
        snapshot_url = f"https://api.binance.com/api/v3/trades?symbol={symbol}&limit=1000"
        x = requests.get(snapshot_url)
        content = x.content.decode("utf-8")
        data = json.loads(content)
        #self.__log_message(content)
        return content

    def start(self) -> bool:
        self.__connect()
        return True

    def stop(self) -> bool:
        self.__ws.close()
        return True


def main():
    symbols = ["BTCUSDT"]
    orderbook_client = OrderBookClient(symbols, utils.process_message)
    orderbook_client.start()

def orderbook_timer(time, symbol):
    __orderbook_client = OrderBookClient([symbol], utils.process_message)
    __ws_thread = threading.Thread(target=__orderbook_client.start)
    __ws_thread.start()
    __timer = threading.Timer(time, __orderbook_client.stop)
    __timer.start()
    return


if __name__ == '__main__':
    #symbols = ['BTCUSDT']
    symbols = ["BTCUSDT", "DOGEUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "PEPEUSDT", "NOTUSDT", "WIFUSDT"]
    from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
    import threading
    with ThreadPoolExecutor(max_workers=len(symbols)) as executor:
        futures = {executor.submit(orderbook_timer, 15, symbol): symbol for symbol in symbols}
