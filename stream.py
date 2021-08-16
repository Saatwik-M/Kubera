# Imports
import pandas as pd, numpy as np, sqlite3 as sql
import websocket, talib, json, telegram
from collections import deque
from datetime import datetime, timedelta
from binance.client import Client
import config

# Initialise database
con = sqlite3.connect('kubera.db')
cur = con.cursor()
cur.execute('''CREATE TABLE IF NOT EXISTS stream (timestamp INTEGER NOT NULL PRIMARY KEY, open REAL, high REAL, low REAL, close REAL,
                            volume REAL, n_trades INTEGER, macd REAL, macdsignal REAL, macdhist REAL, rsi_slowk REAL, rsi_slowd REAL,
                            obv REAL, atr REAL, ema200 REAL, sar REAL)''')

con.commit()
con.close()

# Initialise telegram bot
telegram_bot = telegram.Bot(token=TELEGRAM_TOKEN)

# Temporary data to calculate technical indicators
buffer_data = dict(open=deque([], maxlen=200), high=deque([], maxlen=200), low=deque([], maxlen=200), close=deque([], maxlen=200), volume=deque([], maxlen=200))

# ---------Useful functions---------
# Add new data to buffer
def add_buffer_data(open, high, low, close, volume):
    global buffer_data

    buffer_data['open'].append(open)
    buffer_data['high'].append(high)
    buffer_data['low'].append(low)
    buffer_data['close'].append(close)
    buffer_data['volume'].append(volume)

# Get technical indicators
def process_data(cs):
    global buffer_data
    new_data = dict(timestamp=int(cs['t']/1000), open=float(cs['o']), high=float(cs['h']), low=float(cs['l']), close=float(cs['c']), volume=float(cs['v']), n_trades=int(cs['n']))
    add_buffer_data(new_data['open'], new_data['high'], new_data['low'], new_data['close'], new_data['volume'])
    ti = get_technical_info()
    new_data.update(dict(macd=ti[0], macdsignal=ti[1], macdhist=ti[2], rsi_slowk=ti[3], rsi_slowd=ti[4], obv=ti[5], atr=ti[6], ema200=ti[7], sar=ti[8]))
    return new_data

# Get timestamp from few hours ago
def get_timestamp(diff_in_hours):
    start_time = datetime.utcnow() - timedelta(hours=5)
    return int(datetime(start_time.year, start_time.month, start_time.day, start_time.hour, start_time.minute).timestamp())*1000

# Add previous few hours data to buffer data
interval_dict = dict(
    Client.KLINE_INTERVAL_1MINUTE = '1m', Client.KLINE_INTERVAL_3MINUTE = '3m', Client.KLINE_INTERVAL_5MINUTE = '5m',
    Client.KLINE_INTERVAL_15MINUTE = '15m', Client.KLINE_INTERVAL_30MINUTE = '30m', Client.KLINE_INTERVAL_1HOUR = '1h',
    Client.KLINE_INTERVAL_2HOUR = '2h', Client.KLINE_INTERVAL_4HOUR = '4h', Client.KLINE_INTERVAL_6HOUR = '6h',
    Client.KLINE_INTERVAL_8HOUR = '8h', Client.KLINE_INTERVAL_12HOUR = '12h', Client.KLINE_INTERVAL_1DAY = '1d',
    Client.KLINE_INTERVAL_3DAY = '3d', Client.KLINE_INTERVAL_1WEEK = '1w', Client.KLINE_INTERVAL_1MONTH = '1M')

interval_rev_dict = {v: k for k, v in interval_dict.items()}

def add_previous_data(symbol):
    client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)
    candlesticks = client.get_historical_klines(symbol.upper(), interval_rev_dict[STREAM_INTERVAL], get_timestamp(4))
    for d in candlesticks[-200:]: add_buffer_data(float(d[1]), float(d[2]), float(d[3]), float(d[4]), float(d[5]))

# Add new data to the database
def add_to_database(d):
    cur = con.cursor()
    cur.execute(f'''INSERT INTO stream VALUES ({d['timestamp']}, {d['open']}, {d['high']}, {d['low']}, {d['close']}, {d['volume']}, {d['n_trades']},
                                                 {d['macd']}, {d['macdsignal']}, {d['macdhist']}, {d['rsi_slowk']}, {d['rsi_slowd']},
                                                 {d['obv']}, {d['atr']}, {d['ema200']}, {d['sar']})''')
    con.commit()
    con.close()

# ---------Socket stream functions---------
# On connection to the socket add previous data to buffer
def on_open(ws):
    add_previous_data(symbol)
    msg = '>>>>> STARTED <<<<<''
    print(msg, '\n')
    telegram_bot.send_message(text=msg, chat_id=LOGS_GROUP_CHATID)

# On connection close
def on_close(ws, close_status_code, close_msg):
    msg = '>>>>> CLOSED <<<<<''
    print(msg, '\n')
    telegram_bot.send_message(text=msg, chat_id=LOGS_GROUP_CHATID)

# Print error
def on_error(ws, error):
    msg = f'>>>>> ERROR: {error}'
    print(msg, '\n')
    telegram_bot.send_message(text=msg, chat_id=ERRORS_GROUP_CHATID)

# On receiving data process and add to the database
def on_message(ws, message):
    message = json.loads(message)
    cs = message['k']
    if cs['x']:
        new_data = process_data(cs)
        add_to_database(new_data)
        msg = f'>>>>> ADDED DATA: {new_data['timestamp']} - close {new_data['close']}'
        print(msg)
        telegram_bot.send_message(text=msg, chat_id=LOGS_GROUP_CHATID)

# Start streaming
stream_socket = f'wss://stream.binance.com:9443/ws/{SYMBOL.lower()}@kline_{STREAM_INTERVAL}'
ws = websocket.WebSocketApp(stream_socket, on_message=on_message, on_open=on_open, on_close=on_close, on_error=on_error)

ws.run_forever()
