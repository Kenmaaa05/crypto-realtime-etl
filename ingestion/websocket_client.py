import websocket
import json

def start_trade_stream(symbols, on_event):
    
    # we create a multistream ws here.
    ws_url = "wss://stream.binance.com:9443"

    streams = "/stream?streams="+"/".join([f"{s}@trade" for s in symbols])
    socket = f"{ws_url}{streams}"

    def on_message(ws, message):
        data = json.loads(message)
        on_event(data)

    def on_close(ws):
        print("Connection Closed!")

    def on_open(ws):
        print("Connection Opened!")

    ws = websocket.WebSocketApp(
        socket,
        on_open=on_open,
        on_message=on_message,
        on_close=on_close
        )

    ws.run_forever()