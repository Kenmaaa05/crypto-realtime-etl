import json
import signal
import sys
from kafka import KafkaProducer
from websocket_client import start_trade_stream
import time

KAFKA_BROKER = "localhost:9094"
TOPIC = "binance_trades"

producer = KafkaProducer(
    bootstrap_servers = KAFKA_BROKER,
    value_serializer = lambda v: json.dumps(v).encode("utf-8"),
    key_serializer = lambda k: k.encode("utf-8"),
    linger_ms = 10,
    acks = "all",
    retries = 3
)

def handle_trade(event):
    symbol = event["data"]["s"]
    event["data"]["ingestion_time"] = int(time.time() * 1000) # i was sending the ingestion_time directly, spark was expecting it to be data -> ingestion_time, so i had to change so ingestion time was inside data.
    producer.send(
        TOPIC,
        key=symbol,
        value=event
    )

if __name__ == "__main__":

    symbols = ["btcusdt","ethusdt", "bnbusdt"]
    start_trade_stream(symbols, handle_trade)
    
