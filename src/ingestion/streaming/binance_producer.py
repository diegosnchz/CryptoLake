import asyncio
import json
import logging
import os
import sys
import structlog
import websockets
from confluent_kafka import Producer

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))
from src.config.settings import settings

# Configure Structlog
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
)

logger = structlog.get_logger()

# Kafka Producer Configuration
conf = {
    'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS,
    'client.id': 'binance-producer-v2',
    'acks': 'all'
}
producer = Producer(conf)

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result. """
    if err is not None:
        logger.error("message_delivery_failed", error=str(err))
    else:
        # logger.debug("message_delivered", topic=msg.topic(), partition=msg.partition())
        pass

async def binance_ws_listener():
    # WebSocket URL for multiple streams
    # Format: wss://fstream.binance.com/stream?streams=<stream1>/<stream2>/...
    # We want aggTrade for btcusdt, ethusdt, solusdt, bnbusdt
    streams = [
        "btcusdt@aggTrade",
        "ethusdt@aggTrade",
        "solusdt@aggTrade",
        "bnbusdt@aggTrade"
    ]
    stream_string = "/".join(streams)
    ws_url = f"wss://fstream.binance.com/stream?streams={stream_string}"
    
    logger.info("connecting_to_websocket", url=ws_url)

    async with websockets.connect(ws_url) as websocket:
        logger.info("websocket_connected", url=ws_url)
        
        while True:
            try:
                message = await websocket.recv()
                data = json.loads(message)
                
                # The combined stream format returns: {"stream": "<streamName>", "data": <payload>}
                # We want to extract the payload and maybe enrich it with the stream name if needed,
                # but aggTrade payload 's' usually contains the symbol.
                
                if 'data' in data:
                    payload = data['data']
                    symbol = payload.get('s', 'unknown')
                    
                    # Send to Kafka
                    producer.produce(
                        settings.KAFKA_TOPIC_PRICES_REALTIME, 
                        key=str(symbol), 
                        value=json.dumps(payload), 
                        callback=delivery_report
                    )
                    producer.poll(0)
                
            except websockets.exceptions.ConnectionClosed as e:
                logger.error("connection_closed", error=str(e))
                break
            except Exception as e:
                logger.error("processing_error", error=str(e))
                await asyncio.sleep(1)

if __name__ == "__main__":
    try:
        asyncio.run(binance_ws_listener())
    except KeyboardInterrupt:
        logger.info("stopping_producer")
    finally:
        producer.flush()
