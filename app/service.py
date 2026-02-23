import asyncio
import json
import os

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from app.manager import DatabaseManager
from app.models import OrderModel
from app.schemas import OrderCreate

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
TOPIC_NAME = "orders_topic"

class KafkaService:
    def __init__(self):
        self.producer = None

    async def start(self):
        self.producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        
        # retry logic
        # connect 10 times - 5 sec wait
        for i in range(10): 
            try:
                print(f"connect to Kafka - {i+1}/10")
                await self.producer.start()
                print("Kafka Producer Connected")
                return
            except Exception as e:
                print(f"Kafka connection failed - {e}")
                await asyncio.sleep(5)
        

        raise Exception("Could not connect to Kafka")

    async def stop(self):
        if self.producer:
            await self.producer.stop()

    async def send_order_event(self, order_id: int, order_data: dict):
        event = {"order_id": order_id, "data": order_data}
        value_json = json.dumps(event).encode("utf-8")
        await self.producer.send_and_wait(TOPIC_NAME, value_json)

kafka_service = KafkaService()

async def consume_orders():
    # Background Task: Consumes messages and processes business logic.
    await asyncio.sleep(10) 
    
    consumer = AIOKafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="order_group"
    )
    
    while True:
        try:
            await consumer.start()
            print("Kafka Consumer Connected")
            break
        except Exception:
            await asyncio.sleep(5)

    try:
        async for msg in consumer:
            payload = json.loads(msg.value.decode("utf-8"))
            order_id = payload["order_id"]
            order_data = payload["data"]

            new_status = "COMPLETED"
            if order_data["quantity"] > 10:
                new_status = "REJECTED"
                print(f"Order {order_id} rejected (Quantity too high)")

            async with DatabaseManager() as db:
                await db.update_order_status(order_id, new_status)

            # async with db_session_factory() as session:
            #     result = await session.execute(select(OrderModel).where(OrderModel.id == order_id))
            #     order = result.scalar_one_or_none()
            #     if order:
            #         order.status = "COMPLETED"
            #         await session.commit()
            #         print(f"{order_id} processed successfully")
    finally:
        await consumer.stop()