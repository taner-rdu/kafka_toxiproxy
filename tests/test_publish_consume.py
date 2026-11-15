import uuid

from confluent_kafka import Consumer, Producer

TOPIC = "test"
PRODUCER_PROXY = "localhost:9094"  # producer -> toxiproxy -> kafka
KAFKA = "localhost:9092"  # consumer -> kafka (direct)


def test_publish_and_consume():
    message_id = str(uuid.uuid4())
    payload = f"msg-{message_id}".encode()

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA,
            "group.id": f"test-{message_id}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC])

    producer = Producer({"bootstrap.servers": PRODUCER_PROXY})
    producer.produce(TOPIC, key=message_id.encode(), value=payload)
    assert producer.flush(10) == 0

    received = None
    try:
        for _ in range(30):
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            if msg.key() == message_id.encode():
                received = msg.value()
                break
    finally:
        consumer.close()

    assert received == payload
