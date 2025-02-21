# https://kafka-python.readthedocs.io/en/master/apidoc/modules.html
from kafka import KafkaConsumer, TopicPartition
from shared import bootstrap_servers, topic
import datetime

def epoch_to_str(e):
    e = int(e/1000)  # shift left 3 
    return datetime.datetime.fromtimestamp(e).strftime('%Y-%m-%d %I:%M:%S %p')


# topic = 'test_microservice'
# bootstrap_servers = 'localhost:9092'
consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers,
    # auto_offset_reset='latest', 
    # auto_offset_reset='earliest', 
    # enable_auto_commit=False,
    # group_id=None
)

for partition_idx in consumer.partitions_for_topic(topic):
    partition = TopicPartition(topic,partition_idx)
    consumer.assign([partition])
    consumer.seek_to_end(partition)
    last_offset=consumer.position(partition)
    print(f"topic,partition,last_offset = {partition.topic},{partition.partition},{last_offset}")    
    last_offset+=-3   # get last two messages

    if last_offset >= 0:
        consumer.seek(partition, last_offset)       
        message = consumer.poll(timeout_ms=5000)    # Wait up to 5 seconds
        for tp, records in message.items():
                # print(f"topic={tp.topic}, partition={tp.partition}")
                for record in records:
                    print(f"{record.offset} {epoch_to_str(record.timestamp)} {record.value.decode('utf-8')} ")

consumer.close()