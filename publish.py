from kafka import KafkaProducer
from shared import bootstrap_servers,topic
# import base64

producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
for i in range(10):
    msg = f'This is a message{i}'
    print(f'publishing {msg} to topic {topic} @ {bootstrap_servers}')
    producer.send(topic,msg.encode('utf-8'))
producer.flush()