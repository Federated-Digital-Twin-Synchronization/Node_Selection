from kafka import KafkaProducer
import random
import time
import json

def read_config(filename):
    with open(filename, 'r') as file:
        config = json.load(file)
    return config

def main():
    config = read_config('/Users/hyeonsuoh/Desktop/Jeju_PoC/Digital_Twin_Syncronization_Server/config.json')
    kafka_server = config['Kafka_Server']
    kafka_server_ip = kafka_server['IP']
    kafka_server_port = kafka_server['PORT']
    print(kafka_server_ip, kafka_server_port)
    # Kafka Producer 설정
    producer = KafkaProducer(
        bootstrap_servers="{}:{}".format(kafka_server_ip, kafka_server_port),
        value_serializer=lambda v: str(v).encode('utf-8')
    )

    # 메시지 전송
    topic = "odometry_topic"  # 전송할 토픽 이름
    while True:
        value = dict()
        value['x'] = random.randint(-20, 20)
        value['y'] = random.randint(-20, 20)
        time.sleep(1)
        producer.send(topic, value=value)        

    # Producer 종료
    producer.close()

if __name__ == '__main__':
    main()
