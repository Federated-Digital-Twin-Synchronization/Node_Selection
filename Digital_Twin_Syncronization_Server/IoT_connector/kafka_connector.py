from kafka import KafkaConsumer, TopicPartition


class KafkaConnector:
    def __init__(self, IP, PORT):
        self.consumer = KafkaConsumer(
            bootstrap_servers=['{}:{}'.format(IP, PORT)],
            auto_offset_reset='earliest',
            group_id='mygroup',
            enable_auto_commit=False,
        )

    # def consume_messages(consumer, topic, num_last_messages):
    #     partitions = consumer.partitions_for_topic(topic)
    #     last_offsets = {p: offset - 1 for p in partitions
    # for offset in consumer.end_offsets([TopicPartition(topic, p)]).values()}

    #     for p, last_offset in last_offsets.items():
    #         start_offset = max(0, last_offset - num_last_messages + 1)
    #         tp = TopicPartition(topic, p)
    #         consumer.assign([tp])
    #         consumer.seek(tp, start_offset)

    #     for message in consumer:
    #         print("Received message: {}".format(message.value))

    # def main():
    #     topic = 'odometry_topic'
    #     consumer = create_consumer()
    #     consume_messages(consumer, topic, 1)  # Start from the last 10 messages
