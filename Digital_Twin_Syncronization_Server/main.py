from IoT_connector.mobius_connector import MobiusConnector
from IoT_connector.kafka_connector import KafkaConnector
from spatial_selection.mapping_function import euclidean_distance, euclidean_mapping
from spatial_selection.nodemapper import NodeGraph, NodeMapper
from kafka import TopicPartition
from logging.handlers import RotatingFileHandler

import json
import logging

def initialize_logger():
    # 로거 설정
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # 터미널에 로그를 출력하기 위한 핸들러
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)

    # 로그 파일에 로그를 저장하기 위한 핸들러
    file_handler = RotatingFileHandler("./log/Digital_Twin_Syncronize.log", maxBytes=5000000, backupCount=5)
    file_handler.setLevel(logging.INFO)
    file_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_format)
    logger.addHandler(file_handler)

class Spatial_Selection_Service:
    def __init__(self):
        self._iot_hub_connector = None
        self._message_broker_connector = None
        self._mapping_function = None
        self._mapping_algorithm = None
        self._node_graph = None
        self._node_mapper = None
        self.MESSAGE_BROKER_IP = None
        self.MESSAGE_BROKER_PORT = None
        self.IoT_HUB_IP = None
        self.IoT_HUB_PORT = None
        initialize_logger()
        logging.info("Spatial Selection Service initialized")

    @property
    def iot_hub_connector(self):
        if self._iot_hub_connector is None:
            raise Exception("MobiusConnector is not set")
        return self._iot_hub_connector

    @iot_hub_connector.setter
    def iot_hub_connector(self, mobius_connector):
        self._iot_hub_connector = mobius_connector
        logging.info("MobiusConnector set to: {}".format(mobius_connector))

    @property
    def message_broker_connector(self):
        if self._message_broker_connector is None:
            raise Exception("KafkaConnector is not set")
        return self._message_broker_connector

    @message_broker_connector.setter
    def message_broker_connector(self, kafka_connector):
        self._message_broker_connector = kafka_connector
        logging.info("KafkaConnector set to: {}".format(kafka_connector))

    @property
    def mapping_function(self):
        if self._mapping_function is None:
            raise Exception("Mapping function is not set")
        return self._mapping_function

    @mapping_function.setter
    def mapping_function(self, mapping_function):
        self._mapping_function = mapping_function
        logging.info("Mapping function set to: {}".format(mapping_function))

    @property
    def mapping_algorithm(self):
        if self._mapping_algorithm is None:
            raise Exception("Mapping algorithm is not set")
        return self._mapping_algorithm

    @mapping_algorithm.setter
    def mapping_algorithm(self, mapping_algorithm):
        self._mapping_algorithm = mapping_algorithm
        logging.info("Mapping algorithm set to: {}".format(mapping_algorithm))

    @property
    def node_graph(self):
        if self._node_graph is None:
            raise Exception("Node graph is not set")
        return self._node_graph

    @node_graph.setter
    def node_graph(self, node_graph):
        self._node_graph = node_graph
        logging.info("Node graph set to: {}".format(node_graph))

    @property
    def node_mapper(self):
        if self._node_mapper is None:
            raise Exception("Node mapper is not set")
        return self._node_mapper

    @node_mapper.setter
    def node_mapper(self, node_mapper):
        self._node_mapper = node_mapper
        logging.info("Node mapper set to: {}".format(node_mapper))

    def set_IoT_hub_connector_config(self, IP, PORT):
        self.IoT_HUB_IP = IP
        self.IoT_HUB_PORT = PORT

    def set_message_broker_connector_config(self, IP, PORT):
        self.MESSAGE_BROKER_IP = IP
        self.MESSAGE_BROKER_PORT = PORT

    def run(self):
        topic = "odometry_topic"
        num_last_messages = 1
        print(self._message_broker_connector.partitions_for_topic(topic))
        partitions = self._message_broker_connector.partitions_for_topic(topic)
        last_offsets = {p: offset - 1 for p in partitions
                        for offset in self._message_broker_connector.end_offsets([TopicPartition(topic, p)]).values()}
        for p, last_offset in last_offsets.items():
            start_offset = max(0, last_offset - num_last_messages + 1)
            tp = TopicPartition(topic, p)
            self._message_broker_connector.assign([tp])
            self._message_broker_connector.seek(tp, start_offset)

        for message in self._message_broker_connector:
            logging.info("Message received: {}".format(message))
            agent_position = eval(message.value.decode('utf-8'))
            agent_x = agent_position["x"]
            agent_y = agent_position["y"]
            agent_pos = (agent_x, agent_y)
            target_node_idx = self._node_mapper.map_node(agent_pos)
            lod_list = self._node_mapper.calculate_RoI(target_node_idx)
            sensor_lod_list = self._node_mapper.map_sensor_lod(lod_list)
            logging.info("RoI calculated: {}".format(sensor_lod_list))
            for lod, sensors in sensor_lod_list.items():
                if lod == "LoD2":
                    for sensor in sensors:
                        self.iot_hub_connector.send("Cam_{}/Human".format(str(sensor)), "2")
                elif lod == "LoD1":
                    for sensor in sensors:
                        self.iot_hub_connector.send("Cam_{}/Human".format(str(sensor)), "1")
                else:
                    for sensor in sensors:
                        self.iot_hub_connector.send("Cam_{}/Human".format(str(sensor)), "0")

def read_config(filename):
    with open(filename, 'r') as file:
        config = json.load(file)
    return config

if __name__ == "__main__":
    config = read_config("/Users/hyeonsuoh/Desktop/Jeju_PoC/Digital_Twin_Syncronization_Server/config.json")
    kafka_server = config['Kafka_Server']
    kafka_server_ip = kafka_server['IP']
    kafka_server_port = kafka_server['PORT']
    mobius_server = config['Mobius_Server']
    mobius_server_ip = mobius_server['IP']
    mobius_server_port = mobius_server['PORT']

    sss = Spatial_Selection_Service()
    sss.set_IoT_hub_connector_config(mobius_server_ip, mobius_server_port)
    sss.set_message_broker_connector_config(kafka_server_ip, kafka_server_port)
    sss.iot_hub_connector = MobiusConnector("{}:{}".format(mobius_server_ip, mobius_server_port))
    sss.message_broker_connector = KafkaConnector(sss.MESSAGE_BROKER_IP, sss.MESSAGE_BROKER_PORT).consumer
    sss.mapping_function = euclidean_mapping
    sss.mapping_algorithm = euclidean_distance
    sss.node_graph = NodeGraph()
    sss.node_mapper = NodeMapper(sss.node_graph.graph)
    sss.node_mapper.mapping_function = sss.mapping_function
    sss.run()
