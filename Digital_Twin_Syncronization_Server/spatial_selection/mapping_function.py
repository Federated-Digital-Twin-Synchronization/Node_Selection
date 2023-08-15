from typing import Tuple
import math


def euclidean_distance(pos1, pos2):
    x1, y1 = pos1
    x2, y2 = pos2
    return math.sqrt((x1 - x2)**2 + (y1 - y2)**2)


def euclidean_mapping(nodes_data, robot_pos: Tuple) -> Tuple[int, int]:
    # nodes_data : 노드의 위치
    # pos : 로봇의 현재 위치
    temp = float('inf')
    node = 0

    for node_name in nodes_data.keys():
        calc = euclidean_distance(nodes_data[node_name]['pos'], robot_pos)
        if temp > calc:
            temp = calc
            node = node_name
    return int(node)
