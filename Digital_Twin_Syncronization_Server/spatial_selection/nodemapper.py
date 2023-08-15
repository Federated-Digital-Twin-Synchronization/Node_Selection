from typing import Tuple

import json
import networkx as nx


class NodeGraph():
    def __init__(self, node_path: str = "./data/node.json"):
        self.node_path = node_path
        with open(node_path) as f:
            self.graph_data = json.load(f)
        self.nodes = self.graph_data["node_locations"]
        self.edges = self.graph_data["graph"]

        self.node_graph = nx.Graph()

        for idx, node in enumerate(self.nodes):
            self.node_graph.add_node(idx, pos=(node[0], node[1]))

        pos = {}
        for idx, node in enumerate(self.nodes):
            pos[idx] = (node[0], node[1])

        for center, edges_dict in self.edges.items():
            for edge in edges_dict["edges"]:
                self.node_graph.add_edge(int(center), edge)

    @property
    def graph(self):
        return self.node_graph

    @graph.setter
    def graph(self, graph):
        self.node_graph = graph


class NodeMapper:
    def __init__(self, graph, sensor_topology_path: str = "./data/node.json"):
        self._graph = graph
        self._mapping_function = None
        with open(sensor_topology_path) as f:
            self._sensor_topology = json.load(f)
        self._sensor_topology = self._sensor_topology["sensor_topology"]

    @property
    def mapping_function(self):
        if self._mapping_function is None:
            print("Mapping function not set")
        else:
            return self._mapping_function

    @mapping_function.setter
    def mapping_function(self, mapping_function):
        self._mapping_function = mapping_function
        print("Mapping function set to: {}".format(mapping_function))

    @property
    def graph(self):
        return print(self._graph.nodes, self._graph.edges)

    @graph.setter
    def graph(self, graph):
        self._graph = graph

    def map_node(self, pos: Tuple[int, int]):
        nodes_data = dict(self._graph.nodes.data())
        return self._mapping_function(nodes_data, pos)

    def calculate_RoI(self, node_idx: int):
        bfs_tree = nx.bfs_tree(self._graph, source=node_idx)
        roi_list = [[node_idx]]

        for roi_level in range(0, 2):
            roi_list.append([node for node, depth, in nx.shortest_path_length(
                bfs_tree, source=node_idx).items() if depth == roi_level])
        roi_list.reverse()
        return roi_list

    def map_sensor_lod(self, calc_roi):
        ans = {}
        selected_cameras = set()
        for idx, nodes in enumerate(calc_roi[::-1]):
            temp_2 = []
            for comp in nodes:
                found = self._sensor_topology.get(str(comp), [])
                if all(isinstance(item, (int, float, str)) for item in found):
                    found = [item for item in found if item not in selected_cameras]
                    temp_2.extend(found)
                    selected_cameras.update(found)
            ans["LoD" + str(len(calc_roi) - idx - 1)] = temp_2
        return ans


if __name__ == "__main__":
    ng = NodeGraph()
    print(ng.node_graph.nodes.data())
