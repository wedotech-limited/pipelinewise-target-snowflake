from collections import defaultdict
from copy import deepcopy
from typing import List
from . import stream_utils


class __StreamGraph:
    def __init__(self) -> None:
        self.graph = defaultdict(list)
        self.streams = list()

    def add_edge(self, parent_stream, child_stream):
        if parent_stream not in self.streams:
            self.streams.append(parent_stream)

        if child_stream not in self.streams:
            self.streams.append(child_stream)
        
        self.graph[parent_stream].append(child_stream)

    def sort(self):
        visited = {stream: False for stream in self.streams}
        result = []

        for stream in self.streams: 
            if not visited[stream]:
                self.__visit(stream, visited, result)

        return result

    def __visit(self, stream, visited, result: List):
        visited[stream] = True

        for child_stream in self.graph[stream]:
            if not visited[child_stream]:
                self.__visit(child_stream, visited, result)

        result.insert(0, stream)

def topological_sort_relationships(db_syncs: List):
    # compute unique flattened list of relationships
    relationships_map = {}

    for db_sync in db_syncs:
        relationships = deepcopy(db_sync.referential_relationships)
        __flatten_relationships(relationships_map, relationships)

    flattened_relationships = relationships_map.values()

    # topological sort the relationships
    graph = __StreamGraph()
    for relationship in flattened_relationships:
        graph.add_edge(relationship['parent_tap_stream_id'], relationship['tap_stream_id'])
    
    stream_ids = graph.sort()

    filtered_relationships = []

    for stream_id in stream_ids:
        filtered_relationships.extend(list(filter(lambda rel: rel['tap_stream_id'] == stream_id, flattened_relationships)))

    return filtered_relationships

def get_target_stream_id(config, tap_stream_id):
    config_default_target_schema = config.get('default_target_schema', '').strip()
    config_schema_mapping = config.get('schema_mapping', {})
    stream_data = stream_utils.stream_name_to_dict(tap_stream_id)
    target_schema_name = config_default_target_schema
    if stream_data['schema_name'] in config_schema_mapping:
        target_schema_name = config_schema_mapping[stream_data['schema_name']]['target_schema']

    if target_schema_name is None:
        raise Exception(
                    "Target schema name not defined in config. "
                    "Neither 'default_target_schema' (string) nor 'schema_mapping' (object) defines "
                    f"target schema for {tap_stream_id} stream.")

    return target_schema_name + '-' + stream_data['table_name']

def __flatten_relationships(relationships_map, relationships):
    if relationships is None or len(relationships) == 0:
        return relationships_map

    for relationship in relationships:
        relationship_key = f'{relationship["tap_stream_id"]}_{relationship["parent_tap_stream_id"]}'
        if relationship.get("active") and relationship_key not in relationships_map:
            sub_relationships = relationship.pop("referential_relationships", None)
            relationships_map[relationship_key] = relationship
            __flatten_relationships(relationships_map, sub_relationships)

    return relationships_map