



import json
from util import load_config








class Producer:
    def __init__(self, yaml_path=str):
        # load config
        cfg = load_config(yaml_path=yaml_path)

        # available_data_paths
        self.available_data_paths = cfg['data_paths']

        # build generator
        # generator = self.build_generator(json_path=self.available_data_paths[0])


    def show_all_columns(self, json_path=str):
        columns = set()
        with open(json_path, 'r') as f:
            for record in f.readlines():
                record = json.loads(record)
                columns = columns | set(record.keys())
        return list(columns)


    def build_generator(self, json_path=str):
        with open(json_path, 'r') as f:
            for record in f.readlines():
                record = json.loads(record)
                yield record
    






