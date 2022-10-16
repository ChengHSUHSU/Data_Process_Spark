



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
                #record = json.loads(record)
                record = eval(record)
                columns = columns | set(record.keys())
        return list(columns)


    def build_generator(self, json_path=str):
        with open(json_path, 'r') as f:
            for record in f.readlines():
                #record = json.loads(record)
                record = eval(record)
                yield record
    



if __name__ == '__main__':
    
    yaml_path = 'pipeline.yaml'
   
    
    producer = Producer(yaml_path)

    available_data_paths = producer.available_data_paths


    print('available_data_paths : ', available_data_paths)


    columns = producer.show_all_columns(available_data_paths[0])

    print('columns ; ', columns)


    generator = producer.build_generator(available_data_paths[1])


    for d in generator:
        print(d)
        quit()














