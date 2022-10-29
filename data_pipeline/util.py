


import yaml





def load_config(yaml_path=str):
    with open(yaml_path) as f:
        cfg = yaml.load(f, Loader=yaml.FullLoader)
    return cfg



def read_json_data(json_path):
    with open(json_path, 'r') as f:
        for record in f.readlines():
            #record = json.loads(record)
            record = eval(record)
            yield record


def all_columns(json_path):
    columns = set()
    with open(json_path, 'r') as f:
        for record in f.readlines():
            #record = json.loads(record)
            record = eval(record)
            columns = columns | set(record.keys())
    return list(columns)



