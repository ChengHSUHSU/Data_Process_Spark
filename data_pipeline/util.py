


import yaml





def load_config(yaml_path=str):
    with open(yaml_path) as f:
        cfg = yaml.load(f, Loader=yaml.FullLoader)
    return cfg

