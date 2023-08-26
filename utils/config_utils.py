import sys

from utils.schema_utils import JsDict, class_to_dict


def get_conf():
    from config.config import Dev
    start_model = "dev"
    if 'prod' in sys.argv:
        start_model = 'prod'
    config_env = {}
    config_class_name = f'{start_model.capitalize()}'
    exec(f'from config.config import {config_class_name}', config_env)
    config: Dev = JsDict(class_to_dict(config_env[config_class_name]))
    return config
