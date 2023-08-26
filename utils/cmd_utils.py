from configparser import ConfigParser

from utils.schema_utils import make_class_from_json, JsDict


def make_config_type(config_file,  class_name='Config'):
    conf = ConfigParser()
    conf.read(config_file,
              encoding='utf-8-sig')
    return make_class_from_json(class_name, conf._sections)
