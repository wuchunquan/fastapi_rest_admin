# 类型提示一些工具，如将配置文件转为class
from typing import List


def make_class_from_json(class_name, data):
    """
    json转class,用于类型提示
    :param class_name:
    :param data:
    :return:

        json_data = {
        "name": "Alice",
        "age": 30,
        "address": {
            "street": "123 Main St",
            "city": "Exampleville"
        },
        "friends": [
            {"name": "Bob", "age": 28},
            {"name": "Charlie", "age": 32}
        ]
        }

        # 生成类定义的代码字符串
        class_code = make_class_from_json("Person", json_data)

    """

    def generate_class_code(_data, _class_name="GeneratedClass", indent=0):
        class_definition = f"\n{indent * ' '}class {_class_name}:\n"
        indent_str = " " * (indent + 4)

        for key, value in _data.items():
            if isinstance(value, dict):
                class_definition += generate_class_code(value, key, indent + 4)
            elif isinstance(value, list) and value and isinstance(value[0], dict):
                class_definition += generate_class_code(value[0], key, indent + 4)
                class_definition += f"\n{indent_str}{key}: List[{key}]\n"
            else:
                attr_type = type(value).__name__
                class_definition += f"{indent_str}{key}: {attr_type}\n"

        return class_definition

    return generate_class_code(data, class_name)[1:]


# python字典转js字典
class JsDict(dict):

    def __new__(cls, *args, **kwargs):
        cls.__setitem__ = cls.__setattr__
        return super().__new__(cls)

    def __init__(self, dic):
        for key, value in dic.items():
            self.__setattr__(key, value)

    def __make_item__(self, item):
        if type(item) == dict:
            item = JsDict(item)
        elif type(item) == list:
            item = [self.__make_item__(one) for one in item]
        return item

    def __setattr__(self, key, item):
        new_item = self.__make_item__(item)
        dict.__setattr__(self, key, new_item)
        self.update({key: new_item})


def class_to_dict(class_obj):
    class_dict = {}
    for key, value in class_obj.__dict__.items():
        if not key.startswith("__"):
            if isinstance(value, type):
                class_dict[key] = class_to_dict(value)
            else:
                class_dict[key] = value
    return class_dict
