import json
from functools import wraps
from typing import Literal, List, Any, Optional, Union, Generic, TypeVar, Dict, Type

from fastapi import HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, and_, asc, desc, func, String, Table, ForeignKey
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, Query, Load
from sqlalchemy_utils import database_exists, create_database, get_columns, get_column_key, get_type, get_primary_keys
from pydantic import BaseModel
from sqlalchemy import Column
from sqlalchemy.orm import InstrumentedAttribute, ColumnProperty, Relationship, DeclarativeMeta, Session
from sqlalchemy_utils import get_columns, get_column_key


class SqlalchemyConnect:
    def __init__(self, host="127.0.0.1", user="", password="", db="",
                 db_type: Literal['mysql', 'postgresql'] = 'mysql'):
        self.base = declarative_base()
        self.host = host
        self.user = user
        self.password = password
        self.db = db
        if db_type == 'mysql':
            self.engine = self.init_engine()
        if db_type == 'postgresql':
            self.engine = self.init_postgresql_engine()
        # self.async_engine = self.init_async_engine()

    def init_engine(self):
        engine = create_engine(
            f"mysql+pymysql://{self.user}:{self.password}@{self.host}/{self.db}?charset=utf8mb4", pool_recycle=3600 * 4)
        return engine

    def init_postgresql_engine(self):
        engine = create_engine(f'postgresql+psycopg2://{self.user}:{self.password}@{self.host}/{self.db}',
                               pool_recycle=3600 * 4, echo=False)
        return engine

    def create_db(self):
        if not database_exists(self.engine.url):
            create_database(self.engine.url)

    def get_db(self) -> sessionmaker:
        try:
            session = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
            db = session()
            yield db
        finally:
            db.close()

    def get_db_commit(self) -> sessionmaker:
        try:
            session = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
            db = session()
            yield db
            db.commit()
        finally:
            db.close()

    def get_db_i(self) -> Session:
        # engine = create_engine(
        #     f"mysql+pymysql://{self.user}:{self.password}@{self.host}/{self.db}?charset=utf8mb4")
        session = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        db = session()
        return db

    def init_database(self, create_db=False):
        if create_db:
            self.create_db()
        self.base.metadata.create_all(bind=self.engine)

    def init_async_engine(self):
        async_engine = create_async_engine(
            f"mysql+aiomysql://{self.user}:{self.password}@{self.host}/{self.db}?charset=utf8mb4", pool_recycle=3600 * 4
        )
        return async_engine


def get_model_column_keys(model):
    return [get_column_key(model, item) for item in get_columns(model)]


def marge_col_names(col_names, include: List[str] = None, ex_include: List[str] = None):
    if include:
        col_names = [item for item in include if item in col_names]
    if ex_include:
        col_names = [item for item in col_names if item not in ex_include]
    return set(col_names)


# 通用查询接口

QueryType = Literal['=', '==', '>', '>=', '<', '<=', 'in', 'like', 'range', "find_in_set"]


class QueryParam(BaseModel):
    name: str
    type: QueryType
    value: Any


# class OrderParam(BaseModel):
#     order_by: str
#     type: Literal['asc', 'desc'] = 'asc'


ModelInfo = TypeVar('ModelInfo', bound=BaseModel)


class QueryInclude(BaseModel):
    include: Optional[List[str]] = None
    ex_include: Optional[List[str]] = None
    relation_use_id: bool = False  # 为true的话，m2m关系返回的是id数组


class QueryParams(QueryInclude, Generic[ModelInfo]):
    params: Optional[List[QueryParam]] = None
    order: Optional[Dict[str, Literal["asc", "desc", None]]] = None
    query: Optional[ModelInfo] = None
    page: int
    page_size: int


def query_common(db: Session, model, query_params: QueryParams):
    """
    通用数据库查询接口
    """
    query = db.query(model)
    # if query_params.ex_include:
    #     query = query.options(Load(model).load_only(*query_params.include))
    # if query_params.include:
    #     query = query.options(Load(model).noload(*query_params.ex_include))
    page = query_params.page
    page_size = query_params.page_size
    # 排序
    if query_params.order:
        query = query_order(model, query, query_params.order)

    if query_params.query:
        query = query_common_query_core(model, query, query_params.query)
    # 筛选
    if query_params.params:
        query = query_common_params_core(model, query, query_params.params)

    count = query.count()

    return count, query, page, page_size


def query_order(model, query: Query, order_param: Dict[str, Literal["asc", "desc", None]]):
    """
    查询结果排序
    """
    for key, value in order_param.items():
        if value:
            column: Column = getattr(model, key)
            if value == 'asc':
                query = query.order_by(asc(column))
            if value == 'desc':
                query = query.order_by(desc(column))
    return query


def query_common_params_core(model, query: Query, params: List[QueryParam]):
    cols: Dict[str, TypeInfo] = model.model_config.cols
    for item in params:
        name = item.name
        child_name = ""
        if '.' in name:
            [name, child_name] = name.split('.')
        query_type = item.type
        value = item.value
        col = cols[name]
        col_base_type = col.col_base_type
        column: Column = getattr(model, name)
        if col_base_type == 'relation' and col.relation:
            if col.relation.relation_type in ['m2o', 'o2o']:
                if col.relation.source_foreign_key:
                    query = query.join(col.relation.target_model,
                                       getattr(col.relation.target_model, col.relation.target_id_key) == getattr(model,
                                                                                                                 col.relation.source_foreign_key))
                else:
                    query = query.join(col.relation.target_model)
                column = getattr(col.relation.target_model, child_name)
        else:
            if col_base_type == 'json':
                column = column[child_name]
        if query_type in ['=', '==']:
            if col_base_type == 'relation' and col.relation and not child_name:
                relation_type = col.relation.relation_type
                secondary = col.relation.secondary
                target_secondary_key = col.relation.target_secondary_key
                source_secondary_key = col.relation.source_secondary_key
                if relation_type == 'm2m':
                    query = query.join(secondary, model.id == secondary.columns.get(source_secondary_key)) \
                        .filter(secondary.columns.get(target_secondary_key) == value)
            else:
                if col.col_base_type == 'datetime':
                    query = query.filter(func.date(column) == value)
                elif col_base_type == 'json':
                    query = query.filter(column == json.dumps(value))
                else:
                    query = query.filter(column == value)

        elif query_type == ">":
            query = query.filter(column > value)
        elif query_type == "<":
            query = query.filter(column < value)
        elif query_type == ">=":
            query = query.filter(column >= value)
        elif query_type == "<=":
            query = query.filter(column <= value)
        elif query_type == "in":
            if col_base_type == 'relation' and col.relation:
                relation_type = col.relation.relation_type
                if relation_type == 'm2m':
                    secondary = col.relation.secondary
                    target_secondary_key = col.relation.target_secondary_key
                    source_secondary_key = col.relation.source_secondary_key
                    if value and type(value) == list:
                        query = query.join(secondary, model.id == secondary.columns.get(source_secondary_key)) \
                            .filter(secondary.columns.get(target_secondary_key).in_(value))
                    else:
                        query = query.join(secondary, model.id == secondary.columns.get(source_secondary_key)) \
                            .filter(secondary.columns.get(target_secondary_key) == value)

            else:
                query = query.filter(column.in_(value))
        elif query_type == "like":
            query = query.filter(column.cast(String).like(f'%{value}%'))
        elif query_type == "find_in_set":
            query = query.filter(func.find_in_set(value, column))
        elif query_type == "range":
            if value:
                if len(value) == 1 and value[0] is not None:
                    query = query.filter(column >= value[0])
                if len(value) == 2:
                    if value[0] is not None and value[1] is None:
                        query = query.filter(column >= value[0])
                    if value[0] is None and value[1] is not None:
                        query = query.filter(column <= value[1])
                    if value[0] is not None and value[1] is not None:
                        query = query.filter(and_(column >= value[0], column <= value[1]))
    return query


def query_common_query_core(model, query: Query, query_data: BaseModel):
    q_data = query_data.dict(exclude_unset=True)
    cols: Dict[str, TypeInfo] = model.model_config.cols
    for key, value in q_data:
        annotation = query_data.__annotations__[key]
        if key in cols:
            column: Column = getattr(model, key)
            col = cols[key]
            col_base_type = col.col_base_type
            if col_base_type in ['int', 'float', 'any', 'enum', 'str']:
                if type(value) == list and value:
                    query = query.filter(column.in_(value))
                else:
                    if col_base_type == 'str' and value:
                        query = query.filter(column.like(f'%{value}%'))
                    else:
                        query = query.filter(column == value)

            elif col_base_type in ['date', 'datetime', 'time']:
                if value:
                    if type(value) == list:
                        if len(value) == 2:
                            if value[0] is not None:
                                query = query.filter(column >= value)
                            if value[1] is not None:
                                query = query.filter(column <= value)

                    else:
                        if col_base_type == 'datetime':
                            # datetime转date来查
                            query = query.filter(func.date(column) == value)
                        else:
                            query = query.filter(column == value)
            elif col_base_type == 'relation' and col.relation:
                relation_type = col.relation.relation_type
                if relation_type == 'm2m':
                    secondary = col.relation.secondary
                    target_secondary_key = col.relation.target_secondary_key
                    source_secondary_key = col.relation.source_secondary_key

                    if value and type(value) == list:
                        query = query.join(secondary, model.id == getattr(secondary, source_secondary_key)) \
                            .filter(getattr(secondary, target_secondary_key).in_(value))
                    else:
                        query = query.join(secondary, model.id == getattr(secondary, source_secondary_key)) \
                            .filter(getattr(secondary, target_secondary_key) == value)
                elif relation_type == 'o2m':
                    target_model = col.relation.target_model
                    target_id_key = col.relation.target_id_key

                    if value and type(value) == list:
                        query = query.join(target_model).filter(getattr(target_model, target_id_key).in_(value))
                    else:
                        query = query.join(target_model).filter(getattr(target_model, target_id_key == value))

    return query


def tree_table_to_json(db: Session, model, belong_str='belong', key_str='id'):
    """
    树结构数据库存储到json
    """
    item_list = db.query(model).all()
    node_list = []
    for item in item_list:
        item_dic = item.to_dict()
        item_dic['children'] = []
        node_list.append(item_dic)

    nodes_dic = {node[key_str]: node for node in node_list}
    tree = []
    for node in node_list:
        belong = node[belong_str]
        if belong:
            if belong in nodes_dic:
                nodes_dic[belong]['children'].append(node)
        else:
            tree.append(node)
    return tree


def get_relation(col: InstrumentedAttribute):
    prop = col.property
    if isinstance(prop, Relationship):
        model = prop.entity.entity
        if prop.secondary is not None:
            columns = prop.secondary.columns
            foreign_key_0 = list(columns[0].foreign_keys)[0]
            foreign_key_1 = list(columns[1].foreign_keys)[0]
            if foreign_key_0.column.table.key == model.__tablename__:
                target_secondary_key = columns[0].key
                target_key = foreign_key_0.column.key
                source_secondary_key = columns[1].key
                source_key = foreign_key_1.column.key
            else:
                source_secondary_key = columns[0].key
                source_key = foreign_key_0.column.key
                target_secondary_key = columns[1].key
                target_key = foreign_key_1.column.key
            return {'relation': 'm2m',
                    'model': model,
                    'secondary': prop.secondary,
                    'source_secondary_key': source_secondary_key,
                    'source_key': source_key,
                    'target_secondary_key': target_secondary_key,
                    'target_key': target_key,
                    }
        else:
            return {'relation': 'm2o', 'model': model}
    else:
        return False


def get_relation_col_dic(model: Type[DeclarativeMeta]):
    relation_col_dic = {

    }
    for col_name in dir(model):
        col = getattr(model, col_name)
        if isinstance(col, InstrumentedAttribute):
            relation = get_relation(col)
            if relation:
                relation_col_dic[col_name] = relation
    return relation_col_dic


# 一个模型的字段类型
# 基本类型
RelationType = Literal['m2m', 'm2o', 'o2m', 'o2o']
BaseType = Literal[
    'int', 'float', 'bool', 'str', 'date', 'datetime', 'list', 'time', 'any', 'enum', 'set', 'relation', "json", "jsonb"]


class Relation:
    relation_type: RelationType = None
    source_model: Type[DeclarativeMeta] = None  # 左表
    target_model: Type[DeclarativeMeta] = None  # 右表
    secondary: Any = None  # 中间表
    source_secondary_key: str = None  # 中间表左表字段
    source_key: str = None  # 左表在中间表使用的key
    target_secondary_key: str = None  # 中间表右表字段
    target_key: str = None  # 右表在中间表使用的key
    source_id_key: str = None  # 左表id
    target_id_key: str = None  # 右表id
    source_foreign_key: str = None  # 左表里字段对应的foreign_key字段


ColOrgTypeEnum = Literal[
    "BigInteger",
    "Boolean",
    "Date",
    "DateTime",
    "Enum",
    "Double",
    "Float",
    "Integer",
    "Interval",
    "LargeBinary",
    "MatchType",
    "Numeric",
    "PickleType",
    "SchemaType",
    "SmallInteger",
    "String",
    "Text",
    "Time",
    "Unicode",
    "UnicodeText",
    "Uuid",
    "JSON",
    "JSONB",
    'DeclarativeMeta',
]


class TypeInfo:
    col_org_type: ColOrgTypeEnum = None
    col_base_type: BaseType = None
    relation: Relation = None


class ModelConfig:
    cols: Dict[str, TypeInfo] = {}
    id_key: str = 'id'


COL_ORG_TYPE_MAP = {
    "BigInteger": 'int',
    "Boolean": 'bool',
    "Date": 'date',
    "DateTime": 'datetime',
    "Enum": 'enum',
    "Double": 'float',
    "Float": 'float',
    "Integer": 'int',
    "Interval": 'any',
    "LargeBinary": 'any',
    "MatchType": 'any',
    "Numeric": 'any',
    "PickleType": 'any',
    "SchemaType": 'any',
    "SmallInteger": 'int',
    "String": 'str',
    "Text": 'str',
    "Time": 'time',
    "Unicode": 'str',
    "UnicodeText": 'str',
    "Uuid": 'str',
    "JSON": "json",
    "JSONB": "jsonb",
    'DeclarativeMeta': 'relation'
}


def get_model_id_key(model: Type[DeclarativeMeta]):
    id_keys = list(get_primary_keys(model).keys())
    print(id_keys)
    if id_keys:
        return id_keys[0]


def get_relation_info(col: InstrumentedAttribute) -> Relation:
    prop = col.property
    if isinstance(prop, Relationship):
        r = Relation()
        source_model = col.property.parent.entity
        source_id_key = get_model_id_key(source_model)
        target_model = prop.entity.entity
        target_id_key = get_model_id_key(target_model)
        source_uselist = prop.uselist
        target_uselist = False
        r.source_model = source_model
        r.target_model = target_model
        r.source_id_key = source_id_key
        r.target_id_key = target_id_key
        if col.property._user_defined_foreign_keys:
            r.source_foreign_key = list(col.property._user_defined_foreign_keys)[0].name
            print(r.source_foreign_key)
        #  找到另一个表内反向引用的字段id
        for name in get_model_col_names(target_model):
            target_col = getattr(target_model, name)
            col_org_type = get_type(target_col)
            if source_model == col_org_type:
                target_uselist = target_col.property.uselist
            # 寻找是否右表有对应的foreign_key
            if hasattr(target_col, 'foreign_keys'):
                for foreign_key in list(target_col.foreign_keys):
                    if foreign_key.column.table.name == source_model.__tablename__:
                        r.source_key = foreign_key.column.key
        #  找到本表内反向引用的字段id
        # for name in get_model_col_names(source_model):
        #     source_col = getattr(target_model, name)
        #     col_org_type = get_type(source_col)
        #     if target_model == col_org_type:
        #         source_uselist = source_col.property.uselist
        #     # 寻找是否右表有对应的foreign_key
        #     if hasattr(source_col,'foreign_keys'):
        #         for foreign_key in List[source_col.foreign_keys]:
        #             if foreign_key.column.table.name==target_model.__tablename__:
        #                 r.target_key=foreign_key.column.key
        if (source_uselist and target_uselist) or prop.secondary is not None:
            r.relation_type = 'm2m'
            columns = prop.secondary.columns
            foreign_key_0 = list(columns[0].foreign_keys)[0]
            foreign_key_1 = list(columns[1].foreign_keys)[0]
            if foreign_key_0.column.table.key == target_model.__tablename__:
                target_secondary_key = columns[0].key
                target_key = foreign_key_0.column.key
                source_secondary_key = columns[1].key
                source_key = foreign_key_1.column.key
            else:
                source_secondary_key = columns[0].key
                source_key = foreign_key_0.column.key
                target_secondary_key = columns[1].key
                target_key = foreign_key_1.column.key
            r.secondary = prop.secondary
            r.source_secondary_key = source_secondary_key
            r.target_secondary_key = target_secondary_key
            r.source_key = source_key
            r.target_key = target_key
        elif source_uselist == True and target_uselist == False:
            r.relation_type = 'o2m'
        elif source_uselist == False and target_uselist == False:
            r.relation_type = 'o2o'
        elif source_uselist == False and target_uselist == True:
            r.relation_type = 'm2o'
        return r
    else:
        return None


def get_model_config(model: Type[DeclarativeMeta]) -> ModelConfig:
    """
    这个接口获取一个sql模型的各种信息
    :param model:
    :return:
    """

    # 映射

    model_config = ModelConfig()
    cols = {}
    for col_name in dir(model):
        col = getattr(model, col_name)
        if isinstance(col, InstrumentedAttribute):
            if hasattr(col, 'primary_key') and col.primary_key:
                model_config.id_key = col_name
            type_info = TypeInfo()
            col_org_type = get_type(col).__class__.__name__
            col_base_type = COL_ORG_TYPE_MAP.get(col_org_type) or 'any'
            relation = get_relation_info(col)
            type_info.col_org_type = col_org_type
            type_info.col_base_type = col_base_type
            type_info.relation = relation
            cols[col_name] = type_info
    model_config.cols = cols
    return model_config


def get_model_col_names(model: Type[DeclarativeMeta]):
    columns = []
    for col_name in dir(model):
        if isinstance(getattr(model, col_name), InstrumentedAttribute):
            columns.append(col_name)

    return columns


class SalBase:
    __model_config: ModelConfig = None

    @classmethod
    @property
    def model_config(cls) -> ModelConfig:
        if cls.__model_config is None:
            cls.__model_config = 'process_ing'
            cls.__model_config = get_model_config(cls)
        return cls.__model_config

    def to_dict(self, include: List[str] = None, ex_include: List[str] = None):
        col_names = marge_col_names([c.name for c in self.__table__.columns], include, ex_include)
        data = {name: getattr(self, name) for name in col_names}
        return data

    def to_full_dict(self, include: List[str] = None, ex_include: List[str] = None, relation_use_id=False):
        col_names = marge_col_names(set(self.model_config.cols.keys()), include, ex_include)
        data = {}
        for col_name in col_names:
            col = self.model_config.cols[col_name]
            col_val = getattr(self, col_name)
            relation = col.relation
            if relation:
                if relation.relation_type == 'm2m':
                    if relation_use_id:
                        col_data = [getattr(item, relation.target_id_key) for item in col_val]
                    else:
                        col_data = [item.to_dict() for item in col_val]
                    data[col_name] = col_data
                elif relation.relation_type == 'o2m':
                    if relation_use_id:
                        col_data = [getattr(item, relation.target_id_key) for item in col_val]
                    else:
                        col_data = [item.to_dict() for item in col_val]
                    data[col_name] = col_data
                elif relation.relation_type == 'o2o':
                    data[col_name] = col_val and col_val.to_dict()
                elif relation.relation_type == 'm2o':
                    data[col_name] = col_val and col_val.to_dict()
                else:
                    data[col_name] = col_val
            else:
                data[col_name] = col_val
        return data


class TableModel(DeclarativeMeta, SalBase):
    pass


def handle_db_errors(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except IntegrityError as e:
            if 'UniqueViolation' in str(e.args):
                raise HTTPException(status_code=400, detail="数据重复")
            else:
                raise e

    return wrapper


class ModelCRUD:

    def __init__(self, model: Type[TableModel]):
        self.model = model

    @handle_db_errors
    def update(self, db: Session, data: BaseModel):
        model = self.model
        id_key = self.model.model_config.id_key
        item = db.query(model).filter(getattr(model, id_key) == (getattr(data, id_key))).first()
        data = data.dict(exclude_unset=True)
        col_names = get_model_col_names(model)
        model_config: ModelConfig = model.model_config
        for k, v in data.items():
            if '.' in k:
                paths = k.split('.')
                if paths[0] in col_names:
                    col_type_info: TypeInfo = model.model_config.cols[paths[0]]
                    if col_type_info.relation:
                        # 一对一时，{"config.age":1}
                        relation = col_type_info.relation
                        if relation.relation_type in ["o2o"]:
                            col = getattr(item, k)
                            if not col:
                                if v is not None:
                                    setattr(item, paths[0], relation.target_model(**{paths[1]: v}))
                            else:
                                setattr(col, paths[1], v)
            if k in col_names:
                col_type_info: TypeInfo = model.model_config.cols[k]
                # col = getattr(model, k)
                relation = col_type_info.relation
                if relation:
                    if relation.relation_type in ['m2m']:
                        if v is not None:
                            if v and type(v[0]) == dict:
                                v = [item[model_config.id_key] for item in v]
                            # 多对多更新的值是一个id数组，所以先用id查出对应项，再更新
                            target_model = relation.target_model
                            target_key = relation.target_id_key
                            update_values = {child for child in
                                             db.query(target_model).filter(getattr(target_model, target_key).in_(v))}
                            setattr(item, k, update_values)
                    elif relation.relation_type == 'o2o':
                        if type(v) == dict:
                            col = getattr(item, k)
                            if not col:
                                setattr(item, k, relation.target_model(**v))
                            else:
                                for v_k, v_v in v.items():
                                    setattr(col, v_k, v_v)

                else:
                    setattr(item, k, v)
        db.commit()
        db.refresh(item)
        return item

    @handle_db_errors
    def get(self, db: Session, data: BaseModel):
        item = db.query(self.model).filter(getattr(self.model, self.model.model_config.id_key) == (
            getattr(data, self.model.model_config.id_key))).first()
        return item

    @handle_db_errors
    def add(self, db: Session, data: BaseModel):

        model = self.model
        item = model()
        data = data.dict(exclude_unset=True)
        col_names = get_model_col_names(model)
        for k, v in data.items():
            if k in col_names:
                col_type_info: TypeInfo = model.model_config.cols[k]
                # col = getattr(model, k)
                relation = col_type_info.relation
                if relation:
                    if relation.relation_type == 'm2m':
                        if v is not None:
                            target_model = relation.target_model
                            update_values = {child for child in
                                             db.query(target_model).filter(
                                                 getattr(target_model, relation.target_id_key).in_(v))}
                            setattr(item, k, update_values)
                    elif relation.relation_type == 'o2o':
                        if type(v) == dict:
                            setattr(item, k, relation.target_model(**v))
                else:
                    setattr(item, k, v)
        db.add(item)
        db.commit()
        db.refresh(item)
        return item

    @handle_db_errors
    def query(self, db: Session, data: QueryParams) -> (int, List[Any]):
        return query_common(db, self.model, data)

    @handle_db_errors
    def delete(self, db: Session, data: BaseModel):
        db.query(self.model).filter(getattr(self.model, self.model.model_config.id_key) == (
            getattr(data, self.model.model_config.id_key))).delete()
        db.commit()
        return True


def make_link_table(base: declarative_base, left: str, right: str, table_name="") -> Table:
    link_table = Table(
        table_name or f"{left}__{right}",
        base.metadata,
        Column(f"{left}_id", ForeignKey(f"{left}.id", ondelete='CASCADE', onupdate='CASCADE'), primary_key=True),
        Column(f"{right}_id", ForeignKey(f"{right}.id", ondelete='CASCADE', onupdate='CASCADE'), primary_key=True),
    )
    return link_table
