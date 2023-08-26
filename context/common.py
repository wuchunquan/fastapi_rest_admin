from utils.config_utils import get_conf
from utils.mongo_utils import MongoConnect
from utils.redis_utils import RedisPool
from utils.sal_utils import SqlalchemyConnect

# 获取配置数据，并给v(value)添加类型提示
conf = get_conf()

# 数据库，一种类型的数据库尽量就一个
common_db = SqlalchemyConnect(**conf.database)

# redis连接池
redis_pool = RedisPool(**conf.redis)
redis_pool.connect()

# mongodb
mg_db = MongoConnect(**conf.mongo)

class ctx:
    conf = conf
    common_db = common_db
    redis_pool = redis_pool
