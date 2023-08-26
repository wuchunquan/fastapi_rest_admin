import redis as redis


class RedisPool:
    def __init__(self, host: str, port: int, password: str = None):
        self.host = host
        self.port = int(port)
        self.password = password if password else None
        self.redis_pool: redis.ConnectionPool = None
        self.conn: redis.Redis = None

    def connect(self, max_connections=10):
        if self.password:
            self.redis_pool = redis.ConnectionPool(host=self.host, port=self.port,
                                                   decode_responses=True,
                                                   max_connections=max_connections)
        else:
            self.redis_pool = redis.ConnectionPool(host=self.host, port=self.port,
                                                   password=self.password,
                                                   decode_responses=True,
                                                   max_connections=max_connections)
        self.conn = self.get_redis_client()

    def get_redis_client(self):
        conn = redis.Redis(connection_pool=self.redis_pool, decode_responses=True)
        return conn
