class Dev:
    class app:  # 系统配置
        title = "系统"
        sys_name = 'admin'
        port = 8888  # 端口

    class database:
        db_type = "postgresql"
        host = "127.0.0.1"
        user = 'postgres'
        password = "12345"
        db = 'fastapi_rest_admin'

    class redis:
        host = "127.0.0.1"
        port = 6379

    class mongo:
        host = "127.0.0.1"
        port = "27017"
        db = 'fastapi_rest_admin'


class Prod:
    class app:  # 系统配置
        port = 8888  # 端口

    class database:
        db_type = "postgresql"
        host = "127.0.0.1"
        user = 'postgres'
        password = "12345"
        db = 'fastapi_rest_admin'

    class redis:
        host = "127.0.0.1"
        port = 6379

    class mongo:
        host = "127.0.0.1"
        port = "27017"
        db = 'fastapi_rest_admin'
