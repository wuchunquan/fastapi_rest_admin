try:
    from utils.print_utils import hook_print

    hook_print()
except Exception as e:
    raise e
import os.path
from pathlib import Path
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.staticfiles import StaticFiles
from config.log import uvicorn_log_config
from context.common import conf, common_db


def init_app():
    app_conf = conf.app
    sys_name = app_conf.get('sys_name', 'admin')
    # 初始化app
    app = FastAPI(
        title=app_conf.get('title', '系统'),
        description=app_conf.get('description', '系统'),
        version=app_conf.get('version', "v1.0.0"),
        docs_url=f"/{sys_name}/docs",
        openapi_url=f"/{sys_name}/openapi.json",
    )

    # 挂载静态目录
    static_dir = Path(app_conf.get('static_dir', 'static'))
    if not os.path.exists(static_dir):
        os.makedirs(static_dir)
    app.mount(f"/{sys_name}/static", StaticFiles(directory=static_dir), name="static")

    # 跨域
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.router.prefix = f"/{sys_name}"
    help.print(f"接口文档链接:  http://127.0.0.1:{conf.app.port}{app.docs_url}")
    return app


app = init_app()




if __name__ == '__main__':
    uvicorn.run(app, log_config=uvicorn_log_config, host="0.0.0.0", port=conf.app.port)
