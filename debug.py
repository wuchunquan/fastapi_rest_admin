# -*- coding: utf-8 -*-
import builtins
from pathlib import Path

from config.log import uvicorn_log_config
from main import app
import uvicorn
from jurigged import watch
from context.common import conf

# debug环境

builtins_open = builtins.open


def open_hook(file, mode='r', buffering=None, encoding=None, errors=None, newline=None, closefd=True):
    f = builtins_open(file, mode, buffering, encoding, errors, newline, closefd)
    return f


def start_watch(ex_path=['venv']):
    builtins.open2 = open_hook
    ex_include_path_abs = [str(Path(item).absolute()) for item in ex_path]
    root_path = str(Path("./").absolute())

    def watch_pattern(filename: str):
        for path in ex_include_path_abs:
            if filename.startswith(path):
                return False
            if not filename.startswith(root_path):
                return False
        return filename

    watch(pattern=watch_pattern)


start_watch(['venv'])
if __name__ == '__main__':
    uvicorn.run(app, log_config=uvicorn_log_config, host="0.0.0.0", port=conf.app.port)
