import builtins
import sys
import traceback


def print_error(*args, **kwargs):
    """
    打印错误使用
    :param args:
    :param kwargs:
    :return:
    """
    try:
        t = sys._getframe().f_back
        file = t.f_code.co_filename
        line = t.f_lineno
        print(f'File "{file}", line {line}')
    except Exception as e:
        print(e)
        pass
    print(*args, **kwargs)
    etype, value, tb = sys.exc_info()
    if value:
        for line in traceback.TracebackException(
                type(value), value, tb, ).format(chain=None):
            print(line, file=sys.stderr, end="")


def hook_print():
    """
    打印出具体打印位置和错误信息
    """
    help.print_error = print_error
    help.print = print_error


def hook_print_exception(only_print_local_file_error=True):
    """
    精简异常打印信息，只保留当前项目文件相关异常信息
    :param only_print_local_file_error:
    :return:
    """

    def print_exception_hook(etype, value, tb, limit=None, file=None, chain=True):
        if file is None:
            file = sys.stderr
        for line in traceback.TracebackException(
                type(value), value, tb, limit=limit).format(chain=chain):
            if only_print_local_file_error:
                if "site-packages" not in line:
                    print(line, file=file, end="")
            else:
                print(line, file=file, end="")

    traceback.print_exception = print_exception_hook
