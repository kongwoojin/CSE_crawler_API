import inspect
import os.path
import time

from app.dataclass.board import Board
from app.dataclass.enums.department import Department


def attribute_exception_error(url, exception):
    caller = os.path.splitext(os.path.basename(inspect.stack()[1].filename))[0]
    print("{} {:<24}: Failed to crawling {} due to attribute error, {}".format(current_time(), caller, url, exception))


def unknown_exception_error(exception):
    caller = os.path.splitext(os.path.basename(inspect.stack()[1].filename))[0]
    print("{} {:<24}: {}".format(current_time(), caller, exception))


def http_response_error(status, url: str):
    caller = os.path.splitext(os.path.basename(inspect.stack()[1].filename))[0]
    print("{} {:<24}: {} received when trying to crawling {}".format(current_time(), caller, status, url))


def article_crawling_log(data: Board, department: str):
    caller = os.path.splitext(os.path.basename(inspect.stack()[1].filename))[0]
    print("{} {:<24}: Start crawling article {} from {} of {}".format(current_time(), caller, data.article_url, data.board, department))


def board_crawling_log(department: str, board: str, page: int):
    caller = os.path.splitext(os.path.basename(inspect.stack()[1].filename))[0]
    print("{} {:<24}: Start crawling page {} from {} of {}".format(current_time(), caller, page, board, department))


def unknown_last_page_error(url: str):
    caller = os.path.splitext(os.path.basename(inspect.stack()[1].filename))[0]
    print("{} {:<24}: Failed to get last page from {}".format(current_time(), caller, url))


def current_time():
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
