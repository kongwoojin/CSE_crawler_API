import inspect
import os.path
import time

from app.dataclass.enums.department import Department


def message_sent_succeed(department: Department, message_id):
    caller = os.path.splitext(os.path.basename(inspect.stack()[1].filename))[0]
    print("{} {:<24}: Successfully sent message to topic \"{}\"! message id: {}"
          .format(current_time(), caller, department.department.lower(), message_id))


def message_sent_failed(department: Department, error, retry_second):
    caller = os.path.splitext(os.path.basename(inspect.stack()[1].filename))[0]
    print("{} {:<24}: Error sending message to topic \"{}\"! error: {}. retry in {} seconds..."
          .format(current_time(), caller, department.department.lower(), error, retry_second))


def cannot_get_article_list(department: Department, board: str, e: Exception):
    caller = os.path.splitext(os.path.basename(inspect.stack()[1].filename))[0]
    print("{} {:<24}: Cannot get article list from department \"{}\" and board \"{}\" due to error: {}"
          .format(current_time(), caller, department.department, board, e))

def current_time():
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
