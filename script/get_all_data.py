import asyncio

from app.crawler.v3 import dorm_crawler, common_crawler, school_crawler
from app.dataclass.enums.department import Department
from script.last_page import get_dorm_last_page, get_school_last_page, get_common_last_page


async def main_common_crawler():
    # Remove Department.ACE because not logged-in user can't access the board
    departmentList = [Department.ARCH, Department.CSE, Department.MSE, Department.IDE, Department.ITE,
                      Department.MECHANICAL, Department.MECHATRONICS, Department.SIM]
    for department in departmentList:
        for index in range(0, department.board_len()):
            for i in range(1, await get_common_last_page(*department.code[index]) + 1):
                await common_crawler.manual_board_crawler(department, index, i, i)
                await asyncio.sleep(1.0)


async def main_school_crawler():
    department = Department.SCHOOL
    for index in range(0, department.board_len()):
        for i in range(1, await get_school_last_page(*department.code[index]) + 1):
            await school_crawler.manual_board_crawler(department, index, i, i)
            await asyncio.sleep(1.0)


async def main_dorm_crawler():
    department = Department.DORM
    for index in range(0, department.board_len()):
        for i in range(1, await get_dorm_last_page(department.code[index]) + 1):
            await dorm_crawler.manual_board_crawler(department, index, i, i)
            await asyncio.sleep(1.0)


async def start_crawler():
    await main_common_crawler()
    await main_school_crawler()
    await main_dorm_crawler()


asyncio.run(start_crawler())
