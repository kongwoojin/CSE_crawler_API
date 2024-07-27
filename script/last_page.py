import re

import aiohttp
from bs4 import BeautifulSoup

from app.crawler.v3 import headers
from app.logs import crawling_log


async def get_common_last_page(mid: str, board_id: str):
    async with aiohttp.ClientSession() as session:
        async with session.get(f"https://www.koreatech.ac.kr/board.es?mid={mid}&bid={board_id}") as resp:
            if resp.status == 200:
                html = await resp.text()
                soup = BeautifulSoup(html, 'html.parser')

                try:
                    page = soup.select_one("#contents_body > div.board_pager > a.arr.last").get("href")
                    pattern = r"nPage=(\d+)"
                    match = re.search(pattern, page)
                    return int(match.group(1))

                except AttributeError:
                    crawling_log.unknown_last_page_error(str(resp.url))
                    return 1
            else:
                crawling_log.http_response_error(resp.status, str(resp.url))
                return 1


async def get_school_last_page(mid: str, board_id: str):
    async with aiohttp.ClientSession() as session:
        async with session.get(f"https://www.koreatech.ac.kr/notice/list.es?mid={mid}&board_id={board_id}") as resp:
            if resp.status == 200:
                html = await resp.text()
                soup = BeautifulSoup(html, 'html.parser')

                try:
                    page = soup.select_one("#contents_body > div.board_pager > a.arr.last").get("href")
                    pattern = r"page=(\d+)"
                    match = re.search(pattern, page)
                    return int(match.group(1))

                except AttributeError:
                    crawling_log.unknown_last_page_error(str(resp.url))
                    return 1
            else:
                crawling_log.http_response_error(resp.status, str(resp.url))
                return 1


async def get_dorm_last_page(mid: str, board_name: str):
    async with aiohttp.ClientSession() as session:
        async with session.get(
                f"https://dorm.koreatech.ac.kr/ko/{mid}/board/{board_name}/", headers=headers) as resp:
            if resp.status == 200:
                html = await resp.text()
                soup = BeautifulSoup(html, 'html.parser')

                try:
                    page_text = soup.select_one("body > div.subBoard > div > div.boardPage > a:last-child").get("href")
                    matches = re.findall(r"\d+(?:\.\d+)?", page_text)
                    return int(matches[0])
                except AttributeError:
                    crawling_log.unknown_last_page_error(str(resp.url))
                    return 1
            else:
                crawling_log.http_response_error(resp.status, str(resp.url))
                return 1
