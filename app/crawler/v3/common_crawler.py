import json
import re

import asyncio

import aiohttp
from aiohttp import ClientConnectorError
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import edgedb

from app.crawler.v3 import headers, gather_with_concurrency, ServerRefusedError, DAYS_TO_PARSE
from app.crawler.v3.utils.get_article_count import get_article_count, get_notice_article_count
from app.dataclass.board import Board
from app.dataclass.enums.department import Department
from app.db.v3 import edgedb_client
from app.firebase.send_message import send_fcm_message
from app.logs import crawling_log
from app.logs.crawling_log import cannot_read_article_error


async def article_parser(department: Department, session, data: Board):
    """
    Article parser

    This function parse article from article list

    :param department: department to crawl
    :param session: aiohttp session
    :param data: article list to parse
    """
    client = edgedb_client()
    now = datetime.now()

    board = data.board
    pattern = r"list_no=(\d+)"
    match = re.search(pattern, data.article_url)
    num = int(match.group(1))
    is_notice = data.is_notice

    crawling_log.article_crawling_log(data, department.name)

    try:
        async with session.get(data.article_url, headers=headers) as resp:
            # add small delay for avoid ServerDisconnectedError
            await asyncio.sleep(0.01)
            if resp.status == 200:
                html = await resp.text()
                soup = BeautifulSoup(html, 'html.parser')

                file_list = []

                try:
                    title_parsed = soup.select_one(
                        "#contents_body > article.board_view > h2").text.strip()
                    text_parsed = soup.select_one(
                        "#contents_body > article.board_view > div.contents").decode_contents()
                    text_parsed = text_parsed.replace("<img", "<br><img")

                    writer_parsed = soup.select_one(
                        "#contents_body > article.board_view > ul > li:nth-child(1) > span") \
                        .text.strip()
                    write_date_parsed = soup.select_one(
                        "#contents_body > article.board_view > ul > li.date > span").text.strip()
                    write_date_parsed = datetime.strptime(write_date_parsed, '%Y.%m.%d')
                    read_count_parsed = soup.select_one(
                        "#contents_body > article.board_view > ul > li.hit > span").text.strip().replace(",", "")
                    read_count_parsed = int(read_count_parsed)

                    files = soup.select("#contents_body > article.board_view > div.file > ul > li")

                    for file in files:
                        file_url = file.select_one("a")["href"]
                        file_name = file.select_one("a").text.strip()
                        file_name = re.sub("\[.*]", "", file_name).strip()

                        file_dic = {
                            "file_url": file_url,
                            "file_name": file_name
                        }

                        file_list.append(file_dic)
                except AttributeError:
                    cannot_read_article_error(data.article_url)
                    return

                try:
                    client.query("""
                    insert notice {
                        department := <Department><str>$department,
                        board := <Board><str>$board,
                        num := <int64>$num,
                        is_notice := <bool>$is_notice,
                        title := <str>$title,
                        writer := <str>$writer,
                        write_date := <cal::local_date>$write_date,
                        read_count := <int64>$read_count,
                        article_url := <str>$article_url,
                        content := <str>$content,
                        init_crawled_time := <cal::local_datetime>$crawled_time,
                        update_crawled_time:=<cal::local_datetime>$crawled_time,
                        notice_start_date:=<cal::local_date>$write_date,
                        notice_end_date:=<cal::local_date>'9999-12-31',
                        category:=<Category>Category.NONE,
                        files := (with
                                  raw_data := <json>$file_data,
                                  for item in json_array_unpack(raw_data) union (
                                    insert Files {
                                        file_name := <str>item['file_name'],
                                        file_url := <str>item['file_url']            
                                    } unless conflict on .file_url else (
                                    update Files
                                    set {
                                        file_name := <str>item['file_name'],
                                        file_url := <str>item['file_url']            
                                    }
                                    )
                                  )
                                  )
                    }
                """, department=department.department, num=num, board=board, title=title_parsed,
                                 writer=writer_parsed,
                                 write_date=write_date_parsed, read_count=read_count_parsed,
                                 article_url=data.article_url, content=text_parsed, crawled_time=now,
                                 file_data=json.dumps(file_list), is_notice=is_notice)

                except edgedb.errors.ConstraintViolationError:
                    client.query("""
                    update notice
                    filter .article_url = <str>$article_url
                    set {
                        title := <str>$title,
                        writer := <str>$writer,
                        is_notice := <bool>$is_notice,
                        write_date := <cal::local_date>$write_date,
                        read_count := <int64>$read_count,
                        content := <str>$content,
                        update_crawled_time := <cal::local_datetime>$crawled_time,
                        files := (with
                                  raw_data := <json>$file_data,
                                  for item in json_array_unpack(raw_data) union (
                                    insert Files {
                                        file_name := <str>item['file_name'],
                                        file_url := <str>item['file_url']            
                                    } unless conflict on .file_url else (
                                    update Files
                                    set {
                                        file_name := <str>item['file_name'],
                                        file_url := <str>item['file_url']            
                                    }
                                    )
                                  )
                                  )
                    }
                """, title=title_parsed, write_date=write_date_parsed, writer=writer_parsed,
                                 read_count=read_count_parsed, content=text_parsed, crawled_time=now,
                                 article_url=data.article_url, file_data=json.dumps(file_list),
                                 is_notice=is_notice)

            else:
                crawling_log.http_response_error(resp.status, data.article_url)
    except (Exception, ClientConnectorError) as e:
        print(e)
        raise ServerRefusedError(data)

    client.close()


async def article_list_crawler(session, department: Department, board_index: int, page: int, ignore_date=False):
    """
    Article list crawler

    This function crawl article list from page

    :param session: aiohttp session
    :param department: department to crawl
    :param board_index: index of board
    :param page: page to crawl
    :param ignore_date: if is True, crawl all articles, for manual crawling
    :return: article list
    """
    mid, board_id = department.code[board_index]

    board_list = []

    crawling_log.board_crawling_log(department.name, department.boards[board_index].name, page)

    url = f"https://www.koreatech.ac.kr/board.es?mid={mid}&bid={board_id}&nPage={page}"

    date_of_last_article = 0

    try:
        async with session.get(url, headers=headers) as resp:
            # add small delay for avoid ServerDisconnectedError
            await asyncio.sleep(0.01)
            if resp.status == 200:
                html = await resp.text()
                soup = BeautifulSoup(html, 'html.parser')

                posts = soup.select("#contents_body > div.board-list > table > tbody > tr")

                for post in posts:
                    try:
                        notice_icon = post.find("i", class_="icon_notice")
                        if notice_icon:
                            is_notice = True
                        else:
                            is_notice = False

                        if is_notice:
                            article_url_parsed = post.select_one("td.txt_left > strong > a").get("href")
                        else:
                            article_url_parsed = post.select_one("td.txt_left > a").get("href")
                        article_url_parsed = f"https://www.koreatech.ac.kr{article_url_parsed}"
                        article_url_parsed = re.sub("&nPage=\d*", "", article_url_parsed)
                        write_date_parsed = post.select_one(
                            f"td:nth-child({len(post.find_all('td')) - 2})").text.strip()
                        write_date_parsed = datetime.strptime(write_date_parsed, '%Y.%m.%d')

                        date_of_last_article = write_date_parsed

                        # If article older than 7 days, pass it
                        if not ignore_date and (datetime.today() - timedelta(days=DAYS_TO_PARSE) > write_date_parsed):
                            if not is_notice:  # If article is notice, don't pass
                                continue

                        board_list.append(Board(
                            board=department.boards[board_index].name,
                            article_url=article_url_parsed,
                            is_notice=is_notice
                        ))
                    except AttributeError:
                        crawling_log.no_article_error(url)
                        return []
                    except Exception as e:
                        crawling_log.unknown_exception_error(e)
    except (Exception, ClientConnectorError):
        raise ServerRefusedError(page)
    if ignore_date:
        return board_list
    else:
        if datetime.today() - timedelta(days=7) > date_of_last_article:
            return board_list
        else:
            board_list.extend(await article_list_crawler(session, department, board_index, page + 1))
            return board_list


async def board_remove_notice(department: Department, board: str):
    """
    Set notice articles to normal articles

    :param department: department to crawl
    :param board: board to crawl
    """
    client = edgedb_client()

    client.query("""
        update notice
        filter .department=<Department><str>$department AND .board=<Board><str>$board AND
          .is_notice=true
        set {
          is_notice := false
        };
    """, department=department.department, board=board)


async def parse_article_from_list(department, session, board_list):
    """
    Parse article from board list

    This function will call real article_parser
    :param department: department to crawl
    :param session: aiohttp session
    :param board_list: board list to parse
    """
    tasks = [asyncio.ensure_future(article_parser(department, session, data)) for data in board_list]
    result = await gather_with_concurrency(100, *tasks)

    failed_data = [i for i in result if i is not None]
    failed_data = [i.data for i in failed_data]

    if failed_data:
        await parse_article_from_list(department, session, failed_data)


async def manual_board_list_crawler(session, department: Department, board_index: int, start_page: int, last_page: int):
    """
    Board list crawler for manual crawling

    This function crawl article list of board from start_page to last_page

    :param session: aiohttp session
    :param department: department to crawl
    :param board_index: index of board
    :param start_page: start page to crawl
    :param last_page: last page to crawl
    :return: board list from start_page to last_page
    """
    board_list = []
    failed_page = []

    pages = [asyncio.ensure_future(article_list_crawler(session, department, board_index, page, True)) for page in
             range(start_page, last_page + 1)]
    datas = await gather_with_concurrency(100, *pages)
    for data in datas:
        try:
            board_list.extend(data)
        except TypeError:
            failed_page.append(int(data.data))

    failed_page.sort()

    if failed_page:
        board_list.extend(
            await manual_board_list_crawler(session, department, board_index, failed_page[0], failed_page[-1]))

    return board_list


async def manual_board_crawler(department: Department, board_index: int, start_page: int, last_page: int):
    """
    Board crawler for manual crawling

    This function should define start_page and last_page to crawl

    :param department: department to crawl
    :param board_index: index of board
    :param start_page: start page to crawl
    :param last_page: last page to crawl
    """
    # limit TCPConnector to 10 for avoid ServerDisconnectedError
    # Enable force_close to disable HTTP Keep-Alive
    connector = aiohttp.TCPConnector(limit=10, force_close=True)
    async with aiohttp.ClientSession(connector=connector) as session:
        board_list = await manual_board_list_crawler(session, department, board_index, start_page, last_page)

        await parse_article_from_list(department, session, board_list)


async def sched_board_crawler(department: Department, board_index: int):
    """
    Board crawler for scheduled crawling

    This function will crawl new articles only

    :param department: department to crawl
    :param board_index: index of board
    """
    old_count = get_article_count(department, department.boards[board_index].board)

    # Before start crawling, remove all notice
    await board_remove_notice(department, department.boards[board_index].board)

    # limit TCPConnector to 10 for avoid ServerDisconnectedError
    # Enable force_close to disable HTTP Keep-Alive
    connector = aiohttp.TCPConnector(limit=10, force_close=True)
    async with aiohttp.ClientSession(connector=connector) as session:
        board_list = await article_list_crawler(session, department, board_index, 1)

        await parse_article_from_list(department, session, board_list)

    new_count = get_article_count(department, department.boards[board_index].board)

    if new_count - old_count > 0:
        await send_fcm_message(department, department.boards[board_index].board)

    await check_article_removed(department, board_index)


async def remove_article(session, department: Department, board_index: int, notice_count: int, board_list=None,
                         page: int = 1):
    """
    Remove article from database which removed from board
    :param notice_count: Count of notice article
    :param board_index: Index of board
    :param session: aiohttp session
    :param department: department
    :param board_list: board list
    :param page: page of board
    """
    if board_list is None:
        board_list = await article_list_crawler(session, department, board_index, page, ignore_date=True)

    client = edgedb_client()
    get_notice_article_query = """
        SELECT notice 
            { id, num, title, writer, write_date, read_count, 
            is_new := .init_crawled_time = .update_crawled_time, is_notice, article_url }
            FILTER .department=<Department><str>$department AND .board=<Board><str>$board AND .is_notice=True order by 
            .write_date DESC THEN .num desc
        """

    get_article_query = """
        SELECT notice 
            { id, num, title, writer, write_date, read_count, 
            is_new := .init_crawled_time = .update_crawled_time, is_notice, article_url }
            FILTER .department=<Department><str>$department AND .board=<Board><str>$board AND .is_notice=False order by 
            .write_date DESC
            THEN .num desc offset <int64>$offset limit <int64>$num_of_items
        """

    num_of_items = 20

    db_board_list = (client.query(get_notice_article_query, department=department.department,
                                  board=department.boards[board_index].board)
                     + client.query(get_article_query, department=department.department,
                                    board=department.boards[board_index].board,
                                    offset=(page - 1) * num_of_items, num_of_items=num_of_items - notice_count))

    i = 0
    while i < num_of_items:
        article = board_list[i]
        db_article = db_board_list[i]

        if article.article_url != db_article.article_url:
            crawling_log.article_remove_log(department, department.boards[board_index].board, db_article.title)
            client.query("DELETE notice filter .id=<uuid>$id", id=db_article.id)
            await remove_article(session, department, board_index, notice_count, board_list, page)
            return
        i += 1

    is_removed = await compare_article_count(session, department, board_index)

    if is_removed:
        await remove_article(session, department, board_index, notice_count, None, page + 1)


async def compare_article_count(session, department: Department, board_index: int) -> bool:
    """
    Compare article count between database and board
    :param session: aiohttp session
    :param department: department
    :param board_index: index of board
    """
    article_count_in_db = get_article_count(department, department.boards[board_index].board)

    mid, board_id = department.code[board_index]

    url = f"https://www.koreatech.ac.kr/board.es?mid={mid}&bid={board_id}&nPage=1"

    try:
        async with session.get(url, headers=headers) as resp:
            # add small delay for avoid ServerDisconnectedError
            await asyncio.sleep(0.01)
            if resp.status == 200:
                html = await resp.text()
                soup = BeautifulSoup(html, 'html.parser')

                article_count_text = soup.select_one(
                    "#contents_body > div.board_info > p > span.total > b").text.replace(",", "")
                article_count = get_notice_article_count(department, department.boards[board_index].board)
                match = re.search(r"\d+(?:\.\d+)?", article_count_text)
                if match:
                    article_count += int(match.group(0))

                if article_count_in_db > article_count:
                    return True
                else:
                    return False

    except Exception as e:
        crawling_log.unknown_exception_error(e)
        return False


async def check_article_removed(department: Department, board_index: int):
    """
    Check article removed from board
    :param department: department
    :param board_index: index of board
    """
    notice_article_count_in_db = get_notice_article_count(department, department.boards[board_index].board)
    connector = aiohttp.TCPConnector(limit=10, force_close=True)
    async with aiohttp.ClientSession(connector=connector) as session:
        is_removed = await compare_article_count(session, department, board_index)
        if is_removed:
            await remove_article(session, department, board_index, notice_article_count_in_db)
