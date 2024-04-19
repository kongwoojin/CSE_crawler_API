import asyncio
import os

from firebase_admin.exceptions import FirebaseError
from app.dataclass.enums.department import Department
from firebase_admin import messaging

from app.db.v3.get_new_article import get_new_articles
from app.logs.message_log import message_sent_succeed, message_sent_failed

from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv(usecwd=True))

is_development = bool(os.getenv("IS_DEVELOPMENT"))
dev_tokens = os.getenv("FCM.DEV.TOKEN").split("\n")


async def send_fcm_message(department: Department, board: str):
    articles = get_new_articles(department, board)

    article_id_list = []

    for article in articles:
        article_id_list.append(str(article.id))

    data = {
        'screen': 'board',
        'department': department.department.lower(),
        'board': board,
        "new_articles": ':'.join(article_id_list)
    }

    messages = []

    if is_development:
        for token in dev_tokens:
            messages.append(
                messaging.Message(
                    token=token,
                    data=data
                )
            )
    else:
        messages.append(
            messaging.Message(
                topic=department.department.lower(),
                data=data,
            )
        )

    try:
        response = messaging.send_each(messages)
        message_sent_succeed(department, response)
    except FirebaseError as e:
        retry_second = 5
        message_sent_failed(department, e, retry_second)
        await asyncio.sleep(retry_second)
        await send_fcm_message(department, board)
