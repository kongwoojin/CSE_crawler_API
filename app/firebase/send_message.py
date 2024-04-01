import asyncio

from firebase_admin.exceptions import FirebaseError
from app.dataclass.enums.department import Department
from firebase_admin import messaging

from app.db.v3.get_new_article import get_new_articles
from app.logs.message_log import message_sent_succeed, message_sent_failed


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

    message = messaging.Message(
        topic=department.department.lower(),
        notification=messaging.Notification(),
        data=data,
    )

    try:
        response = messaging.send(message)
        message_sent_succeed(department, response)
    except FirebaseError as e:
        retry_second = 5
        message_sent_failed(department, e, retry_second)
        await asyncio.sleep(retry_second)
        await send_fcm_message(department, board)
