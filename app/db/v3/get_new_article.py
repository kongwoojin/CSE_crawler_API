from app.dataclass.enums.department import Department
from app.db.v3 import edgedb_client
from app.logs.message_log import cannot_get_article_list


def get_new_articles(department: Department, board: str):
    client = edgedb_client()
    try:
        new_articles = client.query("""
            SELECT notice 
            { id, num, title, writer, write_date, read_count, 
            is_new := .init_crawled_time = .update_crawled_time } 
            FILTER .department=<Department><str>$department AND .board=<Board><str>$board 
            AND .is_new=true order by .is_notice DESC 
            THEN .write_date DESC
            THEN .num desc offset <int64>$offset limit <int64>$num_of_items
        """, department=department.department, board=board, offset=0, num_of_items=20)

        return list(new_articles)
    except Exception as e:
        cannot_get_article_list(department, board, e)
        return list()
