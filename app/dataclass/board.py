from dataclasses import dataclass
from typing import Optional

from app.dataclass.enums.category import Category


@dataclass
class Board:
    board: str | int
    article_url: str
    writer: Optional[str] = None
    is_notice: Optional[bool] = None
    category: Optional[Category] = None
