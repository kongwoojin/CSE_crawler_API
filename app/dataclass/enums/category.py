from enum import Enum


def get_category(text: str):
    if text == "일반공지":
        return Category.NOTICE
    elif text == "대외활동":
        return Category.EA
    elif text == "교내활동":
        return Category.CA
    elif text == "근로장학생모집":
        return Category.WORK
    elif text == "":
        return Category.NONE
    else:
        return Category.ETC


def get_dorm_category(text: str):
    if "안전" in text:
        return Category.DORM_SAFETY
    elif "모집" in text:
        return Category.DORM_RECRUITMENT
    else:
        return Category.DORM_NOTICE


class Category(Enum):
    NONE = "NONE"
    NOTICE = "NOTICE"
    EA = "EA"  # External Activity
    CA = "CA"  # Campus Activity
    WORK = "WORK"
    ETC = "ETC"
    DORM_NOTICE = "DORM_NOTICE"
    DORM_SAFETY = "DORM_SAFETY"
    DORM_RECRUITMENT = "DORM_RECRUITMENT"

    def __init__(self, category):
        self.category = category

    def __str__(self):
        return self.value

    def __repr__(self):
        return self.value
