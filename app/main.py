from rocketry import Rocketry
from rocketry.conditions.api import time_of_week, time_of_day
from rocketry.conds import every

import sys
from os import path

sys.path.append(path.abspath('.'))

from app.crawler.v3.main import main_crawler
from app.firebase.init_firebase import init_firebase

app = Rocketry(execution="async")


# Don't run the crawler on weekends
# Because notices are not updated on weekends
@app.task(
    every('3 hours', based="finish") & time_of_day.between("09:00", "21:00") & time_of_week.between("Mon", "Fri")
)
async def crawler():
    await main_crawler()


# Run this crawler on weekends
# Because notices are not updated on weekends
# This crawler is for missing notice's of weekdays
@app.task(every('12 hours', based="finish") & time_of_day.between("09:00", "21:00") & time_of_week.between("Sat", "Sun"))
async def weekend_crawler():
    await main_crawler()


if __name__ == "__main__":
    init_firebase()
    app.run()
