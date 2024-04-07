from rocketry import Rocketry
from rocketry.conditions.api import time_of_week
from rocketry.conds import every

import sys
from os import path

sys.path.append(path.abspath('.'))

from app.crawler.v3.main import main_crawler
from app.firebase.init_firebase import init_firebase


app = Rocketry(execution="async")


# Don't run the crawler on weekends
# Because notices are not updated on weekends
@app.task(every('3 hours', based="finish") & time_of_week.between("Mon", "Fri"))
async def crawler():
    await main_crawler()


if __name__ == "__main__":
    init_firebase()
    app.run()
