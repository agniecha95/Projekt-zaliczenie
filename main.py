import os
import time

from db_executer import Db_executer
from my_logger import logger

path_to_watch = r'.\files'
before = dict([(f, None) for f in os.listdir(path_to_watch)])

while 1:
    #db_name =r'E:\Moje\Python szkolenie\Python zaawansowany\Project\clinic.db'
    #db = Db_executer(db_name)
    after = dict([(f, None) for f in os.listdir(path_to_watch)])

    added = []
    for name in after:
        if not name in before:
            ext = os.path.splitext(name)[-1].lower()
            if ext == ".csv":
                added.append(name)

    removed = []
    for name in before:
        if not name in after:
            removed.append(name)

    if added:
        logger.info(f"Added : {added}")

    else:
        logger.info("Nothing was added")

    if removed:
        logger.info(f'Removed {removed}')
    else:
        logger.info('Nothing was removed')

    before = after 
    #db.close_conn()
    time.sleep(10)