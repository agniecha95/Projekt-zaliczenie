import csv
from datetime import datetime
from db_handler import Database
from my_logger import logger

class Db_executer(Database):
    def __init__(self, db_name):
        super().__init__(db_name)

    def create_coronavirus_cases_table(self):
        sql_create_coronavirus_cases = """
            CREATE TABLE IF NOT EXISTS coronavirus_cases(
                date timestamp without time zone PRIMARY KEY,
                total_cases integer, 
                total_positive_cases integer,
                new_positive_cases integer,
                hospitalized integer);
        """
        try:
            super().__init__(self.db_name)
            super().execute_sql(sql_create_coronavirus_cases)
        except Exception as e:
            logger.error(f"Can not create table analysis {e}")

    def insert_analysis_of_cases(self, date, total_cases, total_positive_cases, new_positive_cases, hospitalized):
        sql = """
            INSERT INTO coronavirus_cases
            (date, total_cases, total_positive_cases, new_positive_cases, hospitalized)
            VALUES (?,?,?,?,?)    
        """
        analysis_data = [date, total_cases, total_positive_cases, new_positive_cases, hospitalized]
        try:
            super().__init__(self.db_name)
            super().execute_sql(sql, analysis_data)
        except Exception as e:
            logger.error(f"Can not insert new data from date {date} {e}")
            exit()

    def coronavirus_analysis(self, path):
        try:
            with open(path,encoding="UTF8") as csv_file:
                all_row = csv.reader(csv_file, delimiter=',')
                next(csv_file)
                for row in all_row:
                    self.insert_analysis_of_cases(date=row[0], total_cases=row[13], total_positive_cases=row[7], new_positive_cases=row[8],hospitalized=row[2])
        except Exception as e:
            logger.error("Coronavirus analysis failed", exc_info=True)