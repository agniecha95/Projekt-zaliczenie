import csv
import requests
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
            logger.error("Coronavirus cases analysis failed", exc_info=True)

    def create_coronavirus_vacinnes_table(self):
        sql_create_vacinnes_cases = """
            CREATE TABLE IF NOT EXISTS coronavirus_vacinnes(
                age_range text PRIMARY KEY, 
                both_sexes integer,
                males integer,
                females integer,
                first_dose integer,
                second_dose integer);
        """
        try:
            super().__init__(self.db_name)
            super().execute_sql(sql_create_vacinnes_cases)
        except Exception as e:
            logger.error(f"Can not create table analysis {e}")

    def insert_analysis_of_vacinnes(self, age_range, both_sexes, males, females, first_dose, second_dose):
        sql = """
            INSERT INTO coronavirus_vacinnes
            (age_range, both_sexes, males, females, first_dose, second_dose)
            VALUES (?,?,?,?,?,?)    
        """
        analysis_data = [age_range, both_sexes, males, females, first_dose, second_dose]
        try:
            super().__init__(self.db_name)
            super().execute_sql(sql, analysis_data)
        except Exception as e:
            logger.error(f"Can not insert new data from age range {age_range} {e}")
            exit()

    def vacinnes_analysis(self, path):
        try:
            with open(path,encoding="UTF8") as csv_file:
                all_row = csv.reader(csv_file, delimiter=',')
                next(csv_file)
                for row in all_row:
                    self.insert_analysis_of_vacinnes(age_range=row[0], both_sexes=row[1], males=row[2], females=row[3],first_dose=row[11], second_dose=row[12])
        except Exception as e:
            logger.error("Coronavirus vacinnes analysis failed", exc_info=True)

    def insert_doses_for_regions(self, region, administrated_doses, delivered_doses):
        sql = """
            INSERT INTO doses_for_regions
            (region, administrated_doses, delivered_doses)
            VALUES (?,?,?)    
        """
        analysis_data = [region, administrated_doses, delivered_doses]
        try:
            super().__init__(self.db_name)
            super().execute_sql(sql, analysis_data)
        except Exception as e:
            logger.error(f"Can not insert new data for region {region}  {e}")
            exit()

    def doses_analysis(self):
        try:
            req = requests.get('https://raw.githubusercontent.com/italia/covid19-opendata-vaccini/master/dati/vaccini-summary-latest.json')
            jsonResponse = req.json()

            for x in jsonResponse["data"]:
                self.insert_doses_for_regions(region=x["nome_area"], administrated_doses=x["dosi_somministrate"], delivered_doses=x["dosi_consegnate"])
        except Exception as e:
            logger.error("Doses analysis failed", exc_info=True)