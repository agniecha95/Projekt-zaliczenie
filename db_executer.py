from db_handler import Database

class Db_executer(Database):
    def __init__(self, db_name):
        super().__init__(db_name)