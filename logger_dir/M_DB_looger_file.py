import pymongo
from datetime import datetime, date
import os

connect_string = os.getenv("LOCAL_MONGO_DB_URL")

DIR = "Logs"


def log(log_message):
    current_time = datetime.now().strftime("%H:%M:%S")
    current_date = datetime.now().date()
    os.makedirs(DIR, exist_ok=True)
    log_file = open(DIR + "/database_Logs.txt", 'a+')
    log_file.write(str(current_date) + "/" + str(current_time) + "\n" + log_message + "\n")
    log_file.close()


class Mongo_Operations:

    def __init__(self):
        pass

    def mongo_connection(self):
        try:
            client = pymongo.MongoClient(os.getenv("LOCAL_MONGO_DB_URL"))
            log("Database connected successfully....\t")
            return client
        except Exception as e:
            log_file = open("Logs/database_Connection.txt", 'a+')
            log(log_message="Database connection not established....")

    def creating_db_collection(self, DATABASE_NAME, COLLECTION_NAME):
        try:
            Collection_List = self.mongo_connection()[DATABASE_NAME].list_collection_names()  ## show collection command
            mongo_connection = self.mongo_connection()
            ListOfAllDatabases = mongo_connection.list_database_names()  ## query give list of present databases
            if DATABASE_NAME not in ListOfAllDatabases:
                Database = mongo_connection[DATABASE_NAME]
                Collection = Database[COLLECTION_NAME]  # COLLECTION_NAME is as table name
                log("Database and Collection created....")
                return Collection

            elif DATABASE_NAME in ListOfAllDatabases and COLLECTION_NAME not in Collection_List:
                DATABASE_NAME = mongo_connection[DATABASE_NAME]
                Collection = DATABASE_NAME[COLLECTION_NAME]  # COLLECTION_NAME is as table name
                log("Database already Exit New Collection created ")
                return Collection

            elif DATABASE_NAME in ListOfAllDatabases and COLLECTION_NAME in Collection_List:
                DATABASE_NAME = mongo_connection[DATABASE_NAME]
                COLLECTION_NAME = DATABASE_NAME[COLLECTION_NAME]  # COLLECTION_NAME is as table name
                log("Database and Collection already exist")
                return COLLECTION_NAME

            else:
                log("BUG OCCURED..........")

        except Exception as e:
            log("error in database creation:: %s" % e)


    def insert_one_record(self, DATABASE_NAME, COLLECTION_NAME, RECORD):
        try:
            Record = self.creating_db_collection(DATABASE_NAME, COLLECTION_NAME)
            Record.insert_one(RECORD)
            log("one_record entered:: %s" % RECORD)
            print("record inserted")
        except Exception as e:
            log("Check db_NAME OR COLLECTION_NAME ALREADY EXIST:: %s" % e)


    def Insert_Multiple_Record(self, DATABASE_NAME, COLLECTION_NAME, MULTIPLE_RECORD):
        try:
            Multiple_Record = self.creating_db_collection(DATABASE_NAME, COLLECTION_NAME)
            Multiple_Record.insert_many(MULTIPLE_RECORD)
            log("one_record entered:: %s" % MULTIPLE_RECORD)
            print("rec inserted")
        except Exception as e:
            log("Error inserting multiple record:: %s" % e)

