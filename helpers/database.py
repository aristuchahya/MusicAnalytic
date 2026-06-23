import psycopg
from configparser import ConfigParser
from loguru import logger
from helpers.mongo import MongoDriver

class DatabaseDriver:
    def __init__(self, config_path: str = "config.ini"):
        # self.mongoo = MongoDriver('connections')
        # query = {'database': {'$regex': "savine"}}
        # get_connection = self.mongoo.search_data(query=query)
        # print(get_connection)
        self.config = ConfigParser()
        self.config.read(config_path)
        postgresql_config = self.config['postgresql']
        self.host = postgresql_config['host']
        self.port = postgresql_config['port']
        self.username = postgresql_config['username']
        self.password = postgresql_config['password']
        self.dbname = postgresql_config['dbname']
        self.connection = psycopg.connect(
            host=self.host,
            user=self.username,
            password=self.password,
            dbname=self.dbname,
            port=self.port
        )
        self.log = logger
    
    def test_connection(self):
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                if result and result[0] == 1:
                    self.log.info("Database connected successfully")
                    return True
                else:
                    self.log.error("Unexpected result from test query")
                    return False
        except Exception as e:
            self.log.error(f"Connection failed: {e}")
            return False
    
    def excecute_query(self, query: str, params: tuple = None, fetch: bool = False):
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
                self.connection.commit()
                if fetch:
                    return cursor.fetchall()
                return None
        except Exception as e:
            self.log.error(f"Error executing query: {e}")
    
    def get_query(self, query: str):
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()
                return result
        except Exception as e:
            self.log.error(f"Error executing query: {e}")

    def upsert_data(self, table_name: str, data: dict):
        try:
            columns = ", ".join(data.keys())
            values = ", ".join(f"'{value}'" for value in data.values())
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({values}) ON CONFLICT (id) DO UPDATE SET {', '.join(f'{key} = EXCLUDED.{key}' for key in data.keys())};"
            self.excecute_query(query)
        except Exception as e:
            self.log.error(f"Error upserting data: {e}")
    
    def close(self):
        if self.connection:
            self.connection.close()
            self.log.info("Database connection closed")
