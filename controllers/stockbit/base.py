from datetime import datetime
import hashlib
import requests
import re
from urllib.parse import quote_plus
from controllers import Controllers
from controllers.stockbit.model import ModelTable
import json

class StockbitBase(Controllers):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.model = ModelTable(*args, **kwargs)
    
    async def insert_users(self, data: dict):
        try:
            user_id = data.get("userid")
            fullname = data.get("fullname", None)
            username = data.get("username", None)
            avatar = data.get("avatar", None)
            official = data.get("official")
            
            if official == 0:
                official = False
            else:
                official = True
            
            verified_status = data.get("verified_status", None)
            is_author = data.get("is_author", 0)
            is_pro = data.get("is_pro", 0)
            user_priv = data.get("user_priv", 0)
            follow = data.get("follow", 0)
            country = data.get("country", None)

            query = """
            INSERT INTO stockbit.stockbit_users(
            id, fullname, username, avatar, official, verified_status, is_author, is_pro, user_priv, follow, country
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            ) ON CONFLICT DO NOTHING
            """
            values = (user_id, fullname, username, avatar, official, verified_status, is_author, is_pro, user_priv, follow, country)

            self.conn.excecute_query(query=query, params=values)
            self.log.success(f"Successful insert data users {user_id}")
        
        except Exception as e:
            self.log.error(f"Error insert data users {e}")


        
    
    async def main(self):
        self.conn.test_connection()
        # self.model.create_table_users()
        # self.model.create_table_comment()
        # self.model.create_table_comment_topics()
        # self.model.create_table_comment_images()
        with open("stockbit_data.json", "r") as f:
            data = json.load(f)
        await self.insert_users(data)