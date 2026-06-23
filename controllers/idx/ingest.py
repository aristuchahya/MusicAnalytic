from controllers.idx.base import BaseIdx
from helpers.kafka import KafkaMessageConsumer
from sqlalchemy import select, create_engine, text
from controllers.idx.model import Base
from s3 import S3
import json

class IngestDaily(BaseIdx):

    async def main(self):
        with self.engine.connect() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS public"))
            conn.commit()
        Base.metadata.create_all(self.engine)


        consumer = KafkaMessageConsumer(
            topic="sc-company-idx-trading",
            bootstrap_servers=self.broker,
            group_id="consumer-idx"
        )

        for message in consumer.listen(self.handle_message):
            # print(message)
            self.insert_data(message)

class IngestHistorical(BaseIdx):

    async def main(self):
        s3_path = "s3://datalake/data/descriptive/idx/company_profiles/trading_info/metadata/stock/"

        files = S3.get_list_files(s3_path.replace("s3://datalake/", ""))

        for file in files:
            data = S3.get_read_file(file)

        # with open("downloads/AADI.json", "r") as f:
        #     data = json.load(f)
        
            self.insert_data_s3(data)

class IngestProfile(BaseIdx):

    async def main(self):
        with open("downloads/aadi.json", "r") as f:
            data = json.load(f)
        
        self.conn.test_connection()
        
        self.create_table()
        await self.ingest_profile(data)