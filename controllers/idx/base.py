import json
from s3 import S3
from controllers import Controllers
from sqlalchemy import select, create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy import select
from helpers.kafka import KafkaMessageConsumer
from controllers.idx.model import Base, TradingInfo, MetadataTable
from datetime import datetime
import hashlib
import requests
import re
from urllib.parse import quote_plus
from helpers.database import DatabaseDriver

class BaseIdx(Controllers):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.username = self.config.get("postgresql", "username")
        self.password = self.config.get("postgresql", "password")
        self.dbname = self.config.get("postgresql", "dbname")
        self.host = self.config.get("postgresql", "host")
        self.password = quote_plus(self.password)
        self.url = (
            f"postgresql+psycopg2://{self.username}:{self.password}@{self.host}:5432/{self.dbname}"
        )
        self.engine = create_engine(self.url, pool_pre_ping=True, pool_size=20, max_overflow=20, pool_timeout=30)
        self.Session = sessionmaker(autoflush=False, autocommit=False, bind=self.engine)
        self.topic = self.config.get("kafka", "topic")
        self.broker = self.config.get("kafka", "boostrap_servers")
        self.conn = DatabaseDriver()
    
    
    
    # def existing(self, model, id):
    #     session = self.Session()

    #     exist = (
    #         session.query(model)
    #         .filter(model.id == id)
    #         .first()
    #     )

    #     if exist:
    #         self.log.info(f"Data with id {id} in model {model} already exists")
    #         return True

    #     return False

    def existing(self, session, model, obj_id):
        return session.query(model).filter_by(id=obj_id).first() is not None
    
    def insert_data(self, data: dict):
        with self.Session() as session:
            try:
                metadata = data.get("metadata")
                data_idx = data.get("data")
                name = data.get("name")

                if not data_idx:
                    return
                
                link = metadata.get("link")
                source = metadata.get("source")
                tags = metadata.get("tags")
                path_data_raw = metadata.get("path_data")
                crawling_time_epoch = metadata.get("crawling_time")

                dt = datetime.fromtimestamp(crawling_time_epoch)
                crawling_time = dt.strftime("%Y-%m-%d %H:%M:%S")

                company_code = data_idx.get("SecurityCode")
                date_raw = data_idx.get("DTCreate")
                if not date_raw:
                    return

                dt_date = datetime.fromisoformat(date_raw)
                date = dt_date.strftime("%Y-%m-%d")

                open_price = data_idx.get("OpeningPrice")
                high = data_idx.get("HighestPrice")
                low = data_idx.get("LowestPrice")
                closing = data_idx.get("ClosingPrice")
                volume = data_idx.get("TradedVolume")
                value = data_idx.get("TradedValue")
                frequency = data_idx.get("TradedFrequency")

                combine_key = f"{link}-{source}-{path_data_raw}-{crawling_time}"
                metadata_id = self.generate_id(combine_key)

                combine_trading = f"{company_code}-{date}"
                trading_id = self.generate_id(combine_trading)

                if self.existing(session, MetadataTable, metadata_id):
                    self.log.info(f"Metadata sudah ada, skip: {link} - {date}")
                    return
                
                metadata = MetadataTable(
                    id=metadata_id,
                    link=link,
                    tags=tags,
                    source=source,
                    path_data_raw=path_data_raw,
                    crawling_time=crawling_time,
                )

                if self.existing(session, TradingInfo, trading_id):
                    self.log.info(f"Trading data sudah ada, skip: {company_code} - {date}")
                    return

                trading = TradingInfo(
                    id=trading_id,
                    company_code=company_code,
                    company_name=name,
                    date=date,
                    open_price=open_price,
                    high=high,
                    low=low,
                    closing=closing,
                    volume=volume,
                    value=value,
                    frequency=frequency,
                    metadata_id=metadata_id,
                )

                session.add(metadata)
                session.add(trading)
                self.log.success(f"Data with code {company_code} - {date} inserted")
                session.commit()
            except Exception as e:
                session.rollback()
                self.log.error(f"Error inserting data: {e}")

            finally:
                session.close()


    def insert_data_s3(self, data: dict):
        with self.Session() as session:
            try:
                # ambil metadata input
                link = data.get("link")
                source = data.get("source")
                tags = data.get("tags")
                path_data_raw = data.get("path_data")
                crawling_time_epoch = data.get("crawling_time")

                if not path_data_raw:
                    raise ValueError("path_data tidak ditemukan")

                if not crawling_time_epoch:
                    raise ValueError("crawling_time tidak ditemukan")

                # format crawling time
                dt = datetime.fromtimestamp(crawling_time_epoch)
                crawling_time = dt.strftime("%Y-%m-%d %H:%M:%S")

                # generate metadata id
                combine_key = f"{link}-{source}-{path_data_raw}-{crawling_time}"
                metadata_id = self.generate_id(combine_key)

                # skip kalau metadata sudah ada
                if self.existing(session, MetadataTable, metadata_id):
                    self.log.info(f"Metadata sudah ada, skip: {path_data_raw}")
                    return

                # convert s3 path ke endpoint local
                path_data = path_data_raw.replace(
                    "s3://",
                    "http://192.168.180.99:8000/"
                )

                # ambil file json
                response = requests.get(path_data, timeout=30)
                response.raise_for_status()

                res_json = response.json()

                company_code = res_json.get("KodeEmiten")
                company_name = res_json.get("name")
                replies = res_json.get("replies", [])

                if not company_code:
                    raise ValueError("KodeEmiten tidak ditemukan")

                # simpan metadata
                metadata = MetadataTable(
                    id=metadata_id,
                    link=link,
                    tags=tags,
                    source=source,
                    path_data_raw=path_data_raw,
                    crawling_time=crawling_time,
                )

                session.add(metadata)

                inserted_count = 0

                # loop trading data
                for reply in replies:
                    date_raw = reply.get("Date")

                    if not date_raw:
                        self.log.warning("Date kosong, skip 1 record")
                        continue

                    try:
                        dt_date = datetime.fromisoformat(
                            str(date_raw).replace("Z", "+00:00")
                        )
                        date = dt_date.strftime("%Y-%m-%d")

                    except Exception:
                        self.log.warning(f"Format Date invalid: {date_raw}")
                        continue

                    trading_id = self.generate_id(f"{company_code}-{date}")

                    # skip duplicate trading
                    if self.existing(session, TradingInfo, trading_id):
                        continue

                    trading = TradingInfo(
                        id=trading_id,
                        company_code=company_code,
                        company_name=company_name,
                        date=date,
                        open_price=reply.get("OpenPrice"),
                        high=reply.get("High"),
                        low=reply.get("Low"),
                        closing=reply.get("Close"),
                        volume=reply.get("Volume"),
                        value=reply.get("Value"),
                        frequency=reply.get("Frequency"),
                        metadata_id=metadata_id,
                    )

                    session.add(trading)
                    inserted_count += 1

                # commit transaksi
                session.commit()

                self.log.success(
                    f"Berhasil insert metadata + {inserted_count} trading data "
                    f"untuk {company_code}"
                )

            except Exception as e:
                session.rollback()
                self.log.error(f"Error inserting data: {e}")

            finally:
                session.close()

    
    
    def handle_message(self, message):
        
        if not message:
            self.log.warning("Data kosong dari kafka")
            return
        
        return message
    
    def create_table(self):
        try:
            query = """
            CREATE TABLE IF NOT EXISTS bronze.metadata_company_table (
                id TEXT PRIMARY KEY,
                link TEXT,
                tags TEXT[],
                source TEXT,
                path_data_raw TEXT,
                crawling_time TIMESTAMP,
                crawling_time_epoch BIGINT
            )

            CREATE TABLE IF NOT EXISTS public.company_profile_detail_idx (
                company_code TEXT,
                company_name TEXT,
                category TEXT,
                name TEXT,
                position TEXT,
                affiliated TEXT,
                independent BOOLEAN,
                summary FLOAT,
                type TEXT,
                percentage FLOAT,
                unit_percentage TEXT,
                total_assets FLOAT,
                location TEXT,
                operating_status TEXT,
                commercial_year INTEGER,
                metadata_id TEXT
            ) 
        """
            self.conn.excecute_query(query)
            self.log.success(" Create table success")
        except Exception as e:
            self.log.error(f"Error creating table: {e}")

    
    async def insert_metadata(self, data: dict):
        try:
            link = data.get("link")
            source = data.get("source")
            tags = data.get("tags")
            path_data = data.get("path_data")
            created_at = data.get("crawling_time")
            crawling_time = datetime.fromtimestamp(created_at).strftime("%Y-%m-%d %H:%M:%S")

            combine_key = f"{link}-{path_data}"

            metadata_id = self.generate_id(combine_key)

            query = """
            INSERT INTO bronze.metadata_company_table (
                id, link, tags, source, path_data_raw, crawling_time, crawling_time_epoch)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
            """

            self.conn.excecute_query(query=query, params=(
                metadata_id, link, tags, source, path_data, crawling_time, created_at
            ))
            self.log.success(f"Metadata inserted with id: {metadata_id}")
        except Exception as e:
            self.log.error(f"Error inserting metadata: {e}")
    

    async def ingest_profile(self, data: dict):
        try:
            link = data.get("link")
            path_data_raw = data.get("path_data")
            # crawling_time = data.get("crawling_time")
            if not path_data_raw:
                self.log.warning("path_data tidak ditemukan, skip")
                return

            combine_key = f"{link}-{path_data_raw}"

            metadata_id = self.generate_id(combine_key)
            
            path_data = path_data_raw.replace(
                "s3://",
                "http://192.168.180.99:8000/"
            )

            response = requests.get(path_data, timeout=30)

            if response.status_code == 200:
                res_json = response.json()

                if "Direktur" in res_json.get("data", {}):
                    category = "Director"
                elif "Komisaris" in res_json.get("data", {}):
                    category = "Commissioner"
                elif "KomiteAudit" in res_json.get("data", {}):
                    category = "Audit Committee"
                elif "PemegangSaham" in res_json.get("data", {}):
                    category = "Shareholder"
                elif "AnakPerusahaan" in res_json.get("data", {}):
                    category = "Subsidiary"

                profiles = res_json.get("data", {}).get("Profiles", [])
                komisaris = res_json.get("data", {}).get("Komisaris", [])
                direktur = res_json.get("data", {}).get("Direktur", [])
                komite_audit = res_json.get("data", {}).get("KomiteAudit", [])
                pemegang_saham = res_json.get("data", {}).get("PemegangSaham", [])
                anak_perusahaan = res_json.get("data", {}).get("AnakPerusahaan", [])

                for profile in profiles:
                    company_code = profile.get("KodeEmiten")
                    company_name = profile.get("NamaEmiten")
                
                for komis in komisaris:
                    name = komis.get("Nama")
                    position = komis.get("Jabatan")
                    independent = komis.get("Independen", False)
                
                for direkt in direktur:
                    name = direkt.get("Nama")
                    position = direkt.get("Jabatan")
                    affiliated = direkt.get("Afiliasi")
                
                for komite in komite_audit:
                    name = komite.get("Nama")
                    position = komite.get("Jabatan")
                
                for saham in pemegang_saham:
                    name = saham.get("Nama")
                    summary = saham.get("Jumlah")
                    percentage = saham.get("Persentase")
                    type = saham.get("Kategori")
                
                for anak in anak_perusahaan:
                    name = anak.get("Nama")
                    summary = anak.get("Jumlah")
                    type = anak.get("BidangUsaha")
                    percentage = anak.get("Persentase")
                    total_assets = anak.get("JumlahAset")
                    location = anak.get("Lokasi")
                    operating_status = anak.get("StatusOperasi")
                    commercial_year = anak.get("TahunKomersil")
                
                query = """
                INSERT INTO public.company_profile_detail_idx (
                    company_code,
                    company_name,
                    category,
                    name,
                    position,
                    affiliated,
                    independent,
                    summary,
                    type,
                    percentage,
                    unit_percentage,
                    total_assets,
                    location,
                    operating_status,
                    commercial_year,
                    metadata_id
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                ) ON CONFLICT (metadata_id) DO UPDATE SET
                """

                values = (
                    company_code,
                    company_name,
                    category,
                    name,
                    position,
                    affiliated,
                    independent,
                    summary,
                    type,
                    percentage,
                    "percent",
                    total_assets,
                    location,
                    operating_status,
                    commercial_year,
                    metadata_id
                )

                self.conn.excecute_query(query=query, params=values)
                self.log.success(f"Profile inserted with company name: {company_name} {company_code}")
        
        except Exception as e:
            self.log.error(f"Error inserting profile: {e}")
            





            
        

            

        




