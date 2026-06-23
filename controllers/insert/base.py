import random

from controllers import Controllers
import pandas as pd
import json

class InsertData(Controllers):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    


    async def main(self):

        schema = """
                CREATE TABLE IF NOT EXISTS tb_assesment (
                    id_asesmen TEXT PRIMARY KEY,
                    id_kunjungan TEXT,
                    nama TEXT,
                    asmed TEXT,
                    jenis_asmed TEXT,
                    waktu_input TIMESTAMP,
                    user_input TEXT,
                    waktu_hapus TIMESTAMP                    
                )
                """

        # self.conn.excecute_query(schema)
        # self.log.success("Table created successfully")
        cities = ["Jakarta","Bandung","Surabaya","Semarang","Yogyakarta", "Denpasar", "Medan", "Bekasi", "Tangerang", "Tegal", "Malang", "Palembang", "Surakarta", "Bogor", "Pekalongan", "Jambi", "Banten"]
        educations = ["SD", "SMP", "SMA", "S1"]

        combine = [{
            "city": city,
            "education": education
        } for city in cities for education in educations
        ]

        with open("controllers/insert/tb_asesmen.csv", "r") as f:
            df = pd.read_csv(f, sep=";")

        df = df.where(pd.notna(df), None)
        
        datas = df[['id_asesmen', 'id_kunjungan', 'nama', 'asmed', 'jenis_asmed', 'waktu_input', 'user_input', 'waktu_hapus']].to_dict('records')


        query = """
        INSERT INTO public.tb_assesment (id_asesmen, id_kunjungan, nama, asmed, jenis_asmed, waktu_input, user_input, waktu_hapus) VALUES(
            %s, %s, %s, %s, %s, %s, %s, %s
        );
        """

        for data in datas:
            waktu_hapus = data['waktu_hapus']

            if pd.isna(waktu_hapus):
                waktu_hapus = None
            self.conn.excecute_query(query=query, params=(
                data['id_asesmen'], data['id_kunjungan'], data['nama'], data['asmed'], data['jenis_asmed'], data['waktu_input'], data['user_input'], waktu_hapus
            ))

        self.log.success("Data inserted successfully")

            

