import os

from json import dumps, loads
from botocore.exceptions import ClientError

from .connection import ConnectionS3

class S3:
    _instance = None
    _connection = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._connection = ConnectionS3()
            return cls._instance

    @classmethod
    def upload_json(cls, destination: str, body: dict, send: bool = True) -> int:
        def convert_to_serializable(obj):
            """Konversi objek yang tidak bisa di-serialize menjadi dictionary atau string"""
            if hasattr(obj, "__dict__"):
                return obj.__dict__
            return str(obj)
        cls()
        if send: 
            response: dict = cls._connection.s3.put_object(
                Bucket=cls._connection.bucket, 
                Key=destination, 
                Body=dumps(body, indent=4, ensure_ascii=False, default=convert_to_serializable)
                )
            
            return response['ResponseMetadata']['HTTPStatusCode']
        ...
    @classmethod
    def upload_file(cls, path: str, destination: str, send: bool = True) -> int:
        cls()
        if send: 
            response: dict = cls._connection.s3.put_object(
                                Bucket=cls._connection.bucket,
                                Key = destination, 
                                Body = open(path, 'rb')
                            )
            
            return response['ResponseMetadata']['HTTPStatusCode']
        ...
    @classmethod
    def upload(cls, body: any, destination: str, send: bool = True) -> int:
        cls()
        if send: 
            response: dict = cls._connection.s3.put_object(
                                Bucket=cls._connection.bucket,
                                Key = destination, 
                                Body = body
                            )
            
            return response['ResponseMetadata']['HTTPStatusCode']
        ...
    @classmethod
    def local2s3(cls, source: str) -> None:
        for root, dirs, files in os.walk(source.replace('\\', '/')):
            for file in files:
                file_path = os.path.join(root, file).replace('\\', '/')
                
    @classmethod
    def isExist(cls, path) -> bool:
        cls()
        try:
            cls._connection.s3.head_object(
                Bucket=cls._connection.bucket,
                Key= path
            )
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                return False

                # S3.upload_file(
                #     path=file_path,
                #     destination=file_path,
                # )
    
    @classmethod
    def get_file_size(cls, path: str) -> int:
        cls()
        response = cls._connection.s3.head_object(
        Bucket=cls._connection.bucket,
        Key=path
    )
        return response['ContentLength']
    
    @classmethod
    def get_read_file(cls, path: str) -> dict:
        cls()
        response = cls._connection.s3.get_object(
            Bucket=cls._connection.bucket,
            Key=path
        )
        data = response['Body'].read().decode('utf-8')
        return loads(data)
    
    @classmethod
    def get_list_files(cls, path: str) -> dict:
        cls()
        response = cls._connection.s3.list_objects_v2(
            Bucket=cls._connection.bucket,
            Prefix=path
        )
        if "Contents" not in response:
            return []
        
        return [obj["Key"] for obj in response["Contents"]]
