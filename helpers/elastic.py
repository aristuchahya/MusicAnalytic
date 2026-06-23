import json
import requests
from loguru import logger

class EsController:
    def __init__(self, base_url, index_name):
        self.base_url = base_url.rstrip('/')
        self.index_name = index_name
        self.logger = logger
        self.session = requests.Session()
        self.session.headers.update({'Content-Type': 'application/json'})

    def bulk_insert(self, id: str, docs: list):
        if not docs:
            return

        bulk_payload = ""

        for doc in docs:
            doc_id = doc.get(id)

            if not doc_id:
                self.logger.warning("Document without id, skipped")
                continue

            bulk_payload += json.dumps({
                "index": {
                    "_index": self.index_name,    
                    "_id": doc_id
                }
            }) + "\n"

            bulk_payload += json.dumps(doc) + "\n"

        url = f"{self.base_url}/_bulk"
        response = self.session.post(url, data=bulk_payload)

        if response.status_code >= 300:
            self.logger.error(f"Bulk insert failed: {response.text}")
        else:
            self.logger.info(f"Bulk inserted {len(docs)} documents to index {self.index_name}")
