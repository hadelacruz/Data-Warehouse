"""
MongoDB data extractor module.
"""

from pymongo import MongoClient
from typing import Dict, List, Optional
from bson import ObjectId
from utils.logger import get_logger


class MongoExtractor:

    def __init__(self, uri: str, database_name: str):
        self.uri = uri
        self.database_name = database_name
        self.client = None
        self.db = None
        self.logger = get_logger(__name__)

    def connect(self) -> bool:
        try:
            self.client = MongoClient(self.uri)
            self.db = self.client[self.database_name]
            # Test connection
            self.client.admin.command('ping')
            self.logger.info(f"Connected to MongoDB database: {self.database_name}")
            return True
        except Exception as e:
            self.logger.error(f"Error connecting to MongoDB: {e}")
            return False

    def disconnect(self):
        if self.client:
            self.client.close()
            self.logger.info("Disconnected from MongoDB")

    def get_all_collections(self) -> List[str]:
        try:
            collections = self.db.list_collection_names()
            self.logger.info(f"Found {len(collections)} collections in MongoDB")
            return collections
        except Exception as e:
            self.logger.error(f"Error getting collections: {e}")
            return []

    def extract_collection(self, collection_name: str) -> List[Dict]:
        try:
            collection = self.db[collection_name]
            documents = list(collection.find())

            # Convert ObjectId to string for JSON serialization
            for doc in documents:
                if '_id' in doc and isinstance(doc['_id'], ObjectId):
                    doc['_id'] = str(doc['_id'])

            self.logger.info(f"Extracted {len(documents)} documents from collection '{collection_name}'")
            return documents
        except Exception as e:
            self.logger.error(f"Error extracting data from collection '{collection_name}': {e}")
            return []

    def extract_all_data(self) -> Dict[str, List[Dict]]:
        all_data = {}
        collections = self.get_all_collections()

        for collection in collections:
            data = self.extract_collection(collection)
            if data:
                all_data[collection] = data

        self.logger.info(f"Extracted data from {len(all_data)} collections")
        return all_data

    def get_collection_stats(self, collection_name: str) -> Optional[Dict]:
        try:
            stats = self.db.command("collstats", collection_name)
            return {
                'count': stats.get('count', 0),
                'size': stats.get('size', 0),
                'avgObjSize': stats.get('avgObjSize', 0)
            }
        except Exception as e:
            self.logger.error(f"Error getting collection stats for '{collection_name}': {e}")
            return None

    def get_sample_document(self, collection_name: str) -> Optional[Dict]:
        try:
            collection = self.db[collection_name]
            doc = collection.find_one()
            if doc and '_id' in doc and isinstance(doc['_id'], ObjectId):
                doc['_id'] = str(doc['_id'])
            return doc
        except Exception as e:
            self.logger.error(f"Error getting sample document from '{collection_name}': {e}")
            return None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
