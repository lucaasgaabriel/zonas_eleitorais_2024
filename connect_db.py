from pymongo import MongoClient
from pymongo.errors import BulkWriteError
from pymongo import UpdateOne


class MongoDB:
    def __init__(self, db_name):
        self.client = MongoClient("mongodb://localhost:27017/")
        self.db = self.client[db_name]

    def find_one_and_update(self, collection_name, document):
        collection = self.db[collection_name]
        return collection.find_one_and_update(
            filter={"id": document["id"]},
            update={"$set": document},
            upsert=True,
            return_document=True
        )

    def bulk_batch(self, collection_name, operations, batch_size=1000):
        collection = self.db[collection_name]
        for i in range(0, len(operations), batch_size):
            batch = operations[i:i + batch_size]
            try:
                result = collection.bulk_write(batch)
                print(f"Operações em lote concluídas. Documentos modificados: {result.modified_count}, Inseridos: {result.upserted_count}, Deletados: {result.deleted_count}")
            except BulkWriteError as bwe:
                print(f'Erro bulk: {bwe.details}')