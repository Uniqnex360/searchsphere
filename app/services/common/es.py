from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


class ElasticsearchService:
    def __init__(self, es: Elasticsearch, index: str):
        self.es = es
        self.index = index

    def create(self, doc_id: str | int, data: dict):
        """Create a document"""
        return self.es.index(index=self.index, id=doc_id, document=data)

    def update(self, doc_id: str | int, data: dict):
        """Update a document (partial update)"""
        return self.es.update(index=self.index, id=doc_id, doc={"doc": data})

    def upsert(self, doc_id: str | int, data: dict):
        return self.es.update(index=self.index, id=doc_id, doc=data, doc_as_upsert=True)

    def delete(self, doc_id: str | int):
        """Delete a document"""
        return self.es.delete(index=self.index, id=doc_id)

    def get(self, doc_id: str | int):
        """Get a document"""
        return self.es.get(index=self.index, id=doc_id)

    def search(self, query: dict):
        """Search documents"""
        return self.es.search(index=self.index, body=query)

    from elasticsearch.helpers import bulk


class ElasticsearchService:
    def __init__(self, es: Elasticsearch, index: str):
        self.es = es
        self.index = index

    def bulk(self, actions, raise_on_error=False):
        """
        Bulk insert/update with error visibility
        """
        for action in actions:
            action["_index"] = self.index

        success, errors = bulk(
            self.es, actions, raise_on_error=raise_on_error, stats_only=False
        )

        if errors:
            print("❌ Bulk Errors:")
            for err in errors[:5]:  # limit output
                print(err)

        return success, errors


class ElasticsearchIndexManager:
    """this class is used for index level operations"""

    def __init__(self, es: Elasticsearch):
        self.es = es

    def ensure_index(self, index: str, mapping: dict):
        if not self.es.indices.exists(index=index):
            self.es.indices.create(index=index, body=mapping)
            print(f"Created index: {index}")
        else:
            self.es.indices.put_mapping(index=index, body=mapping["mappings"])
            print(f"Updated mapping: {index}")
