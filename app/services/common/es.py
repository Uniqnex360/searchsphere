from elasticsearch import Elasticsearch


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
        """Update if exists, else create"""
        return self.es.update(
            index=self.index, id=doc_id, doc={"doc": data}, doc_as_upsert=True
        )

    def delete(self, doc_id: str | int):
        """Delete a document"""
        return self.es.delete(index=self.index, id=doc_id)

    def get(self, doc_id: str | int):
        """Get a document"""
        return self.es.get(index=self.index, id=doc_id)

    def search(self, query: dict):
        """Search documents"""
        return self.es.search(index=self.index, body=query)
