import uuid
import time
from typing import List, Dict, Any

from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from haystack import Document
from haystack.document_stores.types import DocumentStore


class CassandraDocumentStore(DocumentStore):
    def __init__(
        self,
        host: List[str] = ["localhost"],
        port: int = 9042,
        embedding_dim: int = 1024,
        keyspace: str = "haystack",
        table: str = "document"
    ):
        time.sleep(15)

        self.cluster = Cluster(host, port=port, load_balancing_policy=DCAwareRoundRobinPolicy())
        self.session = self.cluster.connect()

        self.keyspace = keyspace
        self.table = table

        self.session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {self.keyspace}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """)

        self.session.set_keyspace(self.keyspace)

        self.session.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.table} (
                id text PRIMARY KEY,
                embedding vector<float, {embedding_dim}>,
                meta map<text, text>,
                content text
            )
        """)

        self.session.execute(f"""
            CREATE CUSTOM INDEX IF NOT EXISTS embedding_ann_index
            ON {self.table} (embedding)
            USING 'sai'
            WITH OPTIONS = {{'similarity_function': 'cosine'}}
        """)

    def write_documents(self, documents: List[Document]) -> int:
        count = 0
        for doc in documents:
            meta = {k: str(v) for k, v in (doc.meta or {}).items()}
            self.session.execute(
                f"""
                INSERT INTO {self.table} (id, embedding, meta, content)
                VALUES (%s, %s, %s, %s)
                """,
                [doc.id or str(uuid.uuid4()), doc.embedding, meta, doc.content]
            )
            count += 1
        return count
    
    def query_by_embedding(self, embedding: List[float], top_k: int = 5) -> List[Document]:
        rows = self.session.execute(
            f"""
            SELECT id, content, meta, embedding FROM {self.table}
            ORDER BY embedding ANN OF %s
            LIMIT %s
            """,
            [embedding, top_k]
        )
        return [
            Document(
                id=row.id,
                content=row.content,
                meta=dict(row.meta) if row.meta else {},
                embedding=row.embedding
            ) for row in rows
        ]

    def get_document_by_id(self, document_id: str) -> Document:
        row = self.session.execute(
            f"SELECT id, content, meta, embedding FROM {self.table} WHERE id = %s",
            [document_id]
        ).one()
        if row:
            return Document(
                id=row.id,
                content=row.content,
                meta=dict(row.meta) if row.meta else {},
                embedding=row.embedding
            )
        return None
    
    def get_documents_by_id(self, ids: List[str]) -> List[Document]:
        return [doc for doc in (self.get_document_by_id(doc_id) for doc_id in ids) if doc]

    def get_all_documents(self) -> List[Document]:
        rows = self.session.execute(f"SELECT id, content, meta, embedding FROM {self.table}")
        return [
            Document(
                id=row.id,
                content=row.content,
                meta=dict(row.meta) if row.meta else {},
                embedding=row.embedding
            ) for row in rows
        ]
    
    def delete_documents(self, document_ids: List[str]) -> Dict[str, Any]:
        deleted_ids = []
        not_found_ids = []
        for doc_id in document_ids:
            result = self.session.execute(
                f"SELECT id FROM {self.table} WHERE id = %s", [doc_id]
            ).one()
            if result:
                self.session.execute(
                    f"DELETE FROM {self.table} WHERE id = %s", [doc_id]
                )
                deleted_ids.append(doc_id)
            else:
                not_found_ids.append(doc_id)
        return {
            "deleted_count": len(deleted_ids),
            "deleted_ids": deleted_ids,
            "not_found_ids": not_found_ids
        }

    def delete_all_documents(self) -> None:
        self.session.execute(f"TRUNCATE {self.table}")

    def count_documents(self) -> int:
        return self.session.execute(f"SELECT COUNT(*) FROM {self.table}").one()[0]
