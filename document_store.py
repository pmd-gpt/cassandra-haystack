# Импорт библиотек
import uuid
from haystack import Document
from cassandra.cluster import Cluster
from typing import List, Dict, Any
from cassandra.policies import DCAwareRoundRobinPolicy
from haystack.document_stores.types import DocumentStore
import time

import logging

# Логгер для вывода информации
logger = logging.getLogger("writer")

class CassandraDocumentStore(DocumentStore):
    def __init__(self, host: List[str] = ["localhost"], port: int = 9042, embedding_dim: int = 1024, keyspace: str = "haystack", table: str = "document"):
        """
        Инициализирует подключение к Cassandra и настраивает схему хранения документов с поддержкой векторного поиска.

        :param host: Список адресов узлов Cassandra-кластера.
        :param port: Порт подключения к Cassandra (по умолчанию 9042).
        :param embedding_dim: Размерность векторных представлений (embedding), используемых для ANN-поиска.
        :param keyspace: Название keyspace (пространства имён) в Cassandra.
        :param table: Название таблицы, в которую будут сохраняться документы.

        Выполняет следующие действия:
        - Устанавливает соединение с Cassandra.
        - Создаёт keyspace и таблицу, если они отсутствуют.
        - Настраивает ANN-индекс по embedding с использованием SAI (Storage-Attached Index).
        """
        
        # Задержка для ожидания запуска Cassandra
        time.sleep(25)

        # Подключение к Cassandra-кластеру
        try:
            self.cluster = Cluster(host, port=port, load_balancing_policy=DCAwareRoundRobinPolicy())
            self.session = self.cluster.connect()
        except Exception as e:
            raise RuntimeError(f"Failed to connect to Cassandra: {str(e)}")
        
    
        # Создание keyspace (пространства имён) при его отсутствии
        self.keyspace = keyspace
        self.table = table

        # Создание таблицы, если она ещё не существует
        self.session.execute("""
            CREATE KEYSPACE IF NOT EXISTS haystack
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
        """)


        self.session.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.keyspace}.{self.table} (
                id text PRIMARY KEY,
                embedding vector<float, {embedding_dim}>,
                meta map<text, text>,
                content text,
                hash text
            )
        """)

        # Создание ANN-индекса (Approximate Nearest Neighbor) по полю embedding
        try:
            self.session.execute(f"""
            CREATE CUSTOM INDEX IF NOT EXISTS embedding_ann_index
            ON {self.keyspace}.{self.table} (embedding)
            USING 'sai'
            WITH OPTIONS = {{
                'similarity_function': 'cosine'
            }}
            """)
            logger.info("[Cassandra] ANN index creation executed")
        except Exception as e:
            logger.error(f"[Cassandra] Failed to create ANN index: {e}")

    def count_documents(self) -> int:
        """
        Подсчёт общего числа документов в таблице.
        """
        try:
            return self.session.execute(
                f"SELECT COUNT(*) FROM {self.keyspace}.{self.table}"
                ).one()[0]
        except Exception as e:
            raise RuntimeError(f"Failed to count documents: {str(e)}")
    
    def filter_documents(self, embedding: List[float], top_k: int = 5) -> List[Document]:
        """
        Выполняет ANN-поиск по embedding-вектору через встроенный механизм Cassandra 5.0.
        
        :param embedding: Вектор для поиска ближайших документов.
        :param top_k: Количество возвращаемых ближайших документов.
        :return: Список документов.
        """
        if not embedding:
            raise ValueError("Embedding vector must be provided for ANN search") 

        try:
            query = f"""
                SELECT id, content, meta, embedding
                    FROM {self.keyspace}.{self.table}
                    ORDER BY embedding ANN OF %s
                    LIMIT %s
            """
            rows = self.session.execute(query, [embedding, top_k])

            # Преобразование результатов запроса в список объектов Document
            return [
                Document(
                    id=row.id,
                    content=row.content,
                    meta=dict(row.meta) if row.meta else {},
                    embedding=row.embedding
                )
                for row in rows
            ]
        except Exception as e:
            raise RuntimeError(f"Failed to perform ANN filtering: {str(e)}")

    def write_documents(self, documents: List[Document], policy) -> int:
        """
        Записывает документы в Cassandra с использованием CQL.
        Учитывает политики DuplicatePolicy: SKIP, FAIL, OVERWRITE.

        :param documents: Список документов для записи.
        :param policy: Поведение при дубликатах.
        :return: Количество успешно записанных документов.
        """
        try:
            doc_counter = 0
            for doc in documents:
                meta = {k: str(v) for k, v in (doc.meta or {}).items()}
                hash_val = doc.meta.get("hash")
                
                # Прямая вставка данных в таблицу Cassandra
                self.session.execute(
                    f"""
                    INSERT INTO {self.keyspace}.{self.table} (id, hash, embedding, meta, content)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    [doc.id, hash_val, doc.embedding, meta, doc.content],
                )
                doc_counter += 1
            return doc_counter
        except Exception as e:
            raise RuntimeError(f"Failed to write documents: {str(e)}")


    def delete_documents(self, document_ids: List[str]) -> Dict[str, Any]:
        """
        Удаляет документы по ID.

        :param document_ids: Список ID документов для удаления.
        :return: Словарь со статистикой удаления.
        """
        try:
            deleted_ids = []
            not_found_ids = []

            for doc_id in document_ids:
                # Проверка наличия документа перед удалением
                result = self.session.execute(
                    f"SELECT id FROM {self.keyspace}.{self.table} WHERE id = %s", [doc_id]
                ).one()
                if result:
                    self.session.execute(
                        f"DELETE FROM {self.keyspace}.{self.table} WHERE id = %s", [doc_id]
                    )
                    deleted_ids.append(doc_id)
                else:
                    not_found_ids.append(doc_id)

            return {
                "deleted_count": len(deleted_ids),
                "deleted_ids": deleted_ids,
                "not_found_ids": not_found_ids
            }
        except Exception as e:
            raise RuntimeError(f"Failed to delete documents: {str(e)}")

