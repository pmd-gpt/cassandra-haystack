# Импорт библиотек
import hashlib
import logging
from haystack import component, Document
from typing import List
from .document_store import CassandraDocumentStore

# Логгер для вывода информации
logger = logging.getLogger("duplicate_filter")

@component
class DuplicateFilterByHash:
    def __init__(self, document_store: CassandraDocumentStore):
        """
        Инициализация фильтра. Требуется доступ к хранилищу документов для проверки наличия дубликатов.

        :param document_store: Объект CassandraDocumentStore для выполнения запросов к базе.
        """
        self.document_store = document_store

    @component.output_types(documents=List[Document])
    def run(self, documents: List[Document]):
        """
        Фильтрует входные документы, удаляя дубликаты на основе хеша содержимого.

        :param documents: Список входных документов для фильтрации.
        :return: Словарь с ключом "documents", содержащим список уникальных документов.
        """
        filtered_docs = []  # Список для хранения уникальных документов
        skipped_count = 0 # Счётчик отфильтрованных дубликатов

        for doc in documents:
            # Вычисление SHA-256 хеша по содержимому документа
            content_hash = hashlib.sha256(doc.content.encode("utf-8")).hexdigest()

            # Проверка, есть ли уже документ с таким хешем в базе
            existing_docs = self.document_store.session.execute(f"""
                SELECT meta FROM {self.document_store.keyspace}.{self.document_store.table}
                WHERE hash = %s ALLOW FILTERING
            """, [content_hash])

            if not list(existing_docs):
                # Если дубликат не найден — добавляем хеш в метадату и пропускаем документ дальше
                doc.meta = doc.meta or {}
                doc.meta["hash"] = content_hash
                filtered_docs.append(doc)
            else:
                # Если дубликат найден — игнорируем документ
                skipped_count += 1

        # Вывод статистики по фильтрации
        logger.info(f"DuplicateFilterByHash: {len(filtered_docs)} passed, {skipped_count} filtered out due to duplication.")
        return {"documents": filtered_docs}