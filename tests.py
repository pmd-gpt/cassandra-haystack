
import pytest
from cassandra_document_store import CassandraDocumentStore
from haystack.dataclasses import Document
import numpy as np

@pytest.fixture(scope="module")
def store():
    store = CassandraDocumentStore(
        host="127.0.0.1",
        port=9042,
        keyspace="test_haystack",
        embedding_dim=128
    )
    yield store
    store.delete_all_documents()

def test_write_and_get_documents(store):
    docs = [
        Document(content="First test document.", meta={"type": "test"}),
        Document(content="Second document.", meta={"type": "test"})
    ]
    store.write_documents(docs)
    all_docs = store.get_all_documents()
    contents = [doc.content for doc in all_docs]
    assert "First test document." in contents
    assert "Second document." in contents

def test_get_document_by_id(store):
    doc = Document(content="Unique document", meta={"id": "abc123"})
    store.write_documents([doc])
    all_docs = store.get_all_documents()
    assert len(all_docs) > 0
    retrieved = store.get_document_by_id(all_docs[0].id)
    assert retrieved.content == "Unique document"

def test_delete_documents(store):
    doc1 = Document(content="Doc1")
    doc2 = Document(content="Doc2")
    store.write_documents([doc1, doc2])
    all_docs = store.get_all_documents()
    ids = [doc.id for doc in all_docs if doc.content in ["Doc1", "Doc2"]]
    store.delete_documents(ids)
    remaining = store.get_all_documents()
    remaining_contents = [doc.content for doc in remaining]
    assert "Doc1" not in remaining_contents
    assert "Doc2" not in remaining_contents

def test_delete_all_documents(store):
    store.write_documents([
        Document(content="A"),
        Document(content="B")
    ])
    store.delete_all_documents()
    assert store.get_document_count() == 0

def test_count_documents(store):
    store.delete_all_documents()
    store.write_documents([
        Document(content="A"),
        Document(content="B"),
        Document(content="C")
    ])
    assert store.get_document_count() == 3

def test_query_by_embedding(store):
    store.delete_all_documents()
    docs = [
        Document(content="Vector doc 1", embedding=np.ones(128).tolist()),
        Document(content="Vector doc 2", embedding=np.zeros(128).tolist()),
        Document(content="Vector doc 3", embedding=(np.ones(128) * 0.5).tolist()),
    ]
    store.write_documents(docs)
    query_vec = np.ones(128).tolist()
    results = store.query_by_embedding(query_vec, top_k=2)
    assert len(results) == 2
    top_contents = [doc.content for doc in results]
    assert "Vector doc 1" in top_contents
