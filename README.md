# CassandraDocumentStore for Haystack AI

A plug-in document store for [`haystack-ai`](https://github.com/deepset-ai/haystack), using Apache Cassandra 5.0+ with native vector search via SAI.

## ✅ Features

- Fully compatible with `haystack-ai`
- Works with `vector<float, N>` columns and ANN search (cosine similarity)
- Supports standard operations:
  - `write_documents()`
  - `query_by_embedding()`
  - `get_document_by_id()`, `get_all_documents()`, `delete_documents()` etc.

## ⚙️ Requirements

- Apache Cassandra 5.0+
- Python 3.8+
- `cassandra-driver`
- `haystack-ai>=1.0.0`

## 📦 Installation

```bash
pip install haystack-ai-cassandra

```

## License
Apache 2.0

## Usage

```python
from haystack.document_stores import CassandraDocumentStore
from haystack import Document

store = CassandraDocumentStore(
    host=["localhost"],
    keyspace="haystack",
    table="document",
    embedding_dim=1024
)

store.write_documents([
    Document(content="Hello world", embedding=[0.1] * 1024)
])

results = store.query_by_embedding([0.1] * 1024, top_k=1)
print(results[0].content)
