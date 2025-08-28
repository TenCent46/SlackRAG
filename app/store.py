import sqlite3
from typing import List, Dict, Any
import re
from opensearchpy import OpenSearch, RequestsHttpConnection
import json
import os

DB_PATH = "data/db.sqlite"

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# ---------- OpenSearch helpers ----------
def _os_client() -> OpenSearch:
    host = os.getenv("OPENSEARCH_HOST", "http://localhost:9200")
    user = os.getenv("OPENSEARCH_USER", "")
    password = os.getenv("OPENSEARCH_PASSWORD", "")
    if user:
        auth = (user, password)
    else:
        auth = None
    return OpenSearch(
        hosts=[host],
        http_auth=auth,
        use_ssl=host.startswith("https"),
        verify_certs=False,
        ssl_show_warn=False,
        connection_class=RequestsHttpConnection,
    )

def ensure_os_index():
    client = _os_client()
    index = os.getenv("OPENSEARCH_INDEX", "slack_messages")
    if client.indices.exists(index=index):
        return
    # Japanese-friendly analyzer (kuromoji). If plugin unavailable, it falls back to standard.
    settings = {
        "settings": {
            "index": {"number_of_shards": 1, "number_of_replicas": 0},
            "analysis": {
                "analyzer": {
                    "ja_kuromoji": {
                        "type": "custom",
                        "tokenizer": "kuromoji_tokenizer",
                        "filter": ["kuromoji_baseform","kuromoji_part_of_speech","ja_stop","kuromoji_stemmer","lowercase"]
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "id": {"type": "keyword"},
                "channel_id": {"type": "keyword"},
                "ts": {"type": "double"},
                "thread_ts": {"type": "double"},
                "user_id": {"type": "keyword"},
                "permalink": {"type": "keyword"},
                "text_norm": {
                    "type": "text",
                    "analyzer": "ja_kuromoji",
                    "search_analyzer": "ja_kuromoji"
                },
                "created_at": {"type": "date", "format": "epoch_second"},
                "updated_at": {"type": "date", "format": "epoch_second"},
                "deleted": {"type": "boolean"}
            }
        }
    }
    try:
        client.indices.create(index=index, body=settings)
    except Exception:
        # fallback to standard analyzer if kuromoji is unavailable
        settings["mappings"]["properties"]["text_norm"] = {"type": "text"}
        client.indices.create(index=index, body=settings)

def os_index_message(doc: Dict[str, Any]):
    client = _os_client()
    index = os.getenv("OPENSEARCH_INDEX", "slack_messages")
    ensure_os_index()
    # upsert by id
    #client.index(index=index, id=doc["id"], body=doc, refresh="true")
    client.index(index=index, id=doc["id"], body=doc)


def os_mark_deleted(message_id: str):
    client = _os_client()
    index = os.getenv("OPENSEARCH_INDEX", "slack_messages")
    if client.exists(index=index, id=message_id):
        client.update(index=index, id=message_id, body={"doc": {"deleted": True}})

def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS messages (
      id TEXT PRIMARY KEY,
      channel_id TEXT NOT NULL,
      ts TEXT NOT NULL,
      thread_ts TEXT,
      user_id TEXT,
      text_norm TEXT,
      permalink TEXT,
      created_at INTEGER,
      updated_at INTEGER,
      deleted INTEGER DEFAULT 0
    );
    """)
    # FTS5 仮想テーブル（全文検索用）
    cur.execute("""
    CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
      id UNINDEXED, text_norm, content='messages', content_rowid='rowid'
    );
    """)
    # トリガー（同期）
    cur.execute("""
    CREATE TRIGGER IF NOT EXISTS messages_ai AFTER INSERT ON messages BEGIN
      INSERT INTO messages_fts(rowid, id, text_norm) VALUES (new.rowid, new.id, new.text_norm);
    END;
    """)
    cur.execute("""
    CREATE TRIGGER IF NOT EXISTS messages_ad AFTER DELETE ON messages BEGIN
      INSERT INTO messages_fts(messages_fts, rowid, id, text_norm) VALUES('delete', old.rowid, old.id, old.text_norm);
    END;
    """)
    cur.execute("""
    CREATE TRIGGER IF NOT EXISTS messages_au AFTER UPDATE ON messages BEGIN
      INSERT INTO messages_fts(messages_fts, rowid, id, text_norm) VALUES('delete', old.rowid, old.id, old.text_norm);
      INSERT INTO messages_fts(rowid, id, text_norm) VALUES (new.rowid, new.id, new.text_norm);
    END;
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS user_prefs (
      user_id TEXT PRIMARY KEY,
      last_channel_id TEXT,
      updated_at INTEGER
    );
    """)
    conn.commit()
    conn.close()

def upsert_message(rec: Dict[str, Any]):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    INSERT INTO messages (id, channel_id, ts, thread_ts, user_id, text_norm, permalink, created_at, updated_at, deleted)
    VALUES (:id, :channel_id, :ts, :thread_ts, :user_id, :text_norm, :permalink, strftime('%s','now'), strftime('%s','now'), 0)
    ON CONFLICT(id) DO UPDATE SET
      text_norm=excluded.text_norm,
      permalink=COALESCE(excluded.permalink, messages.permalink),
      updated_at=strftime('%s','now'),
      deleted=0;
    """, rec)
    conn.commit()
    conn.close()
    # Also index to OpenSearch (upsert)
    try:
        os_doc = {
            "id": rec["id"],
            "channel_id": rec["channel_id"],
            "ts": float(rec["ts"]) if rec.get("ts") else 0.0,
            "thread_ts": float(rec["thread_ts"]) if rec.get("thread_ts") else 0.0,
            "user_id": rec.get("user_id"),
            "text_norm": rec.get("text_norm") or "",
            "permalink": rec.get("permalink"),
            "created_at": int(__import__("time").time()),
            "updated_at": int(__import__("time").time()),
            "deleted": False
        }
        os_index_message(os_doc)
    except Exception as e:
        # don't crash ingestion on OS failures
        pass

def mark_deleted(message_id: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE messages SET deleted=1, updated_at=strftime('%s','now') WHERE id=?", (message_id,))
    conn.commit()
    conn.close()
    try:
        os_mark_deleted(message_id)
    except Exception:
        pass

def _fts5_safe_query(q: str) -> str | None:
    """
    FTS5 の MATCH に安全に渡せるクエリへ正規化。
    - クォートや演算子を除去
    - 記号→スペース
    - 空白で AND 相当
    """
    if not q:
        return None
    # クォート/FTS演算子を除去
    q = q.replace('"', ' ').replace("'", " ")
    # 記号→スペース（日本語はそのまま通す）
    q = re.sub(r"[^\w\u3040-\u30FF\u31F0-\u31FF\u3000-\u303F\u4E00-\u9FFF]+", " ", q, flags=re.UNICODE)
    terms = [t for t in q.split() if t]
    if not terms:
        return None
    # FTS5 は空白=AND
    return " ".join(terms)

def search_top_k(query: str, channel_id: str, k: int = 5) -> List[Dict[str, Any]]:
    """
    Search via OpenSearch (BM25). Filters to channel_id and deleted=false.
    Uses query_string with AND semantics; supports Japanese via analyzer if available.
    """
    client = _os_client()
    index = os.getenv("OPENSEARCH_INDEX", "slack_messages")
    ensure_os_index()
    # sanitize empty query
    q = (query or "").strip()
    if not q:
        return []

    body = {
        "size": max(1, k),
        "query": {
            "bool": {
                "must": [
                    { "query_string": {
                        "query": q,
                        "default_field": "text_norm",
                        "default_operator": "AND"
                    }}
                ],
                "filter": [
                    {"term": {"channel_id": channel_id}},
                    {"term": {"deleted": False}}
                ]
            }
        },
        "sort": [
            {"_score": "desc"},
            {"ts": "desc"}
        ]
    }
    res = client.search(index=index, body=body)
    hits = []
    for h in res.get("hits", {}).get("hits", []):
        src = h.get("_source", {})
        hits.append({
            "id": src.get("id"),
            "text_norm": src.get("text_norm"),
            "permalink": src.get("permalink"),
            "user_id": src.get("user_id"),
            "ts": str(src.get("ts")),
        })
    return hits[:k]

def set_last_channel(user_id: str, channel_id: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
      INSERT INTO user_prefs (user_id, last_channel_id, updated_at)
      VALUES (?, ?, strftime('%s','now'))
      ON CONFLICT(user_id) DO UPDATE SET
        last_channel_id=excluded.last_channel_id,
        updated_at=strftime('%s','now');
    """, (user_id, channel_id))
    conn.commit()
    conn.close()

def get_last_channel(user_id: str) -> str | None:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT last_channel_id FROM user_prefs WHERE user_id=?", (user_id,))
    row = cur.fetchone()
    conn.close()
    return row["last_channel_id"] if row else None