import sqlite3
from typing import List, Dict, Any
import re

DB_PATH = "data/db.sqlite"

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

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

def mark_deleted(message_id: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE messages SET deleted=1, updated_at=strftime('%s','now') WHERE id=?", (message_id,))
    conn.commit()
    conn.close()

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
    conn = get_conn()
    cur = conn.cursor()
    safe = _fts5_safe_query(query)

    try:
        if safe:
            cur.execute("""
              SELECT
                m.id, m.text_norm, m.permalink, m.user_id, m.ts
              FROM messages_fts AS f
              JOIN messages AS m ON m.rowid = f.rowid
              WHERE m.deleted=0 AND m.channel_id=? AND f MATCH ?
              ORDER BY bm25(f) ASC, m.ts DESC
              LIMIT ?;
            """, (channel_id, safe, k))
            rows = [dict(r) for r in cur.fetchall()]
            conn.close()
            return rows
        else:
            # safe が空（全部記号など）の場合は空返し
            conn.close()
            return []
    except sqlite3.OperationalError:
        # FTS5 構文エラー時のフォールバック（部分一致）
        like = f"%{query}%"
        cur.execute("""
          SELECT m.id, m.text_norm, m.permalink, m.user_id, m.ts
          FROM messages AS m
          WHERE m.deleted=0 AND m.channel_id=? AND m.text_norm LIKE ?
          ORDER BY m.ts DESC
          LIMIT ?;
        """, (channel_id, like, k))
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return rows

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