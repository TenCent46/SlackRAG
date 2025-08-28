from typing import List, Dict
from store import search_top_k


def _prepare_match_query(q: str) -> str:
    """FTS5 MATCH 用のクエリ整形。

    - そのまま MATCH 構文を通す（ユーザーが `"exact phrase"` 等を使えるように）
    - ただし前後の空白は削除
    - 空の場合はワイルドカードにせず、呼び出し元で対処
    """
    return (q or "").strip()


def retrieve(query: str, channel_id: str, k: int = 5) -> List[Dict]:
    """RAG 用 BM25 リトリーバ。

    SQLite FTS5 の bm25 を使用してランキングします。bm25 はスコアが低いほど
    関連度が高いので、上位 k 件を返します。

    例: "token1 token2" で AND、'"exact phrase"' でフレーズ検索。
    """
    q = _prepare_match_query(query)
    if not q:
        return []

    # DB 側で bm25 によるランキングを実施
    # 必要に応じて候補件数を増やし、上位 k を返す設計にも変更可能
    hits = search_top_k(q, channel_id, k=max(1, k))
    return hits[:k]
