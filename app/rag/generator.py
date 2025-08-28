from typing import List, Dict
from llm.client import generate_llm_answer, trim_context

SYSTEM = """You are a Slack RAG assistant. Answer concisely in the language(s) of the user message (JA/EN mixed OK).
Use only the provided Slack context to answer. Include citations (Slack permalinks) for key claims.
If the context is insufficient, say so briefly and suggest a focused follow-up query."""

def build_context_snippets(hits: List[Dict]) -> str:
    lines = []
    for i, h in enumerate(hits, 1):
        txt = (h.get("text_norm") or "").replace("\n", " ").strip()
        link = h.get("permalink")
        lines.append(f"[{i}] {txt}\n<{link}>")
    return "\n\n".join(lines)

def generate_answer(query: str, hits: List[Dict]) -> str:
    ctx = build_context_snippets(hits) if hits else "NO CONTEXT"
    ctx = trim_context(ctx, max_chars=8000)
    prompt = f"""User query:
{query}

Slack context:
{ctx}

Instructions:
- Summarize relevant points and answer directly.
- Quote short key phrases when helpful.
- Add "Sources:" and list only the permalinks you relied on.
"""
    return generate_llm_answer(SYSTEM, prompt)