import os, textwrap
from typing import List, Dict, Optional
from tenacity import retry, stop_after_attempt, wait_exponential

_PROVIDER = os.getenv("RAG_LLM_PROVIDER", "openai").lower()
_MODEL = os.getenv("RAG_LLM_MODEL")  # 任意

def _default_model() -> str:
    if _PROVIDER == "groq":
        return _MODEL or "llama-3.1-70b-versatile"
    return _MODEL or "gpt-5"

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=0.7, max=8))
def generate_llm_answer(system_prompt: str, user_prompt: str) -> str:
    """
    Provider-agnostic text generation.
    """
    if _PROVIDER == "groq":
        from groq import Groq
        client = Groq(api_key=os.environ["GROQ_API_KEY"])
        resp = client.chat.completions.create(
            model=_default_model(),
            messages=[
                {"role":"system","content":system_prompt},
                {"role":"user","content":user_prompt},
            ],
            temperature=0.2,
        )
        return resp.choices[0].message.content.strip()

    else:
        # OpenAI Responses API
        from openai import OpenAI
        client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
        resp = client.responses.create(
            model=_default_model(),
            input=[
                {"role":"system","content":system_prompt},
                {"role":"user","content":user_prompt},
            ],
            temperature=0.2,
        )
        return resp.output_text.strip()

def trim_context(snippets: str, max_chars: int = 8000) -> str:
    if len(snippets) <= max_chars:
        return snippets
    head = int(max_chars * 0.7)
    tail = max_chars - head
    return f"{snippets[:head]}\n...\n{snippets[-tail:]}"