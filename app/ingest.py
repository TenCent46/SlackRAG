import os, re
from dotenv import load_dotenv
from slack_sdk import WebClient
from app.store import init_db, upsert_message

load_dotenv()
client = WebClient(token=os.environ["SLACK_BOT_TOKEN"])
ALLOWED = os.environ["ALLOWED_CHANNEL_ID"]

MENTION = re.compile(r"<@([A-Z0-9]+)>")

def normalize(text: str) -> str:
    return (text or "").strip()

def run_full_sync():
    init_db()
    cursor = None
    while True:
        resp = client.conversations_history(channel=ALLOWED, cursor=cursor, limit=200)
        for msg in resp.get("messages", []):
            ts = msg["ts"]
            text = normalize(msg.get("text", ""))
            # スレッド親 or 子の区別
            thread_ts = msg.get("thread_ts")
            # パーマリンク
            pl = client.chat_getPermalink(channel=ALLOWED, message_ts=ts)["permalink"]
            rec = {
                "id": f"{ALLOWED}-{ts}",
                "channel_id": ALLOWED,
                "ts": ts,
                "thread_ts": thread_ts,
                "user_id": msg.get("user"),
                "text_norm": text,
                "permalink": pl
            }
            upsert_message(rec)
        cursor = resp.get("response_metadata", {}).get("next_cursor")
        if not cursor:
            break
    print("Full sync done.")

if __name__ == "__main__":
    run_full_sync()