import os, re
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_bolt.response import BoltResponse
from rag.retriever import retrieve
from rag.generator import generate_answer
from utils.blocks import build_answer_blocks, build_date_time_picker, build_channel_picker
from store import init_db, get_last_channel, set_last_channel

load_dotenv()
app = App(token=os.environ["SLACK_BOT_TOKEN"])
ALLOWED = os.environ["ALLOWED_CHANNEL_ID"]

JST = timezone(timedelta(hours=9))

# 起動時にDB準備
init_db()

@app.command("/ask")
def on_ask(ack, body, client):
    ack()
    q = (body.get("text") or "").strip()
    user = body["user_id"]
    channel = body["channel_id"]

    if channel != ALLOWED:
        client.chat_postEphemeral(channel=channel, user=user,
            text="このコマンドは指定チャンネルでのみ使用可能である。")
        return

    # RAG: 素朴検索（後で生成を追加）
    hits = retrieve(q, ALLOWED, k=5)
    if not hits:
        answer = f"該当を見つけられなかった。検索語を変えて再試行してほしい。\n> `{q}`"
    else:
        # ここでは回答は簡潔に（将来 GPT-5 を組み合わせ）
        answer = f"問い合わせ: *{q}*\n上位の関連メッセージを返す。"

    # DMへ回答
    im = client.conversations_open(users=user)
    client.chat_postMessage(channel=im["channel"]["id"],
                            text="Answer",
                            blocks=build_answer_blocks(answer, hits))

    # ついでに日付・時刻の希望を聞く UI を送る例
    today = datetime.now(JST).strftime("%Y-%m-%d")
    client.chat_postMessage(channel=im["channel"]["id"],
                            text="Pick date/time",
                            blocks=build_date_time_picker(today, "10:00"))

# ---------- ② DMでの会話 ----------
@app.message(re.compile(r"^.*"))
def on_dm_message(message, say, client, logger, context):
    # DM以外は無視（チャンネルに書かれた雑談を拾わない）
    if message.get("channel_type") != "im":
        return

    user = message["user"]
    text = (message.get("text") or "").strip()

    # 既定チャンネルを取得
    last_ch = get_last_channel(user)

    # まだ設定されてなければ、ピッカーを提示して終了
    if not last_ch:
        say(blocks=build_channel_picker(), text="Choose a channel to search")
        return

    # 普通の問い合わせとして扱う
    try:
        hits = retrieve(text, last_ch, k=6)
        if not hits:
            answer = f"検索対象: <#{last_ch}>\n該当が見つからなかった。もう少し具体的に尋ねてください。"
        else:
            answer = generate_answer(text, hits)

        # 回答 + 出典
        say(blocks=build_answer_blocks(answer, hits), text="Answer")

    except Exception as e:
        logger.exception(e)
        say(text=f"内部エラーが発生しました。管理者に連絡してください。\n```{e}```")

# ---------- ③ チャンネル選択のハンドラ ----------
import re
@app.action("pick_channel")
def on_pick_channel(ack, body, client, say):
    ack()
    user = body["user"]["id"]
    selected = body["actions"][0]["selected_conversation"]  # "C...."
    set_last_channel(user, selected)

    # メッセージをその場で更新して、選ばれた旨を表示
    client.chat_update(
        channel=body["channel"]["id"],
        ts=body["message"]["ts"],
        text="検索対象チャンネルを設定しました。",
        blocks=[
            {"type":"section","text":{"type":"mrkdwn","text":f"✅ 検索対象を <#{selected}> に設定しました。以降はこのDMにメッセージを送るだけで検索できます。"}},
            {"type":"context","elements":[{"type":"mrkdwn","text":"変更したいときは『チャンネル変更』と送ってください。"}]}
        ]
    )

# ---------- ④ チャンネル変更のキーワード ----------
@app.message(re.compile(r"^(チャンネル変更|change channel)$", re.I))
def on_change_channel(message, say):
    say(blocks=build_channel_picker(), text="Choose a channel")

if __name__ == "__main__":
    SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"]).start()
