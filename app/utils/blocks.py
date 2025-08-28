from typing import List, Dict

def build_answer_blocks(answer: str, hits: List[dict]) -> List[Dict]:
    blocks = [
        {"type": "section", "text": {"type": "mrkdwn", "text": answer}},
        {"type": "divider"}
    ]
    if hits:
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "*Sources:*"}})
        for h in hits:
            # 例: #channel は今回は省略。必要なら呼び出し側で渡す。
            link = h.get("permalink")
            snippet = (h.get("text_norm") or "")[:120].replace("\n", " ")
            blocks.append({
                "type":"context",
                "elements":[
                    {"type":"mrkdwn","text": f"• <{link}|Open in Slack> – `{snippet}…`"}
                ]
            })
    return blocks

def build_date_time_picker(initial_date: str, initial_time: str) -> List[Dict]:
    return [
        {"type":"section","text":{"type":"mrkdwn","text":"ご都合の良い日付と時刻を選択してください。"}},
        {"type":"actions","elements":[
            {"type":"datepicker","action_id":"choose_date","initial_date":initial_date,
             "placeholder":{"type":"plain_text","text":"日付を選択"}},
            {"type":"timepicker","action_id":"choose_time","initial_time":initial_time,
             "placeholder":{"type":"plain_text","text":"時刻を選択"}},
            {"type":"button","action_id":"confirm_selection","text":{"type":"plain_text","text":"この内容で送信"},
             "style":"primary","value":"confirm"}
        ]}
    ]

def build_channel_picker(initial_channel: str | None = None) -> list[dict]:
    el = {
        "type": "conversations_select",
        "action_id": "pick_channel",
        "placeholder": {"type": "plain_text", "text": "検索対象チャンネルを選択"},
        "filter": {"include": ["public"], "exclude_archived": True}
    }
    if initial_channel:
        el["initial_conversation"] = initial_channel
    return [
        {"type": "section", "text": {"type": "mrkdwn", "text": "🔎 まず検索対象のチャンネルを選んでください。"}},
        {"type": "actions", "elements": [el]}
    ]