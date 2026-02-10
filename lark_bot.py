import time
import json
import requests
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

app = FastAPI()

# =========================================================
# 1) 必填：从 Lark Developer 后台复制
# =========================================================
APP_ID = "cli_a90b78bc4c381e1a"                 # 例：cli_a90b78bc4c381e1a
APP_SECRET = "HHT1YHnzY4KHv0JyU21pAdmtnhRz7nw0"                 # 例：HHT1YH...
VERIFICATION_TOKEN = "daHagmUJYtjLf0dQqdk3HfhEKZUmt5Vw"         # 例：daHagm...

# （可选）你没启用 Encrypt Key 就留空
ENCRYPT_KEY = ""


# =========================================================
# 2) tenant_access_token 缓存（避免频繁请求）
# =========================================================
_token_cache = {"token": None, "expire_at": 0}

def get_tenant_access_token() -> str:
    now = int(time.time())
    if _token_cache["token"] and now < _token_cache["expire_at"] - 60:
        return _token_cache["token"]

    url = "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal"
    payload = {"app_id": APP_ID, "app_secret": APP_SECRET}

    r = requests.post(url, json=payload, timeout=10)
    data = r.json()

    if data.get("code") != 0:
        # 这里最常见报错：app_secret 不对 / app 未发布没权限
        raise RuntimeError(f"Get tenant_access_token failed: {data}")

    token = data["tenant_access_token"]
    expire = int(data.get("expire", 3600))

    _token_cache["token"] = token
    _token_cache["expire_at"] = now + expire
    return token


# =========================================================
# 3) 发送消息：chat_id 方式（回声回复）
#    ✅ 重点：content 必须是 “字符串 JSON”
# =========================================================
def reply_to_chat(chat_id: str, text: str):
    token = get_tenant_access_token()
    url = "https://open.larksuite.com/open-apis/im/v1/messages?receive_id_type=chat_id"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8",
    }
    payload = {
        "receive_id": chat_id,
        "msg_type": "text",
        "content": json.dumps({"text": text}, ensure_ascii=False),  # 必须是字符串
    }

    r = requests.post(url, json=payload, headers=headers, timeout=10)

    # 打印结果，方便你定位“没回”的原因（权限/参数/发布状态）
    print("=== SEND MESSAGE API RESULT ===")
    print("status:", r.status_code)
    print("body:", r.text)

    try:
        return r.json()
    except Exception:
        return {"http_status": r.status_code, "raw": r.text}


# =========================================================
# 4) Webhook：接收事件
#    - url_verification: 返回 challenge
#    - event_callback: 收到消息则 Echo
# =========================================================
@app.post("/lark/webhook")
async def lark_webhook(req: Request):
    body = await req.json()

    # 4.1 URL 校验
    if body.get("type") == "url_verification":
        return JSONResponse({"challenge": body.get("challenge")})

    # 4.2 事件回调
    if body.get("type") == "event_callback":
        # token 校验（防止被别人伪造请求）
        if body.get("token") != VERIFICATION_TOKEN:
            print("!!! BAD TOKEN !!!", body.get("token"))
            return JSONResponse({"code": 1, "msg": "bad token"}, status_code=403)

        event = body.get("event", {})
        event_type = event.get("type", "")
        print("=== LARK EVENT TYPE ===", event_type)

        # 只处理“收到消息”
        if event_type == "im.message.receive_v1":
            message = event.get("message", {})
            chat_id = message.get("chat_id")

            # content 通常是字符串 JSON，例如：'{"text":"hi"}'
            content_raw = message.get("content", "")
            if isinstance(content_raw, str):
                try:
                    content = json.loads(content_raw)
                except Exception:
                    content = {"text": content_raw}
            else:
                content = content_raw or {}

            text = content.get("text", "")

            sender = event.get("sender", {})
            sender_type = sender.get("sender_type")
            print("chat_id:", chat_id, "sender_type:", sender_type, "text:", text)

            # 防止 bot 自己回自己
            if sender_type in ["app", "bot"]:
                return JSONResponse({"code": 0})

            if chat_id:
                resp = reply_to_chat(chat_id, f"Echo: {text}")
                print("=== REPLY RESULT JSON ===")
                print(resp)

        return JSONResponse({"code": 0})

    return JSONResponse({"code": 0})



