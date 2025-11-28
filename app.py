import os
import time
import json
from typing import Dict, Any, Tuple

import requests
from flask import Flask, request, jsonify

# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------
REFRESH_TOKEN_ENV = "QUESTRADE_REFRESH_TOKEN"
PRACTICE_ENV = "QUESTRADE_PRACTICE"

REFRESH_TOKEN_FILE = "qt_tokens.json"

RISK_PER_TRADE = float(os.getenv("RISK_PER_TRADE", "50"))
MAX_POSITION_USD = float(os.getenv("MAX_POSITION_USD", "1000"))

ALLOWED_SYMBOLS = set(
    s.strip().upper() for s in os.getenv("ALLOWED_SYMBOLS", "").split(",") if s.strip()
)

app = Flask(__name__)

# ---------------------------------------------------------
# Token Persistence
# ---------------------------------------------------------
def _load_token_state() -> Dict[str, Any]:
    if os.path.exists(REFRESH_TOKEN_FILE):
        with open(REFRESH_TOKEN_FILE, "r") as f:
            return json.load(f)
    rt = os.environ.get(REFRESH_TOKEN_ENV)
    if not rt:
        raise RuntimeError("QUESTRADE_REFRESH_TOKEN not set")
    return {"refresh_token": rt}

def _save_token_state(state: Dict[str, Any]) -> None:
    with open(REFRESH_TOKEN_FILE, "w") as f:
        json.dump(state, f)

def _login_base_url() -> str:
    practice = os.getenv(PRACTICE_ENV, "0") == "1"
    return "https://practicelogin.questrade.com" if practice else "https://login.questrade.com"

# ---------------------------------------------------------
# OAuth Refresh
# ---------------------------------------------------------
def get_api_server_and_token() -> Tuple[str, str]:
    state = _load_token_state()
    now = time.time()

    if "access_token" in state and now < state.get("expires_at", 0):
        return state["api_server"], state["access_token"]

    url = f"{_login_base_url()}/oauth2/token"
    params = {
        "grant_type": "refresh_token",
        "refresh_token": state["refresh_token"]
    }
    resp = requests.get(url, params=params)
    resp.raise_for_status()
    data = resp.json()

    access_token = data["access_token"]
    new_refresh = data["refresh_token"]
    api_server = data["api_server"]
    expires_at = now + int(data.get("expires_in", 1800)) - 60

    state.update({
        "refresh_token": new_refresh,
        "access_token": access_token,
        "api_server": api_server,
        "expires_at": expires_at
    })
    _save_token_state(state)

    return api_server, access_token

def _auth_headers(token: str):
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

# ---------------------------------------------------------
# Questrade Helpers (read-only allowed for personal apps)
# ---------------------------------------------------------
def get_primary_account(api_server: str, token: str) -> str:
    url = f"{api_server}/accounts"
    resp = requests.get(url, headers=_auth_headers(token))
    resp.raise_for_status()
    accounts = resp.json()["accounts"]
    primary = next((a for a in accounts if a["isPrimary"]), accounts[0])
    return primary["number"]

def lookup_symbol_id(api_server: str, token: str, symbol: str) -> int:
    url = f"{api_server}/symbols/search"
    resp = requests.get(url, headers=_auth_headers(token), params={"prefix": symbol})
    resp.raise_for_status()
    matches = resp.json()["symbols"]
    match = next((m for m in matches if m["symbol"].upper() == symbol.upper()), None)
    if not match:
        raise RuntimeError(f"Symbol {symbol} not found")
    return match["symbolId"]

def get_last_price(api_server: str, token: str, symbol_id: int) -> float:
    url = f"{api_server}/markets/quotes/{symbol_id}"
    resp = requests.get(url, headers=_auth_headers(token))
    resp.raise_for_status()
    quotes = resp.json()["quotes"]
    return float(quotes[0]["lastTradePrice"])

# ---------------------------------------------------------
# Attempt Order Placement (may require partner permission)
# ---------------------------------------------------------
def place_market_order(symbol: str, side: str, risk_stop_pct: float):
    api_server, token = get_api_server_and_token()
    account = get_primary_account(api_server, token)
    symbol_id = lookup_symbol_id(api_server, token, symbol)
    price = get_last_price(api_server, token, symbol_id)

    risk_fraction = risk_stop_pct / 100
    qty_from_risk = int(RISK_PER_TRADE / (price * risk_fraction))
    qty_from_size = int(MAX_POSITION_USD / price)
    qty = max(1, min(qty_from_risk, qty_from_size))

    order = {
        "accountNumber": account,
        "symbolId": symbol_id,
        "quantity": qty,
        "orderType": "Market",
        "timeInForce": "Day",
        "action": "Buy" if side == "long" else "Sell",
        "primaryRoute": "AUTO",
        "secondaryRoute": "AUTO"
    }

    url = f"{api_server}/accounts/{account}/orders"
    resp = requests.post(url, headers=_auth_headers(token), json=order)

    if resp.status_code >= 400:
        raise RuntimeError(
            f"Order failed: {resp.status_code} {resp.text}"
        )

    return resp.json()

# ---------------------------------------------------------
# TradingView Webhook Endpoint
# ---------------------------------------------------------
@app.route("/tv", methods=["POST"])
def tv_webhook():
    data = request.get_json(silent=True) or {}
    symbol = data.get("symbol", "").upper()
    event = data.get("event", "")
    side = data.get("side", "long")
    risk_stop_pct = float(data.get("risk_stop_pct", 2))

    if not symbol or not event:
        return jsonify({"ok": False, "error": "Invalid payload"}), 400

    if ALLOWED_SYMBOLS and symbol not in ALLOWED_SYMBOLS:
        return jsonify({"ok": False, "error": "Symbol not allowed"}), 403

    try:
        if event == "ENTRY":
            res = place_market_order(symbol, side, risk_stop_pct)
            return jsonify({"ok": True, "action": "buy", "order": res})
        elif event == "EXIT":
            res = place_market_order(symbol, "sell", risk_stop_pct)
            return jsonify({"ok": True, "action": "sell", "order": res})
        else:
            return jsonify({"ok": False, "error": "Unknown event"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
