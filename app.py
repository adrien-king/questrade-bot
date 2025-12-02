import os
import time
import logging

import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

# ------------------------------------------------------------------------------
# Config from environment
# ------------------------------------------------------------------------------

REFRESH_TOKEN = os.environ.get("QUESTRADE_REFRESH_TOKEN")
ACCOUNT_NUMBER = os.environ.get("QUESTRADE_ACCOUNT_NUMBER")
POSITION_DOLLARS = float(os.environ.get("POSITION_DOLLARS", "500"))
DRY_RUN = os.environ.get("DRY_RUN", "1") == "1"

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO))
logger = app.logger

if not REFRESH_TOKEN or not ACCOUNT_NUMBER:
    logger.warning("QUESTRADE_REFRESH_TOKEN or QUESTRADE_ACCOUNT_NUMBER is not set!")

# ------------------------------------------------------------------------------
# Questrade auth state
# ------------------------------------------------------------------------------

access_token = None
api_server = None
token_expiry = 0  # unix timestamp


def refresh_tokens_if_needed():
    """
    Refresh Questrade access token if we don't have one or it's about to expire.
    Updates global access_token, api_server, token_expiry, and REFRESH_TOKEN.
    """
    global access_token, api_server, token_expiry, REFRESH_TOKEN

    now = time.time()
    if access_token and now < token_expiry - 60:
        return  # still valid

    if not REFRESH_TOKEN:
        raise RuntimeError("No Questrade REFRESH_TOKEN configured")

    url = (
        "https://login.questrade.com/oauth2/token"
        f"?grant_type=refresh_token&refresh_token={REFRESH_TOKEN}"
    )
    logger.info("Refreshing Questrade tokens")
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    access_token = data["access_token"]
    api_server = data["api_server"].rstrip("/")  # e.g. https://api01.iq.questrade.com
    expires_in = int(data.get("expires_in", 1800))
    token_expiry = now + expires_in

    new_refresh = data.get("refresh_token")
    if new_refresh and new_refresh != REFRESH_TOKEN:
        REFRESH_TOKEN = new_refresh
        logger.warning(
            "Questrade returned a NEW refresh token. "
            "Update your Render env QUESTRADE_REFRESH_TOKEN if you redeploy."
        )

    logger.info(
        "Questrade auth OK. api_server=%s, expires_in=%s", api_server, expires_in
    )


def qt_request(method: str, path: str, **kwargs):
    """
    Make an authenticated request to the Questrade API.
    path should start with '/v1/...'
    """
    refresh_tokens_if_needed()
    headers = kwargs.pop("headers", {})
    headers["Authorization"] = f"Bearer {access_token}"
    url = api_server + path
    resp = requests.request(method, url, headers=headers, timeout=10, **kwargs)
    logger.info(
        "Questrade %s %s -> %s %s",
        method,
        path,
        resp.status_code,
        resp.text[:300],
    )
    resp.raise_for_status()
    if "application/json" in resp.headers.get("Content-Type", ""):
        return resp.json()
    return resp.text


# ------------------------------------------------------------------------------
# Helpers: symbol lookup, price, order placement
# ------------------------------------------------------------------------------

_symbol_cache = {}


def get_symbol_id(symbol: str) -> int:
    """
    Resolve symbol string like 'AHMA' to Questrade symbolId, with a simple cache.
    """
    symbol = symbol.upper()
    if symbol in _symbol_cache:
        return _symbol_cache[symbol]

    data = qt_request("GET", f"/v1/symbols/search?prefix={symbol}")
    for s in data.get("symbols", []):
        if s.get("symbol") == symbol:
            symbol_id = s["symbolId"]
            _symbol_cache[symbol] = symbol_id
            return symbol_id

    raise ValueError(f"Symbol '{symbol}' not found on Questrade")


def get_last_price(symbol_id: int) -> float:
    data = qt_request("GET", f"/v1/markets/quotes/{symbol_id}")
    quotes = data.get("quotes", [])
    if not quotes:
        raise ValueError(f"No quotes returned for symbolId={symbol_id}")
    q = quotes[0]
    last = q.get("lastTradePrice") or q.get("lastTradePriceTrHrs") or q.get("last")
    if last is None:
        raise ValueError(f"No last price in quote for symbolId={symbol_id}")
    return float(last)


def place_qt_order(symbol: str, side: str, event: str):
    """
    Place a MARKET order on Questrade based on TradingView event.
    - event: 'ENTRY' or 'EXIT'
    - side: 'long' (we only support long for now)
    """
    # TradingView sometimes sends "NASDAQ:AMZN" → strip prefix
    if ":" in symbol:
        symbol = symbol.split(":", 1)[1]

    symbol = symbol.upper()

    if event == "ENTRY" and side == "long":
        action = "Buy"
    elif event == "EXIT" and side == "long":
        action = "Sell"
    else:
        msg = f"Unsupported event/side combination: event={event}, side={side}"
        logger.warning(msg)
        return {"ok": False, "error": msg}

    symbol_id = get_symbol_id(symbol)
    last_price = get_last_price(symbol_id)

    qty = max(1, int(POSITION_DOLLARS // last_price))
    logger.info(
        "Computed quantity: symbol=%s symbolId=%s last=%.4f pos$=%.2f qty=%s",
        symbol,
        symbol_id,
        last_price,
        POSITION_DOLLARS,
        qty,
    )

    order = {
        "symbolId": symbol_id,
        "quantity": qty,
        "isAllOrNone": False,
        "isAnonymous": False,
        "orderType": "Market",
        "timeInForce": "Day",
        "action": action,
        "primaryRoute": "AUTO",
        "secondaryRoute": "AUTO",
    }

    if DRY_RUN:
        logger.info("[DRY RUN] Would send order: %s", order)
        return {"ok": True, "dry_run": True, "order": order}

    payload = {"accountNumber": ACCOUNT_NUMBER, "orders": [order]}
    res = qt_request("POST", f"/v1/accounts/{ACCOUNT_NUMBER}/orders", json=payload)
    return {"ok": True, "response": res}


# ------------------------------------------------------------------------------
# Flask routes
# ------------------------------------------------------------------------------

@app.route("/health", methods=["GET"])
def health():
    return "OK", 200


@app.route("/tv", methods=["GET", "POST"])
def tv():
    if request.method == "GET":
        return "Use POST with JSON body", 400

    raw_body = request.get_data(as_text=True)
    logger.info("TV HIT: method=%s path=%s body=%s", request.method, request.path, raw_body)

    try:
        data = request.get_json(force=True, silent=False)
    except Exception:
        logger.exception("Failed to parse JSON from TradingView")
        return jsonify({"ok": False, "error": "invalid JSON", "raw": raw_body}), 400

    symbol = str(data.get("symbol") or data.get("ticker") or "").strip()
    event = str(data.get("event") or "").strip().upper()
    side = str(data.get("side") or "long").strip().lower()

    if not symbol or not event:
        msg = "Missing symbol or event in payload"
        logger.warning("%s: %s", msg, data)
        return jsonify({"ok": False, "error": msg, "received": data}), 400

    try:
        trade_result = place_qt_order(symbol, side, event)
    except Exception as e:
        logger.exception("Error placing Questrade order")
        return jsonify({"ok": False, "error": str(e), "received": data}), 500

    return jsonify({"ok": True, "received": data, "trade": trade_result}), 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
