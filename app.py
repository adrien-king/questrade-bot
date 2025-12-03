import os
import logging
import json
import requests
from flask import Flask, request, jsonify

# -----------------------------------------------------------------------------
# Basic Flask / logging setup
# -----------------------------------------------------------------------------
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
log = app.logger

# -----------------------------------------------------------------------------
# Environment variables
# -----------------------------------------------------------------------------
QUESTRADE_REFRESH_TOKEN = os.getenv("QUESTRADE_REFRESH_TOKEN", "").strip()
QUESTRADE_ACCOUNT_NUMBER = os.getenv("QUESTRADE_ACCOUNT_NUMBER", "").strip()
QUESTRADE_PRACTICE = os.getenv("QUESTRADE_PRACTICE", "1").strip()  # "1" = practice, "0" = live

POSITION_DOLLARS = float(os.getenv("POSITION_DOLLARS", "1000"))
RISK_PER_TRADE = float(os.getenv("RISK_PER_TRADE", "50"))  # not heavily used yet
DRY_RUN = os.getenv("DRY_RUN", "1") == "1"

# Simple validation at startup
if not QUESTRADE_REFRESH_TOKEN:
    log.warning("ENV: QUESTRADE_REFRESH_TOKEN is missing!")
if not QUESTRADE_ACCOUNT_NUMBER:
    log.warning("ENV: QUESTRADE_ACCOUNT_NUMBER is missing!")
log.info(
    "ENV: DRY_RUN=%s POSITION_DOLLARS=%.2f RISK_PER_TRADE=%.2f PRACTICE=%s",
    DRY_RUN,
    POSITION_DOLLARS,
    RISK_PER_TRADE,
    QUESTRADE_PRACTICE,
)

# -----------------------------------------------------------------------------
# Questrade helpers
# -----------------------------------------------------------------------------

OAUTH_URL = "https://login.questrade.com/oauth2/token"  # same for practice / live


def qt_refresh_access_token():
    """
    Exchange the refresh token for a short-lived access token + api_server.
    This is exactly the same call that worked in your browser.
    """
    if not QUESTRADE_REFRESH_TOKEN:
        raise Exception("QUESTRADE_REFRESH_TOKEN is not set")

    params = {
        "grant_type": "refresh_token",
        "refresh_token": QUESTRADE_REFRESH_TOKEN.strip(),
    }

    log.info(
        "QT: refreshing token at %s (practice=%s) token_prefix=%s...",
        OAUTH_URL,
        QUESTRADE_PRACTICE,
        QUESTRADE_REFRESH_TOKEN[:6],
    )

    r = requests.get(OAUTH_URL, params=params, timeout=10)

    log.info(
        "QT: token refresh response status=%s body=%s",
        r.status_code,
        r.text[:500],
    )

    if r.status_code != 200:
        raise Exception(f"Failed to refresh token: {r.text}")

    data = r.json()
    access_token = data.get("access_token")
    api_server = data.get("api_server")

    if not access_token or not api_server:
        raise Exception(f"Token response missing fields: {data}")

    log.info(
        "QT: token refreshed ok. api_server=%s access_token_prefix=%s...",
        api_server,
        access_token[:10],
    )

    return access_token, api_server


def get_last_price(access_token, api_server, symbol):
    """
    Fetch the last traded price for the symbol from Questrade.
    """
    url = f"{api_server}v1/markets/quotes/{symbol}"
    headers = {"Authorization": f"Bearer {access_token}"}

    log.info("QT: fetching last price for %s from %s", symbol, url)
    r = requests.get(url, headers=headers, timeout=10)
    log.info(
        "QT: last price response status=%s body=%s",
        r.status_code,
        r.text[:500],
    )

    if r.status_code != 200:
        raise Exception(f"Failed to fetch last price: {r.text}")

    data = r.json()
    price = data["quotes"][0]["lastTradePrice"]
    log.info("QT: last price for %s = %s", symbol, price)
    return float(price)


def qt_place_order(symbol, side, risk_stop_pct):
    """
    Place a Questrade order (or dry-run).

    side:
      - "long" with event BUY/ENTRY  -> submit a BUY order
      - "long" with event SELL/EXIT  -> submit a SELL order (close long)

    For now we assume "side" is always "long" from TradingView.
    """
    # 1) Get token + server
    access_token, api_server = qt_refresh_access_token()

    # 2) Get last price and compute shares
    last_price = get_last_price(access_token, api_server, symbol)
    raw_qty = POSITION_DOLLARS / last_price if last_price > 0 else 0
    shares = max(1, int(raw_qty))

    log.info(
        "QT: position sizing -> symbol=%s last_price=%.4f POSITION_DOLLARS=%.2f raw_qty=%.2f shares=%s",
        symbol,
        last_price,
        POSITION_DOLLARS,
        raw_qty,
        shares,
    )

    # 3) Build order payload
    is_buy = side == "long"  # we only handle long side for now
    order_side = "Buy" if is_buy else "Sell"

    order = {
        "accountNumber": QUESTRADE_ACCOUNT_NUMBER,
        "symbol": symbol,
        "quantity": shares,
        "isAllOrNone": False,
        "isAnonymous": False,
        "orderType": "Market",
        "timeInForce": "Day",
        "action": order_side,
    }

    log.info("QT: order payload: %s", json.dumps(order))

    if DRY_RUN:
        log.warning("QT: DRY_RUN=1 – NOT sending order to Questrade.")
        return {
            "dry_run": True,
            "order": order,
            "last_price": last_price,
            "shares": shares,
        }

    url = f"{api_server}v1/accounts/{QUESTRADE_ACCOUNT_NUMBER}/orders"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    log.info("QT: sending LIVE order to %s", url)
    r = requests.post(url, headers=headers, json=order, timeout=10)
    log.info("QT: order response status=%s body=%s", r.status_code, r.text[:500])

    if r.status_code >= 300:
        raise Exception(f"Order failed: status={r.status_code} body={r.text}")

    return r.json()


# -----------------------------------------------------------------------------
# Health check
# -----------------------------------------------------------------------------
@app.route("/health", methods=["GET"])
def health():
    log.info("HEALTH CHECK HIT")
    return "OK", 200


# -----------------------------------------------------------------------------
# TradingView webhook endpoint
# -----------------------------------------------------------------------------
@app.route("/tv", methods=["GET", "POST"])
def tv():
    if request.method == "GET":
        return "Use POST with JSON body", 400

    raw_body = request.get_data(as_text=True)
    log.info("TV Webhook raw body: %s", raw_body)

    try:
        data = request.get_json(force=True) or {}
    except Exception:
        log.exception("TV Webhook: failed to parse JSON")
        return jsonify(ok=False, error="Invalid JSON"), 400

    symbol = str(data.get("symbol", "")).upper().strip()
    event = str(data.get("event", "")).upper().strip()
    side = str(data.get("side", "")).lower().strip()
    risk_stop_pct = float(data.get("risk_stop_pct", RISK_PER_TRADE))

    log.info(
        "TV Webhook parsed -> symbol=%s event=%s side=%s risk_stop_pct=%s DRY_RUN=%s",
        symbol,
        event,
        side,
        risk_stop_pct,
        DRY_RUN,
    )

    # Basic validation
    if not symbol:
        return jsonify(ok=False, error="Missing symbol"), 400
    if event not in ("BUY", "SELL", "ENTRY", "EXIT"):
        log.warning("TV Webhook: unsupported event=%s", event)
        return jsonify(ok=False, error=f"Unsupported event {event}"), 400
    if side != "long":
        log.warning("TV Webhook: unsupported side=%s (only 'long' supported)", side)
        return jsonify(ok=False, error=f"Unsupported side {side}"), 400

    # Map TradingView events into actions
    is_entry = event in ("BUY", "ENTRY")
    is_exit = event in ("SELL", "EXIT")

    try:
        if is_entry:
            log.info("TV Webhook: ENTRY detected -> placing BUY order.")
            result = qt_place_order(symbol, "long", risk_stop_pct)
        elif is_exit:
            log.info("TV Webhook: EXIT detected -> placing SELL order.")
            # For now we still treat this as "long" but with SELL action in qt_place_order
            result = qt_place_order(symbol, "long", risk_stop_pct)
        else:
            # Just in case, though we already filtered above
            log.warning("TV Webhook: event=%s not mapped to action", event)
            return jsonify(ok=False, error=f"Event {event} not handled"), 400

        return jsonify(ok=True, result=result), 200

    except Exception as e:
        log.exception("ERROR:app:Exception on /tv [POST]")
        return jsonify(ok=False, error=str(e)), 500


# -----------------------------------------------------------------------------
# Local dev entrypoint
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
