import os
import logging
from flask import Flask, request, jsonify
import requests

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
log = app.logger

# ----------------------------
# Load environment variables
# ----------------------------

QUESTRADE_REFRESH_TOKEN = os.getenv("QUESTRADE_REFRESH_TOKEN")
QUESTRADE_ACCOUNT_NUMBER = os.getenv("QUESTRADE_ACCOUNT_NUMBER")
QUESTRADE_PRACTICE = os.getenv("QUESTRADE_PRACTICE", "1")
POSITION_DOLLARS = float(os.getenv("POSITION_DOLLARS", "10"))
RISK_PER_TRADE = float(os.getenv("RISK_PER_TRADE", "50"))
DRY_RUN = os.getenv("DRY_RUN", "1") == "1"

# Validate critical variables
if not QUESTRADE_REFRESH_TOKEN:
    raise ValueError("Missing Questrade refresh token.")

if not QUESTRADE_ACCOUNT_NUMBER:
    raise ValueError("Missing Questrade account number.")

# ----------------------------
# Build Questrade URLs
# ----------------------------
def qt_base_url():
    if QUESTRADE_PRACTICE == "1":
        return "https://api01.practice.questrade.com"
    return "https://api01.questrade.com"

# ----------------------------
# Refresh OAuth token
# ----------------------------
def qt_refresh_access_token():
    url = f"https://login.questrade.com/oauth2/token?grant_type=refresh_token&refresh_token={QUESTRADE_REFRESH_TOKEN}"
    r = requests.get(url)
    if r.status_code != 200:
        raise Exception(f"Failed to refresh token: {r.text}")

    data = r.json()
    access_token = data["access_token"]
    api_server = data["api_server"]
    return access_token, api_server

# ----------------------------
# Get last price for a symbol
# ----------------------------
def get_last_price(access_token, api_server, symbol):
    headers = {"Authorization": f"Bearer {access_token}"}
    r = requests.get(f"{api_server}v1/markets/quotes/{symbol}", headers=headers)
    data = r.json()
    return data["quotes"][0]["lastTradePrice"]

# ----------------------------
# Place order
# ----------------------------
def qt_place_order(symbol, side, risk_stop_pct):
    access_token, api_server = qt_refresh_access_token()
    headers = {"Authorization": f"Bearer {access_token}"}

    # Get latest price
    last_price = get_last_price(access_token, api_server, symbol)
    shares = max(1, int(POSITION_DOLLARS / last_price))

    order = {
        "symbol": symbol,
        "quantity": shares,
        "isBuy": True if side == "long" else False,
        "orderType": "Market",
        "timeInForce": "Day",
        "primaryRoute": "AUTO",
        "secondaryRoute": "AUTO"
    }

    if DRY_RUN:
        log.info(f"[DRY RUN] Would execute order: {order}")
        return {"dry_run": True, "order": order}

    r = requests.post(
        f"{api_server}v1/accounts/{QUESTRADE_ACCOUNT_NUMBER}/orders",
        json=order,
        headers=headers,
    )

    return r.json()


# ----------------------------
# Health endpoint
# ----------------------------
@app.route("/health", methods=["GET"])
def health():
    return "OK", 200


# ----------------------------
# TradingView webhook
# ----------------------------
@app.route("/tv", methods=["POST"])
def tv():
    raw = request.get_data(as_text=True)
    log.info(f"TV Webhook: {raw}")

    try:
        data = request.get_json(force=True)
    except:
        return jsonify({"error": "Invalid JSON"}), 400

    symbol = data.get("symbol", "").upper()
    event = data.get("event", "").upper()
    side = data.get("side", "").lower()
    risk_stop = float(data.get("risk_stop_pct", 2))

    if event == "BUY" and side == "long":
        result = qt_place_order(symbol, "long", risk_stop)
        return jsonify({"ok": True, "action": "buy", "result": result}), 200

    if event == "EXIT":
        return jsonify({"ok": True, "msg": "EXIT received — ignore for now"}), 200

    log.warning(f"Unsupported combo: event={event}, side={side}")
    return jsonify({"error": "Unsupported event"}), 400


# ----------------------------
# Run
# ----------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
