import os
import logging
from flask import Flask, request, jsonify
import requests
import json

# --------------------------------------------------------------------
# Basic Flask + logging
# --------------------------------------------------------------------
app = Flask(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
logger = app.logger

# --------------------------------------------------------------------
# Environment variables
# --------------------------------------------------------------------
QUESTRADE_REFRESH_TOKEN = os.getenv("QUESTRADE_REFRESH_TOKEN")
QUESTRADE_ACCOUNT_NUMBER = os.getenv("QUESTRADE_ACCOUNT_NUMBER")
QUESTRADE_PRACTICE = os.getenv("QUESTRADE_PRACTICE", "1")
POSITION_DOLLARS = float(os.getenv("POSITION_DOLLARS", "1000"))
RISK_PER_TRADE = float(os.getenv("RISK_PER_TRADE", "50"))
DRY_RUN = os.getenv("DRY_RUN", "1") == "1"

if not QUESTRADE_REFRESH_TOKEN:
    raise RuntimeError("Missing QUESTRADE_REFRESH_TOKEN env var")
if not QUESTRADE_ACCOUNT_NUMBER:
    raise RuntimeError("Missing QUESTRADE_ACCOUNT_NUMBER env var")

# --------------------------------------------------------------------
# Base URL for trading API (not login)
# --------------------------------------------------------------------
def get_base_api_url(api_server: str) -> str:
    # api_server from Questrade already includes https:// and trailing slash
    return api_server.rstrip("/")

# --------------------------------------------------------------------
# Refresh access token  *** UPDATED ***
# --------------------------------------------------------------------
def qt_refresh_access_token():
    """Refresh Questrade access token using the REFRESH token.

    Uses practicelogin.questrade.com when QUESTRADE_PRACTICE=1.
    Adds detailed logging so we can see exactly what Questrade returned.
    """
    practice = (QUESTRADE_PRACTICE == "1")
    login_host = "https://practicelogin.questrade.com" if practice else "https://login.questrade.com"

    params = {
        "grant_type": "refresh_token",
        "refresh_token": QUESTRADE_REFRESH_TOKEN,
    }

    url = f"{login_host}/oauth2/token"

    logger.info(
        "QT: refreshing token at %s (practice=%s) token_prefix=%s",
        url,
        practice,
        QUESTRADE_REFRESH_TOKEN[:8],
    )

    try:
        r = requests.get(url, params=params, timeout=10)
    except Exception as e:
        logger.exception("QT: exception while calling token endpoint: %s", e)
        raise

    # Log full status + first part of body so we can diagnose 400s
    logger.info(
        "QT: token refresh response status=%s body=%s",
        r.status_code,
        r.text[:500],
    )

    if r.status_code != 200:
        # This is exactly the error you’re seeing now.
        # In practice, 400 here usually means:
        #   - wrong host (login vs practicelogin), or
        #   - refresh token is invalid/expired/already used.
        raise Exception(f"Failed to refresh token: {r.status_code} {r.text}")

    data = r.json()
    access_token = data["access_token"]
    api_server = data["api_server"]

    logger.info("QT: token refresh OK, api_server=%s, expires_in=%s", api_server, data.get("expires_in"))
    return access_token, api_server

# --------------------------------------------------------------------
# Get last price
# --------------------------------------------------------------------
def get_last_price(access_token, api_server, symbol):
    headers = {"Authorization": f"Bearer {access_token}"}
    url = f"{get_base_api_url(api_server)}/v1/markets/quotes/{symbol}"
    logger.info("QT: fetching last price for %s from %s", symbol, url)
    r = requests.get(url, headers=headers, timeout=10)
    logger.info("QT: last price status=%s body=%s", r.status_code, r.text[:300])
    r.raise_for_status()
    data = r.json()
    return data["quotes"][0]["lastTradePrice"]

# --------------------------------------------------------------------
# Place order
# --------------------------------------------------------------------
def qt_place_order(symbol, side, risk_stop_pct):
    access_token, api_server = qt_refresh_access_token()
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    last_price = get_last_price(access_token, api_server, symbol)
    shares = max(1, int(POSITION_DOLLARS / last_price))

    order = {
        "symbol": symbol,
        "quantity": shares,
        "isBuy": True if side == "long" else False,
        "orderType": "Market",
        "timeInForce": "Day",
        "primaryRoute": "AUTO",
        "secondaryRoute": "AUTO",
    }

    logger.info(
        "QT: preparing order (DRY_RUN=%s) symbol=%s side=%s qty=%s last_price=%s risk_stop_pct=%.2f",
        DRY_RUN,
        symbol,
        side,
        shares,
        last_price,
        risk_stop_pct,
    )

    if DRY_RUN:
        logger.info("QT: DRY_RUN=True -> NOT sending order to Questrade")
        return {"status": "dry_run", "order": order}

    url = f"{get_base_api_url(api_server)}/v1/accounts/{QUESTRADE_ACCOUNT_NUMBER}/orders"
    logger.info("QT: POST order to %s", url)
    r = requests.post(url, headers=headers, data=json.dumps(order), timeout=10)
    logger.info("QT: order response status=%s body=%s", r.status_code, r.text[:500])
    r.raise_for_status()
    return r.json()

# --------------------------------------------------------------------
# Health check
# --------------------------------------------------------------------
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200

# --------------------------------------------------------------------
# TradingView webhook endpoint
# --------------------------------------------------------------------
@app.route("/tv", methods=["POST"])
def tv():
    try:
        raw = request.get_data(as_text=True)
        logger.info("TV Webhook raw body: %s", raw)

        data = request.get_json(force=True, silent=False)
        logger.info("TV Webhook parsed JSON: %s", data)

        symbol = data.get("symbol")
        event = data.get("event")
        side = data.get("side")
        risk_stop_pct = float(data.get("risk_stop_pct", RISK_PER_TRADE))

        logger.info(
            "TV Webhook parsed -> symbol='%s', event='%s', side='%s', risk_stop_pct=%.2f, DRY_RUN=%s",
            symbol,
            event,
            side,
            risk_stop_pct,
            DRY_RUN,
        )

        if event == "BUY":
            logger.info("INFO: ENTRY detected => placing BUY order.")
            result = qt_place_order(symbol, "long", risk_stop_pct)
        elif event == "SELL":
            logger.info("INFO: EXIT detected => not placing new order (flat logic only).")
            result = {"status": "sell_signal_received"}
        else:
            logger.warning("TV Webhook: unsupported event '%s'", event)
            return jsonify({"error": "unsupported event"}), 400

        return jsonify({"ok": True, "result": result}), 200

    except Exception as e:
        logger.exception("ERROR: app Exception on /tv [POST]")
        return jsonify({"ok": False, "error": str(e)}), 500

# --------------------------------------------------------------------
# Gunicorn entrypoint
# --------------------------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
