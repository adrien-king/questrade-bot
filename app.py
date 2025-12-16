import os
import time
import json
import logging
import requests
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

from flask import Flask, request, jsonify

# Google Sheets (optional but supported)
try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
except Exception:
    service_account = None
    build = None


# ------------------------------------------------------------------------------
# Flask + logging
# ------------------------------------------------------------------------------

app = Flask(__name__)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = app.logger


# ------------------------------------------------------------------------------
# Environment variables
# ------------------------------------------------------------------------------

# Questrade
QUESTRADE_REFRESH_TOKEN = os.getenv("QUESTRADE_REFRESH_TOKEN")
QUESTRADE_ACCOUNT_NUMBER = os.getenv("QUESTRADE_ACCOUNT_NUMBER")
QUESTRADE_PRACTICE = os.getenv("QUESTRADE_PRACTICE", "1")  # "1" practice, "0" live

# Behavior
DRY_RUN = os.getenv("DRY_RUN", "1") == "1"

# Sizing
POSITION_DOLLARS = float(os.getenv("POSITION_DOLLARS", "1000"))  # fixed notional if USE_RISK_SIZING is false
USE_RISK_SIZING = os.getenv("USE_RISK_SIZING", "0") == "1"
RISK_PER_TRADE = float(os.getenv("RISK_PER_TRADE", "50"))        # dollars risked per trade (if USE_RISK_SIZING)
MAX_POSITION_USD = float(os.getenv("MAX_POSITION_USD", "0") or "0")

# Cooldowns
GLOBAL_COOLDOWN_SEC = int(os.getenv("GLOBAL_COOLDOWN_SEC", "5"))
SYMBOL_COOLDOWN_SEC = int(os.getenv("SYMBOL_COOLDOWN_SEC", "20"))

# Google Sheets logging (optional)
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "")
GOOGLE_SHEET_TAB = os.getenv("GOOGLE_SHEET_TAB", "Sheet1")
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")

# Security (optional): simple shared secret for webhook
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")  # if set, TradingView must send {"secret":"..."} in JSON


# ------------------------------------------------------------------------------
# Runtime state: cooldown tracking
# ------------------------------------------------------------------------------

_last_global_ts = 0.0
_symbol_last_ts: Dict[str, float] = {}


# ------------------------------------------------------------------------------
# Utility
# ------------------------------------------------------------------------------

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def clamp_nonneg(x: float) -> float:
    return x if x > 0 else 0.0

def _login_base_url() -> str:
    return "https://login.questrade.com"

def _qt_headers(access_token: str) -> dict:
    return {"Authorization": f"Bearer {access_token}"}

def _should_use_sheets() -> bool:
    return bool(GOOGLE_SHEET_ID and GOOGLE_SERVICE_ACCOUNT_JSON)

def _get_sheets_service():
    if service_account is None or build is None:
        raise RuntimeError("Google libraries not installed. Add google-* packages to requirements.txt")

    # Stored as JSON in Render env var
    info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    creds = service_account.Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return build("sheets", "v4", credentials=creds)

def sheets_append_row(row: list) -> None:
    """
    Appends one row to your Google Sheet (Sheet tab name from GOOGLE_SHEET_TAB).
    Safe: if not configured, it no-ops.
    """
    if not _should_use_sheets():
        return

    service = _get_sheets_service()
    rng = f"{GOOGLE_SHEET_TAB}!A1"
    service.spreadsheets().values().append(
        spreadsheetId=GOOGLE_SHEET_ID,
        range=rng,
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": [row]},
    ).execute()


# ------------------------------------------------------------------------------
# Cooldowns
# ------------------------------------------------------------------------------

def cooldown_check(symbol: str) -> Optional[str]:
    """
    Returns error string if blocked; otherwise records timestamps and returns None.
    """
    global _last_global_ts
    now = time.time()
    s = symbol.upper()

    if now - _last_global_ts < GLOBAL_COOLDOWN_SEC:
        return f"Global cooldown active ({GLOBAL_COOLDOWN_SEC}s)"

    last_s = _symbol_last_ts.get(s, 0.0)
    if now - last_s < SYMBOL_COOLDOWN_SEC:
        return f"Symbol cooldown active for {s} ({SYMBOL_COOLDOWN_SEC}s)"

    _last_global_ts = now
    _symbol_last_ts[s] = now
    return None


# ------------------------------------------------------------------------------
# Questrade API
# ------------------------------------------------------------------------------

def qt_refresh_access_token() -> Tuple[str, str]:
    """
    Refresh token -> (access_token, api_server)
    """
    if not QUESTRADE_REFRESH_TOKEN:
        raise RuntimeError("Missing QUESTRADE_REFRESH_TOKEN")
    url = f"{_login_base_url()}/oauth2/token?grant_type=refresh_token&refresh_token={QUESTRADE_REFRESH_TOKEN}"

    token_prefix = (QUESTRADE_REFRESH_TOKEN or "")[:6]
    log.info("QT refresh: practice=%s token_prefix=%s...", QUESTRADE_PRACTICE, token_prefix)

    r = requests.get(url, timeout=30)
    log.info("QT refresh response: status=%s body=%s", r.status_code, r.text[:300])

    if r.status_code != 200:
        raise RuntimeError(f"Failed to refresh token: status={r.status_code} body={r.text}")

    data = r.json()
    return data["access_token"], data["api_server"]

def qt_get_symbol_id(access_token: str, api_server: str, symbol: str) -> int:
    url = f"{api_server}v1/symbols/search?prefix={symbol}"
    r = requests.get(url, headers=_qt_headers(access_token), timeout=30)
    log.info("QT symbol search: %s status=%s body=%s", symbol, r.status_code, r.text[:300])

    if r.status_code != 200:
        raise RuntimeError(f"Symbol lookup failed for {symbol}: {r.text}")

    payload = r.json()
    symbols = payload.get("symbols", [])
    if not symbols:
        raise RuntimeError(f"No symbols returned for {symbol}")

    exact = [s for s in symbols if s.get("symbol") == symbol]
    chosen = exact[0] if exact else symbols[0]
    return int(chosen["symbolId"])

def qt_get_last_price(access_token: str, api_server: str, symbol: str) -> float:
    url = f"{api_server}v1/markets/quotes/{symbol}"
    r = requests.get(url, headers=_qt_headers(access_token), timeout=30)
    log.info("QT quote: %s status=%s body=%s", symbol, r.status_code, r.text[:300])

    if r.status_code != 200:
        raise RuntimeError(f"Quote failed for {symbol}: {r.text}")

    data = r.json()
    return float(data["quotes"][0]["lastTradePrice"])

def calc_shares(price: float, risk_stop_pct: float) -> int:
    """
    If USE_RISK_SIZING:
        shares = floor(RISK_PER_TRADE / (price - stop_price))
    else:
        shares = floor(POSITION_DOLLARS / price)
    """
    if price <= 0:
        return 0

    if USE_RISK_SIZING:
        # stop distance in dollars per share
        stop_price = price * (1 - (risk_stop_pct / 100.0))
        risk_per_share = price - stop_price
        risk_per_share = clamp_nonneg(risk_per_share)
        if risk_per_share <= 0:
            return 0
        shares = int(RISK_PER_TRADE / risk_per_share)
        shares = max(1, shares)
        return shares

    shares = int(POSITION_DOLLARS / price)
    return max(1, shares)

def qt_place_market_order(symbol: str, leg_side: str, shares: int) -> Dict[str, Any]:
    """
    Places a Market order (Buy/Sell).
    """
    if not QUESTRADE_ACCOUNT_NUMBER:
        raise RuntimeError("Missing QUESTRADE_ACCOUNT_NUMBER")

    access_token, api_server = qt_refresh_access_token()
    symbol_id = qt_get_symbol_id(access_token, api_server, symbol)

    order_body = {
        "accountNumber": QUESTRADE_ACCOUNT_NUMBER,
        "orderType": "Market",
        "timeInForce": "Day",
        "primaryRoute": "AUTO",
        "secondaryRoute": "AUTO",
        "isAllOrNone": False,
        "isAnonymous": False,
        "orderLegs": [{"symbolId": symbol_id, "legSide": leg_side, "quantity": shares}],
    }

    url = f"{api_server}v1/accounts/{QUESTRADE_ACCOUNT_NUMBER}/orders"
    log.info("QT order POST: %s body=%s", url, order_body)

    r = requests.post(url, headers=_qt_headers(access_token), json=order_body, timeout=30)
    log.info("QT order response: status=%s body=%s", r.status_code, r.text[:500])

    if r.status_code >= 300:
        raise RuntimeError(f"Order rejected: status={r.status_code} body={r.text}")

    return r.json()


# ------------------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------------------

@app.route("/health", methods=["GET"])
def health():
    return "OK", 200

@app.route("/tv", methods=["GET", "POST"])
def tv():
    # Basic UX if someone visits in browser
    if request.method != "POST":
        return jsonify({"ok": False, "error": "Use POST with JSON body"}), 405

    raw = request.get_data(as_text=True)
    log.info("TV raw body: %s", raw[:2000])

    try:
        data = request.get_json(force=True) or {}
    except Exception as e:
        log.exception("Bad JSON")
        return jsonify({"ok": False, "error": "Bad JSON", "detail": str(e)}), 400

    # Optional secret check
    if WEBHOOK_SECRET:
        if str(data.get("secret", "")) != WEBHOOK_SECRET:
            return jsonify({"ok": False, "error": "Unauthorized"}), 401

    symbol = str(data.get("symbol", "")).upper().strip()
    event = str(data.get("event", "")).upper().strip()
    side = str(data.get("side", "")).lower().strip()
    risk_stop_pct = float(data.get("risk_stop_pct", 2.0))
    price_from_tv = float(data.get("price", 0) or 0)

    if not symbol or not event:
        return jsonify({"ok": False, "error": "Missing symbol/event", "received": data}), 400

    # Cooldowns
    blocked = cooldown_check(symbol)
    if blocked:
        # Log it to sheet too so you can see over-trading attempts
        row = [
            utc_now_iso(), symbol, event, side,
            price_from_tv or "", "", "", "", RISK_PER_TRADE,
            "COOLDOWN", blocked
        ]
        try:
            sheets_append_row(row)
        except Exception as e:
            log.warning("Sheets write failed (cooldown): %s", e)

        return jsonify({"ok": False, "error": blocked}), 429

    # Map events (supports BUY/SELL and ENTRY/EXIT)
    mapped_action: Optional[str] = None  # "BUY" or "SELL"
    if side == "long" and event in ("BUY", "ENTRY"):
        mapped_action = "BUY"
    elif side == "long" and event in ("SELL", "EXIT"):
        mapped_action = "SELL"
    else:
        msg = f"Unsupported event/side combo event={event} side={side}"
        log.warning(msg)
        row = [utc_now_iso(), symbol, event, side, price_from_tv or "", "", "", "", RISK_PER_TRADE, "REJECTED", msg]
        try:
            sheets_append_row(row)
        except Exception as e:
            log.warning("Sheets write failed (reject): %s", e)
        return jsonify({"ok": False, "error": msg}), 400

    # --------------------------------------------------------------------------
    # DRY RUN (NO Questrade calls)
    # --------------------------------------------------------------------------
    # IMPORTANT: in DRY_RUN we do NOT refresh tokens (avoids your earlier 400s)
    # --------------------------------------------------------------------------
    if DRY_RUN:
        # You can include "price": {{close}} in TradingView alert JSON to get real-ish simulation
        price = price_from_tv
        shares = calc_shares(price, risk_stop_pct) if price > 0 else 0
        position_value = round(shares * price, 2) if shares and price else None
        stop_price = round(price * (1 - risk_stop_pct / 100.0), 4) if price else None

        note = "Include price in TV JSON for sizing/profit sim (e.g., \"price\": {{close}})."
        status = "DRY_RUN"

        # Log to Google Sheets
        row = [
            utc_now_iso(), symbol, event, side,
            price if price else "", shares if shares else "",
            position_value if position_value is not None else "",
            stop_price if stop_price is not None else "",
            RISK_PER_TRADE, status, note
        ]
        try:
            sheets_append_row(row)
        except Exception as e:
            log.warning("Sheets write failed (dry-run): %s", e)

        return jsonify({
            "ok": True,
            "dry_run": True,
            "symbol": symbol,
            "event": event,
            "side": side,
            "mapped_action": mapped_action,
            "use_risk_sizing": USE_RISK_SIZING,
            "risk_per_trade": RISK_PER_TRADE,
            "position_dollars": POSITION_DOLLARS,
            "price": price if price else None,
            "shares": shares if shares else None,
            "position_value": position_value,
            "stop_price": stop_price,
            "note": note
        }), 200

    # --------------------------------------------------------------------------
    # LIVE MODE (Questrade)
    # --------------------------------------------------------------------------
    try:
        if not QUESTRADE_REFRESH_TOKEN or not QUESTRADE_ACCOUNT_NUMBER:
            raise RuntimeError("Missing Questrade env vars (QUESTRADE_REFRESH_TOKEN / QUESTRADE_ACCOUNT_NUMBER).")

        # If TradingView didn't send a price, fetch quote for sizing
        access_token, api_server = qt_refresh_access_token()
        price = price_from_tv if price_from_tv > 0 else qt_get_last_price(access_token, api_server, symbol)

        shares = calc_shares(price, risk_stop_pct)

        # Cap
        if MAX_POSITION_USD > 0:
            est_value = shares * price
            if est_value > MAX_POSITION_USD:
                # reduce shares to fit cap
                shares = max(1, int(MAX_POSITION_USD / price))

        leg_side = "Buy" if mapped_action == "BUY" else "Sell"

        broker_result = qt_place_market_order(symbol, leg_side, shares)

        position_value = round(shares * price, 2)
        stop_price = round(price * (1 - risk_stop_pct / 100.0), 4)

        row = [
            utc_now_iso(), symbol, event, side,
            price, shares, position_value, stop_price,
            RISK_PER_TRADE, "LIVE_OK", json.dumps({"leg": leg_side, "id": broker_result.get("orderId")})
        ]
        try:
            sheets_append_row(row)
        except Exception as e:
            log.warning("Sheets write failed (live-ok): %s", e)

        return jsonify({
            "ok": True,
            "dry_run": False,
            "symbol": symbol,
            "event": event,
            "side": side,
            "mapped_action": mapped_action,
            "price": price,
            "shares": shares,
            "position_value": position_value,
            "stop_price": stop_price,
            "broker_result": broker_result
        }), 200

    except Exception as e:
        log.exception("LIVE ERROR on /tv")
        row = [utc_now_iso(), symbol, event, side, price_from_tv or "", "", "", "", RISK_PER_TRADE, "LIVE_FAIL", str(e)]
        try:
            sheets_append_row(row)
        except Exception as se:
            log.warning("Sheets write failed (live-fail): %s", se)

        return jsonify({"ok": False, "error": "order_failed", "detail": str(e)}), 500


# ------------------------------------------------------------------------------
# Entry point
# ------------------------------------------------------------------------------

if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
