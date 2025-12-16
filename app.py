import os
import json
import time
import logging
import requests
from datetime import datetime
from urllib.parse import quote

from flask import Flask, request, jsonify

# Google Sheets
from google.oauth2 import service_account
from googleapiclient.discovery import build


# ------------------------------------------------------------------------------
# Flask + logging setup
# ------------------------------------------------------------------------------
app = Flask(__name__)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = app.logger


# ------------------------------------------------------------------------------
# Environment variables
# ------------------------------------------------------------------------------
QUESTRADE_REFRESH_TOKEN = os.getenv("QUESTRADE_REFRESH_TOKEN")
QUESTRADE_ACCOUNT_NUMBER = os.getenv("QUESTRADE_ACCOUNT_NUMBER")
QUESTRADE_PRACTICE = os.getenv("QUESTRADE_PRACTICE", "1")  # "1"=practice, "0"=live

DRY_RUN = os.getenv("DRY_RUN", "1") == "1"

# Sizing
POSITION_DOLLARS = float(os.getenv("POSITION_DOLLARS", "1000"))      # fixed-notional sizing
RISK_PER_TRADE = float(os.getenv("RISK_PER_TRADE", "50"))            # $ risk per trade
USE_RISK_SIZING = os.getenv("USE_RISK_SIZING", "0") == "1"           # 1=use risk-based sizing

MAX_POSITION_USD = float(os.getenv("MAX_POSITION_USD", "0") or "0")  # cap notional, optional

# Cooldowns
GLOBAL_COOLDOWN_SEC = int(os.getenv("GLOBAL_COOLDOWN_SEC", "0") or "0")
SYMBOL_COOLDOWN_SEC = int(os.getenv("SYMBOL_COOLDOWN_SEC", "0") or "0")

# Google Sheets logging
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")               # REQUIRED to log
GOOGLE_SHEET_TAB = os.getenv("GOOGLE_SHEET_TAB", "Sheet1")   # your tab name
GOOGLE_SA_FILE = os.getenv("GOOGLE_SA_FILE")                 # optional secret file path
GOOGLE_SA_JSON = os.getenv("GOOGLE_SA_JSON")                 # recommended: JSON string in env var

# ------------------------------------------------------------------------------
# In-memory state (resets when service restarts)
# ------------------------------------------------------------------------------
_last_global_trade_ts = 0.0
_last_symbol_trade_ts = {}  # symbol -> ts

# dry-run positions (for sim PnL)
_positions = {}  # symbol -> {"entry_price": float, "shares": int, "side": "long", "entry_ts": str}


# ------------------------------------------------------------------------------
# Startup log
# ------------------------------------------------------------------------------
log.info(
    "Config loaded: PRACTICE=%s DRY_RUN=%s USE_RISK_SIZING=%s POSITION_DOLLARS=%.2f RISK_PER_TRADE=%.2f "
    "MAX_POSITION_USD=%.2f GLOBAL_COOLDOWN_SEC=%s SYMBOL_COOLDOWN_SEC=%s LOG_LEVEL=%s",
    QUESTRADE_PRACTICE,
    DRY_RUN,
    USE_RISK_SIZING,
    POSITION_DOLLARS,
    RISK_PER_TRADE,
    MAX_POSITION_USD,
    GLOBAL_COOLDOWN_SEC,
    SYMBOL_COOLDOWN_SEC,
    os.getenv("LOG_LEVEL", "INFO"),
)

# Minimal validation (only require Questrade creds when NOT dry-run)
if not DRY_RUN:
    if not QUESTRADE_REFRESH_TOKEN:
        raise ValueError("Missing Questrade refresh token (QUESTRADE_REFRESH_TOKEN).")
    if not QUESTRADE_ACCOUNT_NUMBER:
        raise ValueError("Missing Questrade account number (QUESTRADE_ACCOUNT_NUMBER).")


# ------------------------------------------------------------------------------
# Helpers: time / logging / cooldowns
# ------------------------------------------------------------------------------
def now_iso():
    return datetime.utcnow().isoformat()


def cooldown_blocked(symbol: str):
    global _last_global_trade_ts, _last_symbol_trade_ts

    now = time.time()

    if GLOBAL_COOLDOWN_SEC > 0 and (now - _last_global_trade_ts) < GLOBAL_COOLDOWN_SEC:
        remaining = int(GLOBAL_COOLDOWN_SEC - (now - _last_global_trade_ts))
        return True, f"Global cooldown active ({remaining}s remaining)"

    if SYMBOL_COOLDOWN_SEC > 0:
        last = _last_symbol_trade_ts.get(symbol, 0.0)
        if (now - last) < SYMBOL_COOLDOWN_SEC:
            remaining = int(SYMBOL_COOLDOWN_SEC - (now - last))
            return True, f"Symbol cooldown active for {symbol} ({remaining}s remaining)"

    return False, ""


def mark_trade_ts(symbol: str):
    global _last_global_trade_ts, _last_symbol_trade_ts
    now = time.time()
    _last_global_trade_ts = now
    _last_symbol_trade_ts[symbol] = now


# ------------------------------------------------------------------------------
# Google Sheets: client + append row
# ------------------------------------------------------------------------------
_sheets_service = None


def get_sheets_service():
    global _sheets_service
    if _sheets_service:
        return _sheets_service

    if not GOOGLE_SHEET_ID:
        raise RuntimeError("GOOGLE_SHEET_ID not set (needed to log to Sheets).")

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]

    if GOOGLE_SA_FILE:
        creds = service_account.Credentials.from_service_account_file(GOOGLE_SA_FILE, scopes=scopes)
    elif GOOGLE_SA_JSON:
        creds = service_account.Credentials.from_service_account_info(json.loads(GOOGLE_SA_JSON), scopes=scopes)
    else:
        raise RuntimeError("Missing Google Sheets credentials (set GOOGLE_SA_JSON or GOOGLE_SA_FILE).")

    _sheets_service = build("sheets", "v4", credentials=creds)
    return _sheets_service


def sheets_append_row(row, sheet_name="Sheet1"):
    service = get_sheets_service()
    body = {"values": [row]}
    service.spreadsheets().values().append(
        spreadsheetId=GOOGLE_SHEET_ID,
        range=f"{sheet_name}!A1",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body=body,
    ).execute()


def log_to_sheet(
    symbol,
    event,
    side,
    price,
    shares,
    position_value,
    stop_price,
    risk_usd,
    status,
    note="",
):
    # Matches your existing headers:
    # timestamp | symbol | event | side | price | shares | position_value | stop_price | risk_usd | status | note
    row = [
        now_iso(),
        symbol,
        event,
        side,
        price,
        shares,
        position_value,
        stop_price,
        risk_usd,
        status,
        note,
    ]

    try:
        if GOOGLE_SHEET_ID and (GOOGLE_SA_JSON or GOOGLE_SA_FILE):
            sheets_append_row(row, sheet_name=GOOGLE_SHEET_TAB)
            log.info("Logged to Google Sheets: %s %s %s", symbol, event, status)
        else:
            log.info("Sheets not configured; skipped sheet log. Row=%s", row)
    except Exception:
        log.exception("Google Sheets logging failed (non-fatal)")


# ------------------------------------------------------------------------------
# Questrade helpers
# ------------------------------------------------------------------------------
def _login_base_url() -> str:
    return "https://login.questrade.com"


def qt_refresh_access_token():
    """
    Refresh token -> access_token + api_server
    NOTE: Refresh token must be a REAL Questrade refresh token (not TradingView).
    """
    if not QUESTRADE_REFRESH_TOKEN:
        raise RuntimeError("QUESTRADE_REFRESH_TOKEN missing")

    token_prefix = (QUESTRADE_REFRESH_TOKEN or "")[:5]
    refresh_token_encoded = quote(QUESTRADE_REFRESH_TOKEN, safe="")  # important

    url = f"{_login_base_url()}/oauth2/token?grant_type=refresh_token&refresh_token={refresh_token_encoded}"

    log.info("QT refresh: practice=%s token_prefix=%s...", QUESTRADE_PRACTICE, token_prefix)
    r = requests.get(url, timeout=30)

    log.info("QT refresh: status=%s body=%s", r.status_code, r.text[:500])
    if r.status_code != 200:
        raise Exception(f"Failed to refresh token: status={r.status_code} body={r.text}")

    data = r.json()
    access_token = data["access_token"]
    api_server = data["api_server"]  # e.g. https://api01.iq.questrade.com/
    return access_token, api_server


def qt_headers(access_token: str) -> dict:
    return {"Authorization": f"Bearer {access_token}"}


def qt_get_symbol_id(access_token: str, api_server: str, symbol: str) -> int:
    url = f"{api_server}v1/symbols/search?prefix={quote(symbol)}"
    r = requests.get(url, headers=qt_headers(access_token), timeout=30)
    log.info("QT symbol search: %s status=%s body=%s", symbol, r.status_code, r.text[:300])

    if r.status_code != 200:
        raise Exception(f"Failed symbol lookup for {symbol}: {r.text}")

    data = r.json()
    symbols = data.get("symbols", [])
    if not symbols:
        raise Exception(f"No symbols found for {symbol}")

    exact = [s for s in symbols if s.get("symbol") == symbol]
    chosen = exact[0] if exact else symbols[0]
    return int(chosen["symbolId"])


def qt_get_last_price(access_token: str, api_server: str, symbol: str) -> float:
    url = f"{api_server}v1/markets/quotes/{quote(symbol)}"
    r = requests.get(url, headers=qt_headers(access_token), timeout=30)
    log.info("QT quote: %s status=%s body=%s", symbol, r.status_code, r.text[:300])

    if r.status_code != 200:
        raise Exception(f"Failed quote for {symbol}: {r.text}")

    data = r.json()
    return float(data["quotes"][0]["lastTradePrice"])


def qt_place_market_order(symbol: str, leg_side: str, quantity: int):
    """
    leg_side: "Buy" or "Sell"
    """
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
        "orderLegs": [{"symbolId": symbol_id, "legSide": leg_side, "quantity": quantity}],
    }

    url = f"{api_server}v1/accounts/{QUESTRADE_ACCOUNT_NUMBER}/orders"
    log.info("QT order POST %s body=%s", url, order_body)

    r = requests.post(url, headers=qt_headers(access_token), json=order_body, timeout=30)
    log.info("QT order response: status=%s body=%s", r.status_code, r.text[:800])

    if r.status_code >= 300:
        raise Exception(f"Order rejected: {r.status_code} {r.text}")

    return r.json()


# ------------------------------------------------------------------------------
# Sizing / stop / risk helpers
# ------------------------------------------------------------------------------
def compute_stop_price(price: float, risk_stop_pct: float) -> float:
    # long stop below entry
    return round(price * (1.0 - (risk_stop_pct / 100.0)), 4)


def compute_shares(price: float, stop_price: float) -> int:
    """
    If USE_RISK_SIZING=True:
      shares = RISK_PER_TRADE / (price - stop_price)
    else:
      shares = POSITION_DOLLARS / price

    Applies MAX_POSITION_USD cap if set.
    """
    if price <= 0:
        return 0

    if USE_RISK_SIZING:
        per_share_risk = max(0.0001, price - stop_price)
        shares = int(RISK_PER_TRADE / per_share_risk)
        shares = max(1, shares)
    else:
        shares = int(POSITION_DOLLARS / price)
        shares = max(1, shares)

    # Cap by max notional
    if MAX_POSITION_USD and MAX_POSITION_USD > 0:
        max_shares = int(MAX_POSITION_USD / price)
        if max_shares >= 1:
            shares = min(shares, max_shares)

    return shares


def position_value(price: float, shares: int) -> float:
    return round(price * shares, 2)


def risk_usd_est(price: float, stop_price: float, shares: int) -> float:
    # for long: risk is (entry - stop) * shares
    return round(max(0.0, (price - stop_price)) * shares, 2)


# ------------------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------------------
@app.route("/", methods=["GET"])
def home():
    return jsonify(
        {
            "ok": True,
            "service": "questrade-bot",
            "endpoints": ["/health", "/tv"],
            "note": "POST JSON to /tv from TradingView webhook.",
        }
    ), 200


@app.route("/health", methods=["GET"])
def health():
    return "OK", 200


@app.route("/tv", methods=["GET", "POST"])
def tv():
    # Always log raw body for debugging
    raw_body = request.get_data(as_text=True)
    log.info("TV raw body: %s", raw_body)

    if request.method != "POST":
        return jsonify({"ok": False, "error": "Use POST with JSON body"}), 405

    # Parse JSON
    try:
        data = request.get_json(force=True) or {}
    except Exception as e:
        log.exception("TV JSON parse error")
        return jsonify({"ok": False, "error": "Bad JSON", "detail": str(e)}), 400

    symbol = str(data.get("symbol", "")).upper().strip()
    event = str(data.get("event", "")).upper().strip()
    side = str(data.get("side", "")).lower().strip()
    risk_stop_pct = float(data.get("risk_stop_pct", data.get("risk_stop", 2.0) or 2.0))

    # Optional (highly recommended): include price in alert JSON for dry-run PnL
    price = data.get("price", None)
    try:
        price = float(price) if price is not None else None
    except Exception:
        price = None

    if not symbol:
        return jsonify({"ok": False, "error": "Missing symbol", "received": data}), 400

    log.info(
        "TV parsed: symbol=%s event=%s side=%s risk_stop_pct=%s price=%s DRY_RUN=%s",
        symbol,
        event,
        side,
        risk_stop_pct,
        price,
        DRY_RUN,
    )

    # Map event+side -> action
    # Accept BUY/SELL and ENTRY/EXIT
    if side != "long":
        return jsonify({"ok": False, "error": "Only long supported right now", "side": side}), 400

    if event in ("BUY", "ENTRY"):
        action = "BUY"
    elif event in ("SELL", "EXIT"):
        action = "SELL"
    else:
        return jsonify({"ok": False, "error": "Unsupported event", "event": event}), 400

    # Cooldowns
    blocked, reason = cooldown_blocked(symbol)
    if blocked:
        log.warning("Cooldown blocked: %s", reason)
        return jsonify({"ok": False, "error": "cooldown", "detail": reason}), 429

    # If no price provided, we can still accept DRY_RUN but won't compute sizing/PnL well
    if price is None:
        note = "No price provided. For best dry-run sizing + PnL simulation, include 'price' in your alert JSON."
        log.warning(note)

    # Compute stats (best-effort when price is missing)
    if price is not None:
        stop_price = compute_stop_price(price, risk_stop_pct)
        shares = compute_shares(price, stop_price)
        pos_val = position_value(price, shares)
        risk_usd = risk_usd_est(price, stop_price, shares)
    else:
        stop_price = None
        shares = None
        pos_val = None
        risk_usd = None

    # ------------------------------------------------------------------
    # DRY RUN: do NOT call Questrade at all
    # ------------------------------------------------------------------
    if DRY_RUN:
        sim_pnl = None
        sim_note = ""

        # Track entry/exit to estimate PnL
        if action == "BUY":
            if price is not None and shares is not None:
                _positions[symbol] = {
                    "entry_price": price,
                    "shares": shares,
                    "side": "long",
                    "entry_ts": now_iso(),
                }
                sim_note = "Simulated entry stored."
            else:
                sim_note = "Simulated BUY received, but missing price so no entry stored."

        if action == "SELL":
            pos = _positions.get(symbol)
            if pos and price is not None:
                entry = float(pos["entry_price"])
                qty = int(pos["shares"])
                sim_pnl = round((price - entry) * qty, 2)
                sim_note = f"Simulated exit. Entry={entry} Shares={qty} PnL={sim_pnl}"
                # close position
                _positions.pop(symbol, None)
            else:
                sim_note = "Simulated SELL received, but no stored entry and/or missing price."

        # Mark cooldown timestamps on DRY_RUN too (prevents spam)
        mark_trade_ts(symbol)

        # Log to Google Sheets
        log_to_sheet(
            symbol=symbol,
            event=event,
            side=side,
            price=price if price is not None else "",
            shares=shares if shares is not None else "",
            position_value=pos_val if pos_val is not None else "",
            stop_price=stop_price if stop_price is not None else "",
            risk_usd=risk_usd if risk_usd is not None else "",
            status="DRY_RUN",
            note=(sim_note if sim_note else ""),
        )

        return jsonify(
            {
                "ok": True,
                "dry_run": True,
                "symbol": symbol,
                "event": event,
                "side": side,
                "mapped_action": action,
                "risk_stop_pct": risk_stop_pct,
                "price": price,
                "shares": shares,
                "position_value": pos_val,
                "stop_price": stop_price,
                "risk_usd": risk_usd,
                "sim_pnl": sim_pnl,
                "note": "Include 'price' in alert JSON for sizing + PnL simulation.",
                "received": data,
            }
        ), 200

    # ------------------------------------------------------------------
    # LIVE: execute Questrade order
    # ------------------------------------------------------------------
    try:
        if price is None:
            # In live mode we can fetch quote ourselves for sizing
            access_token, api_server = qt_refresh_access_token()
            price = qt_get_last_price(access_token, api_server, symbol)
            stop_price = compute_stop_price(price, risk_stop_pct)
            shares = compute_shares(price, stop_price)
            pos_val = position_value(price, shares)
            risk_usd = risk_usd_est(price, stop_price, shares)

        leg_side = "Buy" if action == "BUY" else "Sell"
        mark_trade_ts(symbol)

        broker_result = qt_place_market_order(symbol=symbol, leg_side=leg_side, quantity=int(shares))

        log_to_sheet(
            symbol=symbol,
            event=event,
            side=side,
            price=price,
            shares=shares,
            position_value=pos_val,
            stop_price=stop_price,
            risk_usd=risk_usd,
            status="LIVE",
            note=f"Order sent: {leg_side}",
        )

        return jsonify(
            {
                "ok": True,
                "dry_run": False,
                "symbol": symbol,
                "event": event,
                "side": side,
                "mapped_action": action,
                "price_used": price,
                "shares": shares,
                "position_value": pos_val,
                "stop_price": stop_price,
                "risk_usd": risk_usd,
                "broker_result": broker_result,
            }
        ), 200

    except Exception as e:
        log.exception("LIVE order failed")
        log_to_sheet(
            symbol=symbol,
            event=event,
            side=side,
            price=price if price is not None else "",
            shares=shares if shares is not None else "",
            position_value=pos_val if pos_val is not None else "",
            stop_price=stop_price if stop_price is not None else "",
            risk_usd=risk_usd if risk_usd is not None else "",
            status="ERROR",
            note=str(e)[:300],
        )
        return jsonify({"ok": False, "error": "order_failed", "detail": str(e)}), 500


# ------------------------------------------------------------------------------
# Entry point for local dev (Render uses gunicorn: app:app)
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    app.run(host="0.0.0.0", port=port)
