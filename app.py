import os
import time
import json
import logging
import requests
from datetime import datetime, timezone
from flask import Flask, request, jsonify

# Google Sheets (only used if GOOGLE_SHEET_ID + GOOGLE_CREDS_JSON are set)
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

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO))
log = app.logger


# ------------------------------------------------------------------------------
# Environment variables
# ------------------------------------------------------------------------------
QUESTRADE_REFRESH_TOKEN = os.getenv("QUESTRADE_REFRESH_TOKEN")
QUESTRADE_ACCOUNT_NUMBER = os.getenv("QUESTRADE_ACCOUNT_NUMBER")

# sizing
POSITION_DOLLARS = float(os.getenv("POSITION_DOLLARS", "1000"))
RISK_PER_TRADE = float(os.getenv("RISK_PER_TRADE", "50"))
USE_RISK_SIZING = os.getenv("USE_RISK_SIZING", "false").lower() == "true"
MAX_POSITION_USD = float(os.getenv("MAX_POSITION_USD", "0") or "0")

# dry-run
DRY_RUN = os.getenv("DRY_RUN", "1") == "1"

# cooldowns
GLOBAL_COOLDOWN_SEC = int(os.getenv("GLOBAL_COOLDOWN_SEC", "5"))
SYMBOL_COOLDOWN_SEC = int(os.getenv("SYMBOL_COOLDOWN_SEC", "20"))

# google sheets
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")          # spreadsheet ID
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDS_JSON")      # full service account JSON (string)
GOOGLE_SHEET_TAB = os.getenv("GOOGLE_SHEET_TAB", "Sheet1")  # tab name

# ------------------------------------------------------------------------------
# In-memory cooldown state
# ------------------------------------------------------------------------------
LAST_GLOBAL_TS = 0.0
LAST_SYMBOL_TS = {}  # symbol -> timestamp


# ------------------------------------------------------------------------------
# Utilities
# ------------------------------------------------------------------------------
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _truncate(s: str, n: int = 600) -> str:
    if s is None:
        return ""
    s = str(s)
    return s if len(s) <= n else s[:n] + "..."


def _env_summary() -> str:
    return (
        f"DRY_RUN={DRY_RUN} "
        f"USE_RISK_SIZING={USE_RISK_SIZING} "
        f"POSITION_DOLLARS={POSITION_DOLLARS:.2f} "
        f"RISK_PER_TRADE={RISK_PER_TRADE:.2f} "
        f"MAX_POSITION_USD={MAX_POSITION_USD:.2f} "
        f"GLOBAL_COOLDOWN_SEC={GLOBAL_COOLDOWN_SEC} "
        f"SYMBOL_COOLDOWN_SEC={SYMBOL_COOLDOWN_SEC} "
        f"LOG_LEVEL={LOG_LEVEL} "
        f"SHEETS={'on' if (GOOGLE_SHEET_ID and GOOGLE_CREDS_JSON) else 'off'} "
        f"SHEET_TAB={GOOGLE_SHEET_TAB}"
    )


# ------------------------------------------------------------------------------
# Google Sheets logging
# ------------------------------------------------------------------------------
def _sheets_enabled() -> bool:
    return bool(GOOGLE_SHEET_ID and GOOGLE_CREDS_JSON and service_account and build)


def _sheets_client():
    creds_info = json.loads(GOOGLE_CREDS_JSON)
    creds = service_account.Credentials.from_service_account_info(
        creds_info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return build("sheets", "v4", credentials=creds)


def append_sheet_row(row_values: list):
    """
    Appends a row to GOOGLE_SHEET_TAB (default "Sheet1").
    Your sheet headers can be:
    timestamp | symbol | event | side | price | shares | position_value | stop_price | risk_usd | status | note
    """
    if not _sheets_enabled():
        return

    try:
        service = _sheets_client()
        rng = f"{GOOGLE_SHEET_TAB}!A1"
        service.spreadsheets().values().append(
            spreadsheetId=GOOGLE_SHEET_ID,
            range=rng,
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": [row_values]},
        ).execute()
    except Exception as e:
        # Never break trading flow just because sheets failed
        log.exception("Google Sheets append failed: %s", str(e))


# ------------------------------------------------------------------------------
# Questrade helpers (LIVE only)
# ------------------------------------------------------------------------------
def qt_refresh_access_token():
    """
    Uses your refresh token to get {access_token, api_server}.
    """
    if not QUESTRADE_REFRESH_TOKEN:
        raise Exception("Missing QUESTRADE_REFRESH_TOKEN")
    url = (
        "https://login.questrade.com/oauth2/token"
        f"?grant_type=refresh_token&refresh_token={QUESTRADE_REFRESH_TOKEN}"
    )
    r = requests.get(url, timeout=20)
    if r.status_code != 200:
        raise Exception(f"Failed to refresh token: status={r.status_code} body={_truncate(r.text)}")
    data = r.json()
    return data["access_token"], data["api_server"]


def qt_headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def qt_get_symbol_id(token: str, api_server: str, symbol: str) -> int:
    url = f"{api_server}v1/symbols/search?prefix={symbol}"
    r = requests.get(url, headers=qt_headers(token), timeout=20)
    if r.status_code != 200:
        raise Exception(f"Symbol search failed: status={r.status_code} body={_truncate(r.text)}")
    symbols = r.json().get("symbols", [])
    if not symbols:
        raise Exception(f"No symbols found for {symbol}")
    # prefer exact
    exact = [s for s in symbols if s.get("symbol", "").upper() == symbol.upper()]
    chosen = exact[0] if exact else symbols[0]
    return int(chosen["symbolId"])


def qt_get_last_price(token: str, api_server: str, symbol: str) -> float:
    url = f"{api_server}v1/markets/quotes/{symbol}"
    r = requests.get(url, headers=qt_headers(token), timeout=20)
    if r.status_code != 200:
        raise Exception(f"Quote failed: status={r.status_code} body={_truncate(r.text)}")
    q = r.json().get("quotes", [])
    if not q:
        raise Exception(f"No quote returned for {symbol}")
    return float(q[0]["lastTradePrice"])


def qt_place_market_order(symbol: str, action: str, shares: int) -> dict:
    """
    action: BUY or SELL
    """
    if not QUESTRADE_ACCOUNT_NUMBER:
        raise Exception("Missing QUESTRADE_ACCOUNT_NUMBER")

    token, api = qt_refresh_access_token()
    symbol_id = qt_get_symbol_id(token, api, symbol)

    order_body = {
        "accountNumber": QUESTRADE_ACCOUNT_NUMBER,
        "orderType": "Market",
        "timeInForce": "Day",
        "primaryRoute": "AUTO",
        "secondaryRoute": "AUTO",
        "isAllOrNone": False,
        "isAnonymous": False,
        "orderLegs": [
            {
                "symbolId": symbol_id,
                "legSide": "Buy" if action == "BUY" else "Sell",
                "quantity": int(shares),
            }
        ],
    }

    url = f"{api}v1/accounts/{QUESTRADE_ACCOUNT_NUMBER}/orders"
    r = requests.post(url, headers=qt_headers(token), json=order_body, timeout=20)
    if r.status_code >= 300:
        raise Exception(f"Order rejected: status={r.status_code} body={_truncate(r.text)}")
    return r.json()


# ------------------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------------------
@app.route("/", methods=["GET"])
def root():
    return "OK", 200


@app.route("/health", methods=["GET"])
def health():
    return "OK", 200


@app.route("/tv", methods=["GET", "POST"])
def tv():
    """
    TradingView webhook.

    Expected JSON examples:

    BUY/SELL style:
    {"symbol":"AMCI","event":"BUY","side":"long","risk_stop_pct":2.0,"price":13.19}
    {"symbol":"AMCI","event":"SELL","side":"long","risk_stop_pct":2.0,"price":13.27}

    ENTRY/EXIT style:
    {"symbol":"AMCI","event":"ENTRY","side":"long","risk_stop_pct":2.0,"price":13.19}
    {"symbol":"AMCI","event":"EXIT","side":"long","risk_stop_pct":2.0,"price":13.27}
    """
    global LAST_GLOBAL_TS, LAST_SYMBOL_TS

    if request.method != "POST":
        return jsonify({"ok": False, "error": "Use POST with JSON body"}), 405

    raw = request.get_data(as_text=True)
    log.info("TV raw body: %s", _truncate(raw, 1200))

    try:
        data = request.get_json(force=True) or {}
    except Exception as e:
        log.exception("Bad JSON")
        return jsonify({"ok": False, "error": "bad_json", "detail": str(e)}), 400

    # parse
    symbol = str(data.get("symbol", "")).upper().strip()
    event = str(data.get("event", "")).upper().strip()
    side = str(data.get("side", "")).lower().strip()

    # risk stop can be named risk_stop_pct or risk_stop (accept both)
    risk_stop_pct = float(data.get("risk_stop_pct", data.get("risk_stop", 2.0)))

    # price is optional (but needed for good dry-run simulation)
    price = data.get("price", None)
    try:
        price = float(price) if price is not None else None
    except Exception:
        price = None

    # determine action mapping (we are long-only in mapping)
    if side != "long":
        return jsonify({"ok": False, "error": "only_long_supported", "received": data}), 400

    if event in ("BUY", "ENTRY"):
        action = "BUY"
    elif event in ("SELL", "EXIT"):
        action = "SELL"
    else:
        return jsonify({"ok": False, "error": "unsupported_event", "event": event}), 400

    now = time.time()

    # cooldowns
    if now - LAST_GLOBAL_TS < GLOBAL_COOLDOWN_SEC:
        msg = f"Global cooldown active ({GLOBAL_COOLDOWN_SEC}s)"
        log.warning(msg)
        return jsonify({"ok": False, "error": "cooldown_global", "detail": msg}), 429

    if now - LAST_SYMBOL_TS.get(symbol, 0) < SYMBOL_COOLDOWN_SEC:
        msg = f"Symbol cooldown active for {symbol} ({SYMBOL_COOLDOWN_SEC}s)"
        log.warning(msg)
        return jsonify({"ok": False, "error": "cooldown_symbol", "detail": msg}), 429

    # sizing (needs price). In LIVE we can fetch it if missing.
    note = ""
    used_price = price

    if used_price is None:
        if DRY_RUN:
            note = "No price provided in alert JSON; cannot simulate accurate sizing/PnL."
            used_price = 0.0
        else:
            # LIVE: fetch last price from broker
            try:
                token, api = qt_refresh_access_token()
                used_price = qt_get_last_price(token, api, symbol)
                note = "Price fetched from Questrade quote."
            except Exception as e:
                log.exception("Failed to fetch price in LIVE")
                return jsonify({"ok": False, "error": "price_fetch_failed", "detail": str(e)}), 500

    # calculate shares
    shares = 1
    stop_price = None
    position_value = None
    sim_pnl = None
    risk_usd = RISK_PER_TRADE

    if used_price > 0:
        stop_price = used_price * (1 - (risk_stop_pct / 100.0))
        position_value = used_price * 1  # temp

        if USE_RISK_SIZING:
            risk_per_share = used_price - stop_price
            if risk_per_share <= 0:
                shares = 1
                note = (note + " " if note else "") + "Risk-per-share invalid; defaulting shares=1."
            else:
                shares = int(RISK_PER_TRADE / risk_per_share)
                shares = max(1, shares)
        else:
            shares = int(POSITION_DOLLARS / used_price)
            shares = max(1, shares)

        position_value = used_price * shares

        if MAX_POSITION_USD > 0 and position_value > MAX_POSITION_USD:
            return jsonify({
                "ok": False,
                "error": "max_position_exceeded",
                "detail": f"Position value {position_value:.2f} exceeds MAX_POSITION_USD {MAX_POSITION_USD:.2f}"
            }), 400

        # DRY_RUN sim PnL idea:
        # - For BUY: show "risk to stop" as negative and "reward to +1R" as positive.
        # - For SELL/EXIT: we don’t know entry price, so we just log that this is an exit signal.
        if DRY_RUN and action == "BUY":
            risk_to_stop = (stop_price - used_price) * shares  # negative number
            reward_1r = abs(risk_to_stop)  # +1R
            sim_pnl = {
                "risk_to_stop": round(risk_to_stop, 2),
                "reward_1R": round(reward_1r, 2),
            }

    # log summary
    log.info(
        "parsed: symbol=%s event=%s side=%s action=%s price=%s shares=%s pos_val=%s stop=%s DRY_RUN=%s %s",
        symbol, event, side, action,
        used_price, shares,
        round(position_value, 2) if position_value else None,
        round(stop_price, 4) if stop_price else None,
        DRY_RUN,
        f"note={note}" if note else ""
    )

    # write to Google Sheets (even in dry run)
    sheet_row = [
        utc_now_iso(),                      # timestamp
        symbol,                             # symbol
        event,                              # event
        side,                               # side
        used_price if used_price else "",   # price
        shares if shares else "",           # shares
        round(position_value, 2) if position_value else "",  # position_value
        round(stop_price, 4) if stop_price else "",          # stop_price
        round(risk_usd, 2),                 # risk_usd
        "DRY_RUN" if DRY_RUN else "LIVE",   # status
        note or "",                         # note
    ]
    append_sheet_row(sheet_row)

    # set cooldowns
    LAST_GLOBAL_TS = now
    LAST_SYMBOL_TS[symbol] = now

    # DRY RUN response
    if DRY_RUN:
        return jsonify({
            "ok": True,
            "dry_run": True,
            "env": _env_summary(),
            "symbol": symbol,
            "event": event,
            "action": action,
            "price_used": used_price,
            "shares": shares,
            "position_value": position_value,
            "stop_price": stop_price,
            "sim_pnl": sim_pnl,
            "note": note,
            "received": data,
        }), 200

    # LIVE execution
    try:
        result = qt_place_market_order(symbol, action, shares)
        return jsonify({
            "ok": True,
            "dry_run": False,
            "symbol": symbol,
            "event": event,
            "action": action,
            "price_used": used_price,
            "shares": shares,
            "position_value": position_value,
            "stop_price": stop_price,
            "broker_result": result,
            "note": note,
        }), 200
    except Exception as e:
        log.exception("LIVE order failed")
        return jsonify({"ok": False, "error": "order_failed", "detail": str(e)}), 500


# ------------------------------------------------------------------------------
# Startup log
# ------------------------------------------------------------------------------
log.info("Boot: %s", _env_summary())


# ------------------------------------------------------------------------------
# Local run (Render uses gunicorn)
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
