import os
import time
import json
import math
import logging
import requests
from datetime import datetime, timezone

from flask import Flask, request, jsonify

# Google Sheets (optional)
from google.oauth2 import service_account
from googleapiclient.discovery import build


# ------------------------------------------------------------------------------
# Flask + logging
# ------------------------------------------------------------------------------

app = Flask(__name__)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = app.logger


# ------------------------------------------------------------------------------
# ENV
# ------------------------------------------------------------------------------

QUESTRADE_REFRESH_TOKEN = os.getenv("QUESTRADE_REFRESH_TOKEN")
QUESTRADE_ACCOUNT_NUMBER = os.getenv("QUESTRADE_ACCOUNT_NUMBER")
PRACTICE = os.getenv("PRACTICE", "1") == "1"

DRY_RUN = os.getenv("DRY_RUN", "1") == "1"

# Sizing / risk
POSITION_DOLLARS = float(os.getenv("POSITION_DOLLARS", "1000"))
RISK_PER_TRADE = float(os.getenv("RISK_PER_TRADE", "50"))  # $ risk per trade (used if USE_RISK_SIZING)
USE_RISK_SIZING = os.getenv("USE_RISK_SIZING", "1") == "1"
MAX_POSITION_USD = float(os.getenv("MAX_POSITION_USD", "0") or "0")

GLOBAL_COOLDOWN_SEC = int(os.getenv("GLOBAL_COOLDOWN_SEC", "5"))
SYMBOL_COOLDOWN_SEC = int(os.getenv("SYMBOL_COOLDOWN_SEC", "20"))

# Google Sheets
SHEETS_ON = os.getenv("SHEETS", "off").lower() == "on"
GOOGLE_CREDS_PATH = os.getenv("GOOGLE_CREDS_PATH", "/etc/secrets/google_creds.json")

# You can set either GOOGLE_SHEET_ID or GSHEET_ID; code prefers GOOGLE_SHEET_ID.
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID") or os.getenv("GSHEET_ID")
GOOGLE_SHEET_TAB = os.getenv("GOOGLE_SHEET_TAB", "Sheet1")  # your existing log tab
GOOGLE_POS_TAB = os.getenv("GOOGLE_POS_TAB", "Positions")
GOOGLE_PNL_TAB = os.getenv("GOOGLE_PNL_TAB", "PnL")
GOOGLE_DAILY_TAB = os.getenv("GOOGLE_DAILY_TAB", "Daily")

if not QUESTRADE_REFRESH_TOKEN:
    log.warning("Missing QUESTRADE_REFRESH_TOKEN. (Live trading will fail.)")
if not QUESTRADE_ACCOUNT_NUMBER:
    log.warning("Missing QUESTRADE_ACCOUNT_NUMBER. (Live trading will fail.)")

log.info(
    "Config loaded: PRACTICE=%s DRY_RUN=%s USE_RISK_SIZING=%s POSITION_DOLLARS=%.2f RISK_PER_TRADE=%.2f MAX_POSITION_USD=%.2f GLOBAL_COOLDOWN_SEC=%s SYMBOL_COOLDOWN_SEC=%s SHEETS=%s SHEET_ID=%s SHEET_TAB=%s GOOGLE_CREDS_PATH=%s",
    int(PRACTICE),
    int(DRY_RUN),
    int(USE_RISK_SIZING),
    POSITION_DOLLARS,
    RISK_PER_TRADE,
    MAX_POSITION_USD,
    GLOBAL_COOLDOWN_SEC,
    SYMBOL_COOLDOWN_SEC,
    "on" if SHEETS_ON else "off",
    (GOOGLE_SHEET_ID or "")[:8] + "..." if GOOGLE_SHEET_ID else None,
    GOOGLE_SHEET_TAB,
    GOOGLE_CREDS_PATH,
)


# ------------------------------------------------------------------------------
# In-memory cooldowns (best-effort). State is persisted via Sheets "Positions".
# ------------------------------------------------------------------------------

_last_global_ts = 0.0
_last_symbol_ts = {}  # symbol -> ts


def now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _cooldown_block(symbol: str):
    global _last_global_ts
    t = time.time()

    if t - _last_global_ts < GLOBAL_COOLDOWN_SEC:
        return True, f"Global cooldown active ({GLOBAL_COOLDOWN_SEC}s)."

    last_sym = _last_symbol_ts.get(symbol, 0.0)
    if t - last_sym < SYMBOL_COOLDOWN_SEC:
        return True, f"Symbol cooldown active for {symbol} ({SYMBOL_COOLDOWN_SEC}s)."

    _last_global_ts = t
    _last_symbol_ts[symbol] = t
    return False, ""


# ------------------------------------------------------------------------------
# Questrade helpers
# ------------------------------------------------------------------------------

def _login_base_url() -> str:
    return "https://login.questrade.com"


def qt_refresh_access_token():
    url = f"{_login_base_url()}/oauth2/token?grant_type=refresh_token&refresh_token={QUESTRADE_REFRESH_TOKEN}"
    r = requests.get(url)
    if r.status_code != 200:
        raise Exception(f"Failed to refresh token: status={r.status_code} body={r.text}")
    data = r.json()
    return data["access_token"], data["api_server"]


def qt_headers(access_token: str) -> dict:
    return {"Authorization": f"Bearer {access_token}"}


def qt_get_symbol_id(access_token: str, api_server: str, symbol: str) -> int:
    url = f"{api_server}v1/symbols/search?prefix={symbol}"
    r = requests.get(url, headers=qt_headers(access_token))
    if r.status_code != 200:
        raise Exception(f"Failed to lookup symbolId for {symbol}: {r.text}")
    data = r.json()
    symbols = data.get("symbols", [])
    if not symbols:
        raise Exception(f"No symbols found for {symbol}")
    exact = [s for s in symbols if s.get("symbol") == symbol]
    chosen = exact[0] if exact else symbols[0]
    return int(chosen["symbolId"])


def get_last_price(access_token: str, api_server: str, symbol: str) -> float:
    # Questrade quotes endpoint expects symbolId(s) typically. Some accounts accept symbol string.
    # We'll keep your original approach but include fallback to symbolId quotes if needed.
    url = f"{api_server}v1/markets/quotes/{symbol}"
    r = requests.get(url, headers=qt_headers(access_token))
    if r.status_code == 200:
        data = r.json()
        return float(data["quotes"][0]["lastTradePrice"])

    # fallback: resolve symbolId -> quotes/{id}
    sid = qt_get_symbol_id(access_token, api_server, symbol)
    url2 = f"{api_server}v1/markets/quotes/{sid}"
    r2 = requests.get(url2, headers=qt_headers(access_token))
    if r2.status_code != 200:
        raise Exception(f"Failed to get last price for {symbol}: {r2.text}")
    data2 = r2.json()
    return float(data2["quotes"][0]["lastTradePrice"])


def _calc_shares(entry_price: float, stop_pct: float):
    """
    Returns (shares, position_value, stop_price, risk_usd_used, note)
    """
    if entry_price <= 0:
        return 0, 0.0, None, 0.0, "Bad price"

    stop_price = None
    risk_used = 0.0
    note = ""

    if USE_RISK_SIZING:
        # risk per share = entry_price * stop_pct
        stop_distance = entry_price * (stop_pct / 100.0)
        if stop_distance <= 0:
            # fallback to fixed notional
            shares = max(1, int(POSITION_DOLLARS / entry_price))
            pos_val = shares * entry_price
            note = "Risk sizing enabled but stop distance invalid; used fixed notional."
            return shares, pos_val, None, 0.0, note

        shares = max(1, int(RISK_PER_TRADE / stop_distance))
        pos_val = shares * entry_price
        risk_used = shares * stop_distance
        stop_price = round(entry_price - stop_distance, 4)
        note = "Risk-sizing enabled (RISK_PER_TRADE / stop distance)."
    else:
        shares = max(1, int(POSITION_DOLLARS / entry_price))
        pos_val = shares * entry_price
        note = "Fixed notional sizing."

    # Clamp by max position
    if MAX_POSITION_USD and MAX_POSITION_USD > 0 and pos_val > MAX_POSITION_USD:
        clamped_shares = max(1, int(MAX_POSITION_USD / entry_price))
        clamped_val = clamped_shares * entry_price
        note += f" Clamped by MAX_POSITION_USD."
        # recompute risk_used for clamped
        if USE_RISK_SIZING and stop_pct > 0:
            stop_distance = entry_price * (stop_pct / 100.0)
            risk_used = clamped_shares * stop_distance
            stop_price = round(entry_price - stop_distance, 4)
        return clamped_shares, clamped_val, stop_price, risk_used, note

    return shares, pos_val, stop_price, risk_used, note


def qt_place_order(symbol: str, leg_side: str, quantity: int):
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
    r = requests.post(url, headers=qt_headers(access_token), json=order_body)
    if r.status_code >= 300:
        raise Exception(f"Order rejected: {r.status_code} {r.text}")
    return r.json()


# ------------------------------------------------------------------------------
# Google Sheets helpers
# ------------------------------------------------------------------------------

_scopes = ["https://www.googleapis.com/auth/spreadsheets"]
_sheets_service = None


def sheets_service():
    global _sheets_service
    if _sheets_service is not None:
        return _sheets_service

    if not SHEETS_ON:
        return None
    if not GOOGLE_SHEET_ID:
        raise Exception("SHEETS=on but GOOGLE_SHEET_ID/GSHEET_ID is missing.")
    if not GOOGLE_CREDS_PATH or not os.path.exists(GOOGLE_CREDS_PATH):
        raise Exception(f"SHEETS=on but creds file not found at GOOGLE_CREDS_PATH={GOOGLE_CREDS_PATH}")

    creds = service_account.Credentials.from_service_account_file(GOOGLE_CREDS_PATH, scopes=_scopes)
    _sheets_service = build("sheets", "v4", credentials=creds, cache_discovery=False)
    return _sheets_service


def _get_sheet_metadata():
    svc = sheets_service()
    if svc is None:
        return None
    return svc.spreadsheets().get(spreadsheetId=GOOGLE_SHEET_ID).execute()


def ensure_tab_exists(tab_name: str):
    svc = sheets_service()
    if svc is None:
        return
    meta = _get_sheet_metadata()
    existing = {s["properties"]["title"] for s in meta.get("sheets", [])}
    if tab_name in existing:
        return

    req = {"requests": [{"addSheet": {"properties": {"title": tab_name}}}]}
    svc.spreadsheets().batchUpdate(spreadsheetId=GOOGLE_SHEET_ID, body=req).execute()
    log.info("Sheets: created tab '%s'", tab_name)


def get_values(tab: str, a1: str):
    svc = sheets_service()
    if svc is None:
        return []
    rng = f"{tab}!{a1}"
    res = svc.spreadsheets().values().get(spreadsheetId=GOOGLE_SHEET_ID, range=rng).execute()
    return res.get("values", [])


def append_row(tab: str, row: list):
    svc = sheets_service()
    if svc is None:
        return
    body = {"values": [row]}
    svc.spreadsheets().values().append(
        spreadsheetId=GOOGLE_SHEET_ID,
        range=f"{tab}!A1",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body=body,
    ).execute()


def set_values(tab: str, a1: str, values: list):
    svc = sheets_service()
    if svc is None:
        return
    body = {"values": values}
    svc.spreadsheets().values().update(
        spreadsheetId=GOOGLE_SHEET_ID,
        range=f"{tab}!{a1}",
        valueInputOption="USER_ENTERED",
        body=body,
    ).execute()


def ensure_headers(tab: str, headers: list):
    ensure_tab_exists(tab)
    existing = get_values(tab, "A1:Z1")
    if not existing or not existing[0]:
        set_values(tab, "A1", [headers])
        log.info("Sheets: wrote headers to '%s'", tab)


def ensure_formula_tabs():
    """
    Creates PnL + Daily tabs and drops formulas that auto-aggregate from the log tab.
    Works with your existing log headers.
    """
    ensure_tab_exists(GOOGLE_PNL_TAB)
    ensure_tab_exists(GOOGLE_DAILY_TAB)

    # PnL tab: per-trade realized PnL rows pulled from log tab (EXIT rows)
    ensure_headers(GOOGLE_PNL_TAB, ["date", "timestamp", "symbol", "pnl_usd", "entry_price", "exit_price", "shares"])
    # Write formulas starting at row 2
    # Assumes log tab columns match:
    # A timestamp, B symbol, C event, D side, E price, F shares, G position_value, H stop_price, I risk_usd, J status, K note, L pnl_usd (we will include this col)
    # We will keep pnl_usd in column L of log.
    set_values(GOOGLE_PNL_TAB, "A2", [[
        f'=ARRAYFORMULA(IFERROR(FILTER({GOOGLE_SHEET_TAB}!A2:A, {GOOGLE_SHEET_TAB}!C2:C="EXIT"), ))'
    ]])
    set_values(GOOGLE_PNL_TAB, "B2", [[
        f'=ARRAYFORMULA(IFERROR(FILTER({GOOGLE_SHEET_TAB}!A2:A, {GOOGLE_SHEET_TAB}!C2:C="EXIT"), ))'
    ]])
    set_values(GOOGLE_PNL_TAB, "C2", [[
        f'=ARRAYFORMULA(IFERROR(FILTER({GOOGLE_SHEET_TAB}!B2:B, {GOOGLE_SHEET_TAB}!C2:C="EXIT"), ))'
    ]])
    set_values(GOOGLE_PNL_TAB, "D2", [[
        f'=ARRAYFORMULA(IFERROR(FILTER({GOOGLE_SHEET_TAB}!L2:L, {GOOGLE_SHEET_TAB}!C2:C="EXIT"), ))'
    ]])
    # entry_price, exit_price, shares from note parsing is messy; we’ll store them cleanly in EXIT note below.
    # For now, leave these blank or you can extend later.

    # Daily tab: daily totals from PnL tab (col A date, col D pnl)
    ensure_headers(GOOGLE_DAILY_TAB, ["date", "trades", "pnl_usd", "avg_pnl", "win_rate"])
    # Daily aggregation using QUERY
    set_values(GOOGLE_DAILY_TAB, "A2", [[
        f'=QUERY({GOOGLE_PNL_TAB}!A:D, "select A, count(A), sum(D), avg(D), sum(case when D>0 then 1 else 0 end)/count(A) where A is not null group by A order by A desc", 0)'
    ]])

    log.info("Sheets: ensured PnL + Daily tabs + formulas.")


# ------------------------------------------------------------------------------
# Positions tab (persists state so you don't double-enter/exit)
# ------------------------------------------------------------------------------
POS_HEADERS = ["symbol", "state", "entry_ts", "entry_price", "shares", "stop_pct", "stop_price"]


def pos_get(symbol: str):
    """
    Returns dict or None.
    Positions tab format:
    A symbol | B state | C entry_ts | D entry_price | E shares | F stop_pct | G stop_price
    """
    ensure_headers(GOOGLE_POS_TAB, POS_HEADERS)

    rows = get_values(GOOGLE_POS_TAB, "A2:G")
    for r in rows:
        if len(r) >= 2 and str(r[0]).upper() == symbol.upper():
            # fill safe
            return {
                "symbol": str(r[0]).upper(),
                "state": str(r[1]),
                "entry_ts": r[2] if len(r) > 2 else "",
                "entry_price": float(r[3]) if len(r) > 3 and r[3] != "" else None,
                "shares": int(float(r[4])) if len(r) > 4 and r[4] != "" else None,
                "stop_pct": float(r[5]) if len(r) > 5 and r[5] != "" else None,
                "stop_price": float(r[6]) if len(r) > 6 and r[6] != "" else None,
            }
    return None


def pos_upsert(p: dict):
    ensure_headers(GOOGLE_POS_TAB, POS_HEADERS)
    rows = get_values(GOOGLE_POS_TAB, "A2:G")
    target_row = None
    for idx, r in enumerate(rows, start=2):
        if len(r) >= 1 and str(r[0]).upper() == p["symbol"].upper():
            target_row = idx
            break

    values = [[
        p.get("symbol", ""),
        p.get("state", ""),
        p.get("entry_ts", ""),
        p.get("entry_price", ""),
        p.get("shares", ""),
        p.get("stop_pct", ""),
        p.get("stop_price", ""),
    ]]

    if target_row is None:
        append_row(GOOGLE_POS_TAB, values[0])
    else:
        set_values(GOOGLE_POS_TAB, f"A{target_row}", values)


def pos_clear(symbol: str):
    # simplest: mark as FLAT and clear fields
    pos_upsert({
        "symbol": symbol.upper(),
        "state": "FLAT",
        "entry_ts": "",
        "entry_price": "",
        "shares": "",
        "stop_pct": "",
        "stop_price": "",
    })


# ------------------------------------------------------------------------------
# Log tab schema (yours + pnl col)
# ------------------------------------------------------------------------------
LOG_HEADERS = [
    "timestamp", "symbol", "event", "side", "price", "shares", "position_value",
    "stop_price", "risk_usd", "status", "note", "pnl_usd"
]


def log_trade_row(row: dict):
    """
    Writes a row to your existing log tab.
    """
    if not SHEETS_ON:
        return
    ensure_headers(GOOGLE_SHEET_TAB, LOG_HEADERS)

    append_row(GOOGLE_SHEET_TAB, [
        row.get("timestamp", ""),
        row.get("symbol", ""),
        row.get("event", ""),
        row.get("side", ""),
        row.get("price", ""),
        row.get("shares", ""),
        row.get("position_value", ""),
        row.get("stop_price", ""),
        row.get("risk_usd", ""),
        row.get("status", ""),
        row.get("note", ""),
        row.get("pnl_usd", ""),
    ])


# ------------------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------------------

@app.route("/", methods=["GET"])
def root():
    return "OK", 200


@app.route("/health", methods=["GET"])
def health():
    return "OK", 200


@app.route("/tv", methods=["POST", "GET"])
def tv():
    if request.method != "POST":
        return jsonify({"ok": False, "error": "Use POST with JSON body"}), 405

    raw = request.get_data(as_text=True)
    log.info("TV raw body: %s", raw[:1000])

    try:
        data = request.get_json(force=True) or {}
    except Exception as e:
        log.exception("JSON parse error")
        return jsonify({"ok": False, "error": "Bad JSON", "detail": str(e)}), 400

    symbol = str(data.get("symbol", "")).upper().strip()
    event = str(data.get("event", "")).upper().strip()     # BUY/SELL or ENTRY/EXIT
    side = str(data.get("side", "long")).lower().strip()   # long
    stop_pct = float(data.get("risk_stop_pct", data.get("risk_stop_pct", 2.0)) or 2.0)

    # price: strongly recommended to pass in from TradingView (e.g. {{close}})
    price = data.get("price", None)
    try:
        price = float(price) if price is not None else None
    except:
        price = None

    if not symbol:
        return jsonify({"ok": False, "error": "Missing symbol"}), 400

    # Map events to ENTRY/EXIT
    if side != "long":
        return jsonify({"ok": False, "error": "Only long supported"}), 400

    if event in ("BUY", "ENTRY"):
        mapped = "ENTRY"
    elif event in ("SELL", "EXIT"):
        mapped = "EXIT"
    else:
        return jsonify({"ok": False, "error": "Unsupported event", "event": event}), 400

    # Cooldowns
    blocked, why = _cooldown_block(symbol)
    if blocked:
        log.warning("Cooldown blocked: %s", why)
        return jsonify({"ok": True, "blocked": True, "reason": why}), 200

    # Ensure formula tabs once (cheap enough). You can comment this out later.
    if SHEETS_ON:
        try:
            ensure_formula_tabs()
        except Exception as e:
            log.exception("Sheets formula tabs setup failed (non-fatal): %s", e)

    # Determine current state from Positions tab (persisted)
    current = pos_get(symbol) if SHEETS_ON else None
    state = (current.get("state") if current else "FLAT") or "FLAT"

    # --- No double ENTRY / EXIT ---
    if mapped == "ENTRY" and state == "LONG":
        return jsonify({"ok": True, "ignored": True, "reason": "Already in position (LONG)."}), 200
    if mapped == "EXIT" and state != "LONG":
        return jsonify({"ok": True, "ignored": True, "reason": "No open position to exit (FLAT)."}), 200

    # If price not provided, fetch (works in DRY_RUN too)
    fetched_price = None
    if price is None:
        try:
            at, api = qt_refresh_access_token()
            fetched_price = get_last_price(at, api, symbol)
            price = fetched_price
        except Exception as e:
            # In DRY_RUN we still want to proceed if they didn't send price
            return jsonify({"ok": False, "error": "Missing price and failed to fetch quote", "detail": str(e)}), 500

    ts = now_iso()

    # ENTRY logic
    if mapped == "ENTRY":
        shares, pos_val, stop_price, risk_used, note = _calc_shares(price, stop_pct)

        # Persist position state (so you can't double-enter)
        if SHEETS_ON:
            pos_upsert({
                "symbol": symbol,
                "state": "LONG",
                "entry_ts": ts,
                "entry_price": price,
                "shares": shares,
                "stop_pct": stop_pct,
                "stop_price": stop_price if stop_price is not None else "",
            })

        # Dry run / Live
        broker = None
        status = "dry_run" if DRY_RUN else "live"
        if not DRY_RUN:
            broker = qt_place_order(symbol, "Buy", shares)

        # Log row
        log_trade_row({
            "timestamp": ts,
            "symbol": symbol,
            "event": "BUY",
            "side": "long",
            "price": price,
            "shares": shares,
            "position_value": round(pos_val, 2),
            "stop_price": stop_price if stop_price is not None else "",
            "risk_usd": round(risk_used, 2) if risk_used else "",
            "status": status,
            "note": note,
            "pnl_usd": "",
        })

        return jsonify({
            "ok": True,
            "mapped": "ENTRY",
            "symbol": symbol,
            "price": price,
            "shares": shares,
            "position_value": round(pos_val, 2),
            "stop_price": stop_price,
            "risk_usd": round(risk_used, 2),
            "dry_run": DRY_RUN,
            "broker": broker,
        }), 200

    # EXIT logic
    if mapped == "EXIT":
        # Pull entry details from Positions tab so P&L is real
        if not current or current.get("state") != "LONG":
            return jsonify({"ok": True, "ignored": True, "reason": "No open position found."}), 200

        entry_price = float(current.get("entry_price") or 0.0)
        shares = int(current.get("shares") or 0)
        if shares <= 0 or entry_price <= 0:
            return jsonify({"ok": False, "error": "Bad stored position data", "current": current}), 500

        # Realized P&L (long)
        pnl = (price - entry_price) * shares
        pos_val = price * shares

        status = "dry_run" if DRY_RUN else "live"
        broker = None
        if not DRY_RUN:
            broker = qt_place_order(symbol, "Sell", shares)

        # Clear position (so you can't double-exit, and can re-enter later)
        if SHEETS_ON:
            pos_clear(symbol)

        # Log exit row with pnl_usd filled
        log_trade_row({
            "timestamp": ts,
            "symbol": symbol,
            "event": "EXIT",
            "side": "long",
            "price": price,
            "shares": shares,
            "position_value": round(pos_val, 2),
            "stop_price": "",      # stop not needed on exit
            "risk_usd": "",        # optional
            "status": status,
            "note": f"entry_price={entry_price} exit_price={price} shares={shares}",
            "pnl_usd": round(pnl, 2),
        })

        return jsonify({
            "ok": True,
            "mapped": "EXIT",
            "symbol": symbol,
            "entry_price": entry_price,
            "exit_price": price,
            "shares": shares,
            "pnl_usd": round(pnl, 2),
            "dry_run": DRY_RUN,
            "broker": broker,
        }), 200


# ------------------------------------------------------------------------------
# Local dev entry point
# ------------------------------------------------------------------------------

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    app.run(host="0.0.0.0", port=port)
