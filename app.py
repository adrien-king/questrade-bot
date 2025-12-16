import os
import time
import uuid
import json
import logging
from datetime import datetime, timezone
from collections import defaultdict

import requests
from flask import Flask, request, jsonify, g

# ==============================================================================
# Flask + logging setup
# ==============================================================================

app = Flask(__name__)

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO))
log = app.logger
log.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

# ==============================================================================
# Environment variables
# ==============================================================================

QUESTRADE_REFRESH_TOKEN = (os.getenv("QUESTRADE_REFRESH_TOKEN") or "").strip()
QUESTRADE_ACCOUNT_NUMBER = (os.getenv("QUESTRADE_ACCOUNT_NUMBER") or "").strip()
QUESTRADE_PRACTICE = (os.getenv("QUESTRADE_PRACTICE", "1") or "1").strip()  # "1"=practice, "0"=live

# Sizing
POSITION_DOLLARS = float(os.getenv("POSITION_DOLLARS", "1000"))
MAX_POSITION_USD = float(os.getenv("MAX_POSITION_USD", "0") or "0")  # optional cap
RISK_PER_TRADE = float(os.getenv("RISK_PER_TRADE", "50"))  # dollars risked per trade
USE_RISK_SIZING = (os.getenv("USE_RISK_SIZING", "0") == "1")  # 1 = size by risk, 0 = fixed dollars

# Dry run / paper trading
DRY_RUN = os.getenv("DRY_RUN", "1") == "1"
SIM_SLIPPAGE_PCT = float(os.getenv("SIM_SLIPPAGE_PCT", "0.00"))  # e.g. 0.05 = 0.05%
SIM_FEE_PER_TRADE = float(os.getenv("SIM_FEE_PER_TRADE", "0.00"))  # flat fee in dollars (paper)

# Cooldowns (seconds)
GLOBAL_COOLDOWN_SEC = int(os.getenv("GLOBAL_COOLDOWN_SEC", "0") or "0")
SYMBOL_COOLDOWN_SEC = int(os.getenv("SYMBOL_COOLDOWN_SEC", "0") or "0")

# Multi-symbol routing: JSON dict like {"PLRZ":"12345678","AAPL":"87654321"}
ACCOUNT_MAP_JSON = os.getenv("ACCOUNT_MAP_JSON", "").strip()
ACCOUNT_MAP = {}
if ACCOUNT_MAP_JSON:
    try:
        ACCOUNT_MAP = json.loads(ACCOUNT_MAP_JSON)
        if not isinstance(ACCOUNT_MAP, dict):
            ACCOUNT_MAP = {}
    except Exception:
        ACCOUNT_MAP = {}

# Basic validation
if not QUESTRADE_REFRESH_TOKEN:
    raise ValueError("Missing Questrade refresh token (QUESTRADE_REFRESH_TOKEN).")
if not QUESTRADE_ACCOUNT_NUMBER:
    raise ValueError("Missing Questrade account number (QUESTRADE_ACCOUNT_NUMBER).")

log.info(
    "Config loaded: PRACTICE=%s DRY_RUN=%s USE_RISK_SIZING=%s POSITION_DOLLARS=%.2f RISK_PER_TRADE=%.2f "
    "MAX_POSITION_USD=%.2f GLOBAL_COOLDOWN_SEC=%s SYMBOL_COOLDOWN_SEC=%s LOG_LEVEL=%s",
    QUESTRADE_PRACTICE, DRY_RUN, USE_RISK_SIZING, POSITION_DOLLARS, RISK_PER_TRADE,
    MAX_POSITION_USD, GLOBAL_COOLDOWN_SEC, SYMBOL_COOLDOWN_SEC, LOG_LEVEL
)

# ==============================================================================
# In-memory paper trading state (DRY_RUN)
# ==============================================================================

# Per symbol: shares + avg entry
PAPER_POSITIONS = defaultdict(lambda: {"shares": 0, "avg_price": 0.0, "last_entry_ts": 0.0})
PAPER_REALIZED_PNL = 0.0
PAPER_TRADES = []  # recent trade records (keep small)

MAX_PAPER_TRADES = int(os.getenv("MAX_PAPER_TRADES", "200") or "200")

# Cooldown tracking
LAST_ACTION_TS_GLOBAL = 0.0
LAST_ACTION_TS_BY_SYMBOL = defaultdict(lambda: 0.0)

# ==============================================================================
# Helpers: misc
# ==============================================================================

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def _safe_text(s: str, limit: int = 500) -> str:
    s = s or ""
    s = s.replace("\n", "\\n")
    return s[:limit]

def _redact(s: str) -> str:
    # basic redaction for tokens if ever logged
    if not s:
        return s
    # Don’t try to be perfect; just avoid obvious leaks
    return s.replace(QUESTRADE_REFRESH_TOKEN, "[REDACTED_REFRESH_TOKEN]") if QUESTRADE_REFRESH_TOKEN else s

def _account_for_symbol(symbol: str) -> str:
    # Multi-symbol routing
    mapped = ACCOUNT_MAP.get(symbol)
    return str(mapped).strip() if mapped else QUESTRADE_ACCOUNT_NUMBER

def _apply_slippage(price: float, action: str) -> float:
    # action: BUY or SELL
    if SIM_SLIPPAGE_PCT <= 0:
        return price
    slip = price * (SIM_SLIPPAGE_PCT / 100.0)
    return price + slip if action == "BUY" else price - slip

def _cooldown_ok(symbol: str) -> (bool, str):
    global LAST_ACTION_TS_GLOBAL
    now = time.time()

    if GLOBAL_COOLDOWN_SEC > 0:
        remaining = (LAST_ACTION_TS_GLOBAL + GLOBAL_COOLDOWN_SEC) - now
        if remaining > 0:
            return False, f"Global cooldown active ({remaining:.1f}s remaining)"

    if SYMBOL_COOLDOWN_SEC > 0:
        last = LAST_ACTION_TS_BY_SYMBOL[symbol]
        remaining = (last + SYMBOL_COOLDOWN_SEC) - now
        if remaining > 0:
            return False, f"Symbol cooldown active for {symbol} ({remaining:.1f}s remaining)"

    return True, ""

def _cooldown_mark(symbol: str):
    global LAST_ACTION_TS_GLOBAL
    ts = time.time()
    LAST_ACTION_TS_GLOBAL = ts
    LAST_ACTION_TS_BY_SYMBOL[symbol] = ts

# ==============================================================================
# Questrade helpers
# ==============================================================================

def _login_base_url() -> str:
    return "https://login.questrade.com"

def qt_refresh_access_token():
    url = f"{_login_base_url()}/oauth2/token"
    params = {
        "grant_type": "refresh_token",
        "refresh_token": QUESTRADE_REFRESH_TOKEN,
    }

    token_prefix = (QUESTRADE_REFRESH_TOKEN or "")[:5]
    log.info("qt_refresh_access_token: refreshing token token_prefix=%s...", token_prefix)

    r = requests.get(url, params=params, timeout=20)

    log.info("qt_refresh_access_token: status=%s body=%s", r.status_code, r.text[:400])

    if r.status_code != 200:
        raise Exception(f"Failed to refresh token: status={r.status_code} body={r.text}")

    data = r.json()
    return data["access_token"], data["api_server"]
    
def qt_headers(access_token: str) -> dict:
    return {"Authorization": f"Bearer {access_token}"}

def qt_get_symbol_id(access_token: str, api_server: str, symbol: str) -> int:
    url = f"{api_server}v1/symbols/search?prefix={symbol}"
    log.info("[%s] qt_get_symbol_id: GET %s", g.request_id, url)

    r = requests.get(url, headers=qt_headers(access_token), timeout=20)
    body = _safe_text(r.text, 600)
    log.info("[%s] qt_get_symbol_id: status=%s body=%s", g.request_id, r.status_code, body)

    if r.status_code != 200:
        raise Exception(f"Failed to lookup symbolId for {symbol}: status={r.status_code} body={body}")

    data = r.json()
    symbols = data.get("symbols", [])
    if not symbols:
        raise Exception(f"No symbols found for {symbol} (body={body})")

    exact = [s for s in symbols if s.get("symbol") == symbol]
    chosen = (exact[0] if exact else symbols[0])
    return int(chosen["symbolId"])

def get_last_price(access_token: str, api_server: str, symbol: str) -> float:
    url = f"{api_server}v1/markets/quotes/{symbol}"
    log.info("[%s] get_last_price: GET %s", g.request_id, url)

    r = requests.get(url, headers=qt_headers(access_token), timeout=20)
    body = _safe_text(r.text, 600)
    log.info("[%s] get_last_price: status=%s body=%s", g.request_id, r.status_code, body)

    if r.status_code != 200:
        raise Exception(f"Failed to get last price for {symbol}: status={r.status_code} body={body}")

    data = r.json()
    return float(data["quotes"][0]["lastTradePrice"])

def calc_shares(last_price: float, risk_stop_pct: float) -> int:
    """
    Two sizing modes:
      - fixed notional: POSITION_DOLLARS / last_price
      - risk sizing: RISK_PER_TRADE / (last_price * risk_stop_pct%)
    """
    if last_price <= 0:
        return 0

    if USE_RISK_SIZING:
        stop_dist = last_price * (max(0.0001, risk_stop_pct) / 100.0)
        raw = int(RISK_PER_TRADE // stop_dist) if stop_dist > 0 else 0
        shares = max(1, raw)
    else:
        shares = max(1, int(POSITION_DOLLARS // last_price))

    # optional max notional cap
    if MAX_POSITION_USD > 0:
        max_shares = int(MAX_POSITION_USD // last_price)
        shares = max(1, min(shares, max_shares))

    return shares

def qt_place_order(symbol: str, action: str, risk_stop_pct: float):
    """
    action: "BUY" or "SELL"
    Places a market order.
    """
    access_token, api_server = qt_refresh_access_token()
    symbol_id = qt_get_symbol_id(access_token, api_server, symbol)
    last_price = get_last_price(access_token, api_server, symbol)

    shares = calc_shares(last_price, risk_stop_pct)

    account_number = _account_for_symbol(symbol)

    leg_side = "Buy" if action == "BUY" else "Sell"

    order_body = {
        "accountNumber": account_number,
        "orderType": "Market",
        "timeInForce": "Day",
        "primaryRoute": "AUTO",
        "secondaryRoute": "AUTO",
        "isAllOrNone": False,
        "isAnonymous": False,
        "orderLegs": [{"symbolId": symbol_id, "legSide": leg_side, "quantity": shares}],
    }

    url = f"{api_server}v1/accounts/{account_number}/orders"
    log.info("[%s] qt_place_order: %s %s shares=%s last_price=%.4f risk_stop_pct=%.2f url=%s body=%s",
             g.request_id, action, symbol, shares, last_price, risk_stop_pct, url, order_body)

    r = requests.post(url, headers=qt_headers(access_token), json=order_body, timeout=20)
    body = _safe_text(r.text, 900)
    log.info("[%s] qt_place_order: response status=%s body=%s", g.request_id, r.status_code, body)

    if r.status_code >= 300:
        raise Exception(f"Order rejected: status={r.status_code} body={body}")

    return r.json()

# ==============================================================================
# DRY_RUN simulator (paper positions + P&L)
# ==============================================================================

def paper_entry(symbol: str, price: float, risk_stop_pct: float) -> dict:
    global PAPER_REALIZED_PNL

    fill_price = _apply_slippage(price, "BUY")
    shares = calc_shares(fill_price, risk_stop_pct)
    pos = PAPER_POSITIONS[symbol]

    old_shares = pos["shares"]
    old_avg = pos["avg_price"]

    new_shares = old_shares + shares
    new_avg = ((old_shares * old_avg) + (shares * fill_price)) / new_shares

    pos["shares"] = new_shares
    pos["avg_price"] = new_avg
    pos["last_entry_ts"] = time.time()

    record = {
        "ts": now_iso(),
        "symbol": symbol,
        "action": "BUY",
        "shares": shares,
        "fill_price": round(fill_price, 6),
        "new_position_shares": new_shares,
        "new_position_avg": round(new_avg, 6),
    }
    PAPER_TRADES.append(record)
    del PAPER_TRADES[:-MAX_PAPER_TRADES]

    log.info("[%s] [DRY_RUN ENTRY] %s +%s @ %.4f -> pos=%s @ %.4f (risk_stop_pct=%.2f USE_RISK_SIZING=%s)",
             g.request_id, symbol, shares, fill_price, new_shares, new_avg, risk_stop_pct, USE_RISK_SIZING)

    return record

def paper_exit(symbol: str, price: float) -> dict:
    global PAPER_REALIZED_PNL

    pos = PAPER_POSITIONS[symbol]
    shares = pos["shares"]
    entry = pos["avg_price"]

    if shares <= 0:
        record = {"ts": now_iso(), "symbol": symbol, "action": "EXIT", "note": "no_position"}
        PAPER_TRADES.append(record)
        del PAPER_TRADES[:-MAX_PAPER_TRADES]
        log.info("[%s] [DRY_RUN EXIT] %s no open paper position", g.request_id, symbol)
        return record

    fill_price = _apply_slippage(price, "SELL")

    gross = (fill_price - entry) * shares
    net = gross - float(SIM_FEE_PER_TRADE)
    PAPER_REALIZED_PNL += net

    record = {
        "ts": now_iso(),
        "symbol": symbol,
        "action": "EXIT",
        "shares": shares,
        "entry_avg": round(entry, 6),
        "exit_price": round(fill_price, 6),
        "gross_pnl": round(gross, 2),
        "fee": round(float(SIM_FEE_PER_TRADE), 2),
        "net_pnl": round(net, 2),
        "realized_pnl_total": round(PAPER_REALIZED_PNL, 2),
    }
    PAPER_TRADES.append(record)
    del PAPER_TRADES[:-MAX_PAPER_TRADES]

    PAPER_POSITIONS[symbol] = {"shares": 0, "avg_price": 0.0, "last_entry_ts": 0.0}

    log.info("[%s] [DRY_RUN EXIT] %s -%s @ %.4f (entry %.4f) net_pnl=%.2f total=%.2f",
             g.request_id, symbol, shares, fill_price, entry, net, PAPER_REALIZED_PNL)

    return record

def paper_unrealized(symbol: str, mark: float) -> float:
    pos = PAPER_POSITIONS[symbol]
    if pos["shares"] <= 0:
        return 0.0
    return (mark - pos["avg_price"]) * pos["shares"]

# ==============================================================================
# Request hooks
# ==============================================================================

@app.before_request
def _before():
    g.request_id = request.headers.get("X-Request-Id") or str(uuid.uuid4())[:8]
    g.start_ts = time.time()

@app.after_request
def _after(resp):
    dur_ms = int((time.time() - g.start_ts) * 1000)
    resp.headers["X-Request-Id"] = g.request_id
    log.info("[%s] %s %s -> %s (%sms)", g.request_id, request.method, request.path, resp.status_code, dur_ms)
    return resp

# ==============================================================================
# Routes
# ==============================================================================

@app.route("/health", methods=["GET"])
def health():
    return "OK", 200

@app.route("/status", methods=["GET"])
def status():
    """
    Shows paper positions + realized P&L (useful in DRY_RUN).
    """
    if not DRY_RUN:
        return jsonify({"ok": True, "mode": "live", "note": "paper status is primarily for DRY_RUN"}), 200

    # compute unrealized using latest known prices only if user supplies ?mark=SYMBOL:PRICE,...
    marks = {}
    mark_q = (request.args.get("mark") or "").strip()
    # example: ?mark=PLRZ:1.23,KALA:5.67
    if mark_q:
        for chunk in mark_q.split(","):
            try:
                sym, px = chunk.split(":")
                marks[sym.upper().strip()] = float(px)
            except Exception:
                pass

    positions_out = {}
    unreal_total = 0.0
    for sym, pos in PAPER_POSITIONS.items():
        if pos["shares"] <= 0:
            continue
        mark = marks.get(sym)
        u = paper_unrealized(sym, mark) if mark is not None else None
        if u is not None:
            unreal_total += u
        positions_out[sym] = {
            "shares": pos["shares"],
            "avg_price": pos["avg_price"],
            "unrealized": (round(u, 2) if u is not None else None),
        }

    return jsonify({
        "ok": True,
        "dry_run": True,
        "realized_pnl": round(PAPER_REALIZED_PNL, 2),
        "unrealized_pnl_total": (round(unreal_total, 2) if marks else None),
        "positions": positions_out,
        "recent_trades": PAPER_TRADES[-30:],
        "use_risk_sizing": USE_RISK_SIZING,
        "risk_per_trade": RISK_PER_TRADE,
        "position_dollars": POSITION_DOLLARS,
        "sim_slippage_pct": SIM_SLIPPAGE_PCT,
        "sim_fee_per_trade": SIM_FEE_PER_TRADE,
    }), 200

@app.route("/tv", methods=["GET", "POST"])
def tv():
    """
    TradingView webhook endpoint.

    JSON example:
    {
      "symbol": "PLRZ",
      "event": "BUY" | "ENTRY" | "SELL" | "EXIT",
      "side": "long",
      "risk_stop_pct": 2.0
    }
    """
    raw_body = request.get_data(as_text=True)
    log.info("[%s] TV raw body: %s", g.request_id, _safe_text(raw_body, 1200))

    if request.method != "POST":
        return jsonify({"ok": False, "error": "Use POST with JSON body"}), 405

    try:
        data = request.get_json(force=True) or {}
    except Exception as e:
        log.exception("[%s] TV JSON parse error", g.request_id)
        return jsonify({"ok": False, "error": "Bad JSON", "detail": str(e)}), 400

    symbol = str(data.get("symbol", "")).upper().strip()
    event = str(data.get("event", "")).upper().strip()
    side = str(data.get("side", "")).lower().strip()
    risk_stop_pct = float(data.get("risk_stop_pct", 2.0))

    log.info("[%s] TV parsed -> symbol=%s event=%s side=%s risk_stop_pct=%.2f DRY_RUN=%s",
             g.request_id, symbol, event, side, risk_stop_pct, DRY_RUN)

    if not symbol:
        return jsonify({"ok": False, "error": "Missing symbol", "received": data}), 400

    # Map TradingView event to action
    action = None  # BUY or SELL
    if side == "long" and event in ("BUY", "ENTRY"):
        action = "BUY"
    elif side == "long" and event in ("SELL", "EXIT"):
        action = "SELL"
    else:
        log.warning("[%s] Unsupported combo event=%s side=%s", g.request_id, event, side)
        return jsonify({"ok": False, "error": "Unsupported event/side combo", "event": event, "side": side}), 400

    # Cooldown protection
    ok, why = _cooldown_ok(symbol)
    if not ok:
        log.warning("[%s] Cooldown blocked %s %s: %s", g.request_id, action, symbol, why)
        return jsonify({"ok": False, "error": "cooldown", "detail": why}), 429

    # DRY RUN: simulate fills + P&L
    if DRY_RUN:
        try:
            # We still fetch a price from Questrade for realism (you can remove this if you want)
            access_token, api_server = qt_refresh_access_token()
            last_price = get_last_price(access_token, api_server, symbol)

            if action == "BUY":
                rec = paper_entry(symbol, last_price, risk_stop_pct)
            else:
                rec = paper_exit(symbol, last_price)

            _cooldown_mark(symbol)

            return jsonify({
                "ok": True,
                "dry_run": True,
                "symbol": symbol,
                "event": event,
                "side": side,
                "mapped_action": action,
                "risk_stop_pct": risk_stop_pct,
                "last_price": last_price,
                "paper_result": rec
            }), 200

        except Exception as e:
            log.exception("[%s] DRY_RUN error", g.request_id)
            return jsonify({"ok": False, "error": "dry_run_failed", "detail": str(e)}), 500

    # LIVE: place order
    try:
        result = qt_place_order(symbol, action, risk_stop_pct)
        _cooldown_mark(symbol)

        return jsonify({
            "ok": True,
            "dry_run": False,
            "symbol": symbol,
            "event": event,
            "side": side,
            "mapped_action": action,
            "risk_stop_pct": risk_stop_pct,
            "broker_result": result,
        }), 200

    except Exception as e:
        log.exception("[%s] LIVE order failed", g.request_id)
        return jsonify({"ok": False, "error": "order_failed", "detail": str(e)}), 500

# ==============================================================================
# Entry point for local dev (Render uses gunicorn)
# ==============================================================================

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    app.run(host="0.0.0.0", port=port)
