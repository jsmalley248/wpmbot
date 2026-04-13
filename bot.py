"""
Weather Prediction Market Bot
================================
Scans Kalshi weather markets, computes edge vs. NWS forecast + historical
base rates, logs alerts to SQLite, and prints a paper-trading summary.

Usage:
    python bot.py               # Run one scan and print alerts
    python bot.py --watch       # Loop every 15 min
    python bot.py --summary     # Print paper trading P&L summary
    python bot.py --auto-trade  # Scan and auto-log paper trades for edge alerts
    python bot.py --watch --auto-trade  # Watch mode with auto paper trading

Requirements:
    pip install requests schedule rich

Environment:
    KALSHI_API_KEY  — set this to your Kalshi API key (required for most endpoints)
"""

import argparse
import math
import os
import re
import smtplib
import sqlite3
import time
from datetime import date, datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional

import requests
import schedule
from rich import box
from rich.console import Console
from rich.table import Table

console = Console()

# ─── Config ────────────────────────────────────────────────────────────────────

KALSHI_BASE    = "https://api.elections.kalshi.com/trade-api/v2"
NWS_BASE       = "https://api.weather.gov"
DB_PATH        = "weather_bot.db"
EDGE_THRESHOLD = 5.0   # minimum % edge to trigger an alert
KALSHI_API_KEY = os.getenv("KALSHI_API_KEY", "")
AUTO_TRADE     = False  # set to True via --auto-trade CLI flag

# ─── Email Config ───────────────────────────────────────────────────────────────
EMAIL_TO            = os.getenv("EMAIL_TO", "js277416@gmail.com")
EMAIL_FROM          = os.getenv("GMAIL_ADDRESS", "")
GMAIL_APP_PASSWORD  = os.getenv("GMAIL_APP_PASSWORD", "")

# NWS weight decays linearly with forecast horizon:
# 0.80 at 1 day out → 0.40 at 7+ days out (base rate fills the rest)
NWS_WEIGHT_NEAR = 0.80
NWS_WEIGHT_FAR  = 0.40

# Cities to monitor — grid coords are auto-resolved at startup via NWS /points/
CITIES = [
    {"name": "New York",    "series": "KXHIGHNY",      "lat": 40.7128, "lon": -74.0060},
    {"name": "Chicago",     "series": "KXHIGHCHI",     "lat": 41.8781, "lon": -87.6298},
    {"name": "Los Angeles", "series": "KXHIGHLA",      "lat": 34.0522, "lon": -118.2437},
    {"name": "Miami",       "series": "KXHIGHMIAMI",   "lat": 25.7617, "lon": -80.1918},
    {"name": "Dallas",      "series": "KXHIGHDAL",     "lat": 32.7767, "lon": -96.7970},
    {"name": "Seattle",       "series": "KXHIGHSEATTLE", "lat": 47.6062, "lon": -122.3321},
    {"name": "Philadelphia",  "series": "KXHIGHPHIL",   "lat": 39.9526, "lon": -75.1652},
    {"name": "Minneapolis",   "series": "KXHIGHTMIN",   "lat": 44.9778, "lon": -93.2650},
    {"name": "Washington DC", "series": "KXHIGHTDC",    "lat": 38.9072, "lon": -77.0369},
    {"name": "Atlanta",       "series": "KXHIGHTATL",   "lat": 33.7490, "lon": -84.3880},
]

# ─── NWS Grid Resolution ──────────────────────────────────────────────────────

def resolve_nws_grid(lat: float, lon: float) -> Optional[dict]:
    """
    Use NWS /points/ to get the correct office and grid coordinates for a
    lat/lon pair. Returns {"nws_office", "grid_x", "grid_y"} or None on error.
    Using /points/ avoids stale or wrong hardcoded grid values.
    """
    try:
        r = requests.get(
            f"{NWS_BASE}/points/{lat},{lon}",
            headers={"User-Agent": "weather-prediction-bot/1.0 (js277416@gmail.com)"},
            timeout=10,
        )
        r.raise_for_status()
        props = r.json()["properties"]
        return {
            "nws_office": props["gridId"],
            "grid_x":     props["gridX"],
            "grid_y":     props["gridY"],
        }
    except Exception as e:
        console.print(f"[yellow]NWS grid resolve error ({lat},{lon}): {e}[/yellow]")
        return None


def resolve_all_grids():
    """Resolve NWS grid coords for all cities at startup; patches CITIES in-place."""
    for city in CITIES:
        if "nws_office" in city:
            continue
        result = resolve_nws_grid(city["lat"], city["lon"])
        if result:
            city.update(result)
            console.print(
                f"[dim]Grid: {city['name']} -> "
                f"{result['nws_office']} ({result['grid_x']},{result['grid_y']})[/dim]"
            )
        else:
            console.print(f"[red]Could not resolve NWS grid for {city['name']} — will skip.[/red]")

# ─── Database ─────────────────────────────────────────────────────────────────

def init_db():
    with sqlite3.connect(DB_PATH) as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS alerts (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                ts           TEXT,
                ticker       TEXT,
                city         TEXT,
                question     TEXT,
                side         TEXT,
                market_price REAL,
                nws_prob     REAL,
                base_rate    REAL,
                blended_prob REAL,
                edge         REAL,
                volume       INTEGER,
                horizon      TEXT,
                date         TEXT,
                UNIQUE(ticker, date)
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS paper_trades (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_entered     TEXT,
                ts_resolved    TEXT,
                ticker         TEXT,
                city           TEXT,
                side           TEXT,
                entry_price    REAL,
                model_prob     REAL,
                edge           REAL,
                contracts      INTEGER,
                kelly_fraction REAL,
                outcome        TEXT,
                pnl_cents      REAL
            )
        """)
        con.commit()


def log_alert(alert: dict):
    # INSERT OR IGNORE prevents duplicate alerts for the same ticker on the same day
    with sqlite3.connect(DB_PATH) as con:
        con.execute("""
            INSERT OR IGNORE INTO alerts
                (ts, ticker, city, question, side, market_price, nws_prob,
                 base_rate, blended_prob, edge, volume, horizon, date)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            datetime.now(timezone.utc).isoformat(),
            alert["ticker"], alert["city"], alert["question"],
            alert["side"], alert["market_price"], alert["nws_prob"],
            alert["base_rate"], alert["blended_prob"], alert["edge"],
            alert["volume"], alert["horizon"],
            datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        ))
        con.commit()


def log_paper_trade(ticker, city, side, entry_price, model_prob, edge, contracts=None):
    """
    Log a paper trade. Contracts defaults to a Kelly-sized position (capped at 50).
    kelly_fraction is stored for later analysis.
    """
    kf = kelly_fraction(model_prob / 100.0, entry_price, side)
    if contracts is None:
        # Assumes a paper bankroll of 10,000 cents ($100) — adjust to taste
        contracts = max(1, min(50, round(kf * 10_000 / max(entry_price, 1))))
    with sqlite3.connect(DB_PATH) as con:
        con.execute("""
            INSERT INTO paper_trades
                (ts_entered, ticker, city, side, entry_price, model_prob,
                 edge, contracts, kelly_fraction, outcome, pnl_cents)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (
            datetime.now(timezone.utc).isoformat(),
            ticker, city, side, entry_price, model_prob, edge,
            contracts, round(kf, 4), "open", 0.0,
        ))
        con.commit()
    console.print(
        f"[green]Paper trade logged:[/green] {side} {ticker} @ {entry_price}¢  "
        f"({contracts} contracts, Kelly={kf:.1%})"
    )


def has_open_paper_trade(ticker: str) -> bool:
    """Return True if there is already an open paper trade for this ticker."""
    with sqlite3.connect(DB_PATH) as con:
        row = con.execute(
            "SELECT 1 FROM paper_trades WHERE ticker=? AND outcome='open' LIMIT 1",
            (ticker,),
        ).fetchone()
    return row is not None


def resolve_paper_trade(ticker: str, outcome: str):
    """outcome: 'yes' or 'no'. Resolves all open paper trades for a ticker."""
    with sqlite3.connect(DB_PATH) as con:
        rows = con.execute(
            "SELECT id, side, entry_price, contracts FROM paper_trades "
            "WHERE ticker=? AND outcome='open'",
            (ticker,),
        ).fetchall()
        for id_, side, entry_price, contracts in rows:
            if (side == "YES" and outcome == "yes") or (side == "NO" and outcome == "no"):
                pnl, result = (100 - entry_price) * contracts, "win"
            else:
                pnl, result = -entry_price * contracts, "loss"
            con.execute(
                "UPDATE paper_trades SET outcome=?, pnl_cents=?, ts_resolved=? WHERE id=?",
                (result, pnl, datetime.now(timezone.utc).isoformat(), id_),
            )
            console.print(
                f"[{'green' if result == 'win' else 'red'}]"
                f"Resolved {ticker}: {result}  P&L: {pnl / 100:+.2f}[/]"
            )
        con.commit()

# ─── Kalshi API ───────────────────────────────────────────────────────────────

def _kalshi_headers() -> dict:
    if KALSHI_API_KEY:
        return {"Authorization": f"Bearer {KALSHI_API_KEY}"}
    return {}


def fetch_kalshi_markets(series_ticker: str) -> list[dict]:
    """Fetch open markets for a given series."""
    try:
        r = requests.get(
            f"{KALSHI_BASE}/markets",
            params={"series_ticker": series_ticker, "status": "open", "limit": 100},
            headers=_kalshi_headers(),
            timeout=10,
        )
        r.raise_for_status()
        return r.json().get("markets", [])
    except Exception as e:
        console.print(f"[yellow]Kalshi fetch error ({series_ticker}): {e}[/yellow]")
        return []


def parse_market(market: dict) -> Optional[dict]:
    """Extract key fields from a raw Kalshi market dict. Prices in cents (0–100)."""
    try:
        yes_ask = market.get("yes_ask", 0)
        yes_bid = market.get("yes_bid", 0)
        mid_yes = (yes_bid + yes_ask) / 2 if yes_ask else None
        return {
            "ticker":     market["ticker"],
            "subtitle":   market.get("subtitle", ""),
            "close_time": market.get("close_time", ""),
            "mid_yes":    mid_yes,
            "yes_bid":    yes_bid,
            "yes_ask":    yes_ask,
            "no_bid":     market.get("no_bid", 0),
            "no_ask":     market.get("no_ask", 0),
            "volume":     market.get("volume", 0),
        }
    except Exception:
        return None

# ─── NWS Forecast ─────────────────────────────────────────────────────────────

def fetch_nws_hourly(office: str, grid_x: int, grid_y: int) -> list[dict]:
    """Fetch hourly forecast from NWS gridpoint API (free, no key needed)."""
    try:
        r = requests.get(
            f"{NWS_BASE}/gridpoints/{office}/{grid_x},{grid_y}/forecast/hourly",
            headers={"User-Agent": "weather-prediction-bot/1.0 (js277416@gmail.com)"},
            timeout=15,
        )
        r.raise_for_status()
        return r.json()["properties"]["periods"]
    except Exception as e:
        console.print(f"[yellow]NWS fetch error ({office}): {e}[/yellow]")
        return []


def nws_prob_threshold(
    periods: list[dict],
    threshold_f: float,
    target_date: date,
    above: bool,
) -> float:
    """
    P(daily high [above|below] threshold_f) for target_date.

    Filters hourly NWS periods to those starting on target_date so the forecast
    matches the market's resolution date (not just today). Falls back to the
    first 24 hours if NWS data doesn't extend that far.

    Logistic curve (k=0.25): ±8°F gives meaningful probability spread.
    """
    if not periods:
        return 0.5

    date_str    = target_date.isoformat()   # "YYYY-MM-DD"
    day_periods = [p for p in periods if p.get("startTime", "").startswith(date_str)]
    if not day_periods:
        day_periods = periods[:24]          # fallback: nearest 24 h

    daytime = [p["temperature"] for p in day_periods if p.get("isDaytime", True)]
    temps   = daytime if daytime else [p["temperature"] for p in day_periods]
    if not temps:
        return 0.5

    diff = max(temps) - threshold_f
    prob = 1 / (1 + math.exp(-0.25 * diff))   # P(high > threshold)
    return round(prob if above else 1 - prob, 4)

# ─── Historical Base Rate ─────────────────────────────────────────────────────

# city → month (1–12) → (avg_high_F, std_F)
# In production replace with NOAA ACIS or a local CSV.
CLIMATOLOGY = {
    "New York":    {1:(38,8),  2:(41,8),  3:(51,9),  4:(62,9),  5:(72,8),  6:(81,8),
                   7:(85,7),  8:(84,7),  9:(76,8),  10:(65,8), 11:(54,8), 12:(43,8)},
    "Chicago":     {1:(32,9),  2:(36,9),  3:(47,10), 4:(59,10), 5:(70,9),  6:(80,8),
                   7:(84,7),  8:(82,7),  9:(74,8),  10:(62,9), 11:(48,9), 12:(36,9)},
    "Los Angeles": {1:(68,7),  2:(69,7),  3:(70,7),  4:(73,7),  5:(75,6),  6:(80,6),
                   7:(84,5),  8:(85,5),  9:(83,6),  10:(79,6), 11:(73,7), 12:(68,7)},
    "Miami":       {1:(75,5),  2:(77,5),  3:(80,5),  4:(84,5),  5:(87,4),  6:(90,4),
                   7:(91,4),  8:(91,4),  9:(89,4),  10:(85,5), 11:(80,5), 12:(76,5)},
    "Dallas":      {1:(56,9),  2:(60,9),  3:(68,9),  4:(76,9),  5:(83,8),  6:(91,7),
                   7:(96,6),  8:(96,6),  9:(89,7),  10:(79,8), 11:(66,9), 12:(57,9)},
    "Seattle":       {1:(46,6),  2:(50,6),  3:(54,6),  4:(59,7),  5:(65,7),  6:(70,7),
                     7:(76,6),  8:(77,6),  9:(71,7),  10:(59,7), 11:(50,6), 12:(45,6)},
    "Philadelphia":  {1:(40,8),  2:(43,8),  3:(53,9),  4:(64,9),  5:(74,8),  6:(83,8),
                     7:(88,7),  8:(86,7),  9:(78,8),  10:(67,8), 11:(55,8), 12:(44,8)},
    "Minneapolis":   {1:(24,9),  2:(29,9),  3:(42,10), 4:(57,10), 5:(69,9),  6:(79,8),
                     7:(83,7),  8:(81,7),  9:(71,8),  10:(58,9), 11:(40,9), 12:(26,9)},
    "Washington DC": {1:(43,8),  2:(46,8),  3:(56,9),  4:(67,9),  5:(76,8),  6:(85,8),
                     7:(89,7),  8:(87,7),  9:(80,8),  10:(69,8), 11:(58,8), 12:(46,8)},
    "Atlanta":       {1:(52,8),  2:(57,8),  3:(65,9),  4:(73,8),  5:(80,7),  6:(87,6),
                     7:(90,5),  8:(89,5),  9:(83,7),  10:(73,8), 11:(63,8), 12:(54,8)},
}


def base_rate_prob(city_name: str, threshold_f: float, above: bool) -> float:
    """
    P(high [above|below] threshold) from monthly climatology.
    Uses a logistic approximation of the normal CDF (accurate to ±2%).
    """
    clim = CLIMATOLOGY.get(city_name, {}).get(datetime.now().month)
    if not clim:
        return 0.5
    mean, std = clim
    z    = (threshold_f - mean) / std
    prob = 1 / (1 + math.exp(1.7 * z))   # ≈ P(high > threshold)
    return round(prob if above else 1 - prob, 4)

# ─── Position Sizing ──────────────────────────────────────────────────────────

def kelly_fraction(p: float, price_cents: float, side: str) -> float:
    """
    Full Kelly fraction for a binary Kalshi contract.

    p          : our estimated P(YES resolves) — 0 to 1
    price_cents: mid price in cents (e.g. 48)
    side       : 'YES' or 'NO'

    Returns fraction of bankroll to risk, clamped to [0, 1].
    Use half-Kelly in practice: multiply result by 0.5.
    """
    if side == "NO":
        p           = 1 - p
        price_cents = 100 - price_cents
    if price_cents <= 0 or price_cents >= 100:
        return 0.0
    b = (100 - price_cents) / price_cents   # net odds per unit risked
    f = (p * (b + 1) - 1) / b
    return round(max(0.0, min(1.0, f)), 4)

# ─── Blending ─────────────────────────────────────────────────────────────────

def nws_weight(close_time_str: str) -> float:
    """
    NWS weight decays linearly from NWS_WEIGHT_NEAR (1-day) to NWS_WEIGHT_FAR
    (7-day). Beyond 7 days NWS skill approaches base-rate level.
    """
    try:
        close_dt = datetime.fromisoformat(close_time_str.replace("Z", "+00:00"))
        days_out = max(1, min((close_dt - datetime.now(timezone.utc)).days, 7))
        t        = (days_out - 1) / 6.0   # 0 → day 1, 1 → day 7
        return NWS_WEIGHT_NEAR + t * (NWS_WEIGHT_FAR - NWS_WEIGHT_NEAR)
    except Exception:
        return (NWS_WEIGHT_NEAR + NWS_WEIGHT_FAR) / 2

# ─── Threshold Parsing ────────────────────────────────────────────────────────

def extract_threshold(subtitle: str) -> Optional[tuple[float, bool]]:
    """
    Parse a temperature threshold from a Kalshi market subtitle.
    Returns (threshold_f, above) where above=True means P(high > threshold).
    Returns None for range markets ("55° to 60°") or unparseable subtitles.

    Examples:
        "> 55°"      → (55.0, True)
        "< 40°"      → (40.0, False)
        "55° to 60°" → None   (skipped — range model not implemented)
    """
    if re.search(r"\d+\s*°?\s*to\s*\d+", subtitle, re.IGNORECASE):
        return None                          # range market — skip
    nums = re.findall(r"[\d.]+", subtitle)
    if not nums:
        return None
    above = "<" not in subtitle              # absent or ">" → above threshold
    return (float(nums[0]), above)


def best_side(blended_prob: float, market_price_cents: float) -> tuple[str, float]:
    """Return the better side to trade and its edge in percentage points."""
    yes_edge = (blended_prob - market_price_cents / 100) * 100
    no_edge  = ((1 - blended_prob) - (1 - market_price_cents / 100)) * 100
    if yes_edge >= no_edge:
        return "YES", round(yes_edge, 2)
    return "NO", round(no_edge, 2)

# ─── Main Scan ────────────────────────────────────────────────────────────────

def scan_city(city: dict) -> list[dict]:
    if "nws_office" not in city:
        return []   # grid resolution failed at startup

    markets_raw = fetch_kalshi_markets(city["series"])
    if not markets_raw:
        return []

    nws_periods = fetch_nws_hourly(city["nws_office"], city["grid_x"], city["grid_y"])
    alerts      = []

    for raw in markets_raw:
        m = parse_market(raw)
        if not m or m["mid_yes"] is None:
            continue

        parsed = extract_threshold(m["subtitle"])
        if parsed is None:
            continue   # range market or unparseable
        threshold_f, above = parsed

        # Determine which calendar date the market resolves on
        try:
            close_date = datetime.fromisoformat(
                m["close_time"].replace("Z", "+00:00")
            ).date()
        except Exception:
            close_date = date.today()

        nws_prob  = nws_prob_threshold(nws_periods, threshold_f, close_date, above)
        base_rate = base_rate_prob(city["name"], threshold_f, above)
        w         = nws_weight(m["close_time"])
        blended   = round(w * nws_prob + (1 - w) * base_rate, 4)

        side, edge = best_side(blended, m["mid_yes"])

        if edge >= EDGE_THRESHOLD and m["volume"] >= 10:
            alert = {
                "ticker":       m["ticker"],
                "city":         city["name"],
                "question":     f"{city['name']} high temp {m['subtitle']}?",
                "side":         side,
                "market_price": m["mid_yes"],
                "nws_prob":     round(nws_prob * 100, 1),
                "base_rate":    round(base_rate * 100, 1),
                "blended_prob": round(blended * 100, 1),
                "edge":         edge,
                "volume":       m["volume"],
                "horizon":      m["close_time"][:10] if m["close_time"] else "?",
                "yes_bid":      m["yes_bid"],
                "yes_ask":      m["yes_ask"],
            }
            alerts.append(alert)
            log_alert(alert)
            if AUTO_TRADE and not has_open_paper_trade(alert["ticker"]):
                entry = alert["yes_ask"] if alert["side"] == "YES" else (100 - alert["yes_bid"])
                log_paper_trade(alert["ticker"], alert["city"], alert["side"],
                                entry, alert["blended_prob"], alert["edge"])

    return alerts


def send_email_alert(alerts: list):
    if not EMAIL_FROM or not GMAIL_APP_PASSWORD:
        return  # email not configured, skip silently
    try:
        body_lines = ["Trade alerts from Weather Bot\n"]
        for a in alerts:
            body_lines.append(
                f"  {a['city']} | {a['ticker']} | {a['side']} @ {a['market_price']:.0f}¢ "
                f"| Blended: {a['blended_prob']}% | Edge: +{a['edge']}%"
            )
        body = "\n".join(body_lines)

        msg = MIMEMultipart()
        msg["From"]    = EMAIL_FROM
        msg["To"]      = EMAIL_TO
        msg["Subject"] = f"[WeatherBot] {len(alerts)} alert(s) — {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        msg.attach(MIMEText(body, "plain"))

        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(EMAIL_FROM, GMAIL_APP_PASSWORD)
            server.sendmail(EMAIL_FROM, EMAIL_TO, msg.as_string())
    except Exception as e:
        console.print(f"[red]Email failed: {e}[/red]")


def run_scan():
    console.rule("[bold]Weather Bot Scan[/bold]")
    console.print(f"[dim]{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}[/dim]\n")

    all_alerts = []
    for city in CITIES:
        console.print(f"[dim]Scanning {city['name']}...[/dim]")
        all_alerts.extend(scan_city(city))

    if not all_alerts:
        console.print("[yellow]No alerts above threshold.[/yellow]")
        return

    all_alerts.sort(key=lambda a: a["edge"], reverse=True)

    table = Table(box=box.SIMPLE_HEAVY, show_header=True, header_style="bold")
    table.add_column("City",      style="cyan",  min_width=12)
    table.add_column("Market",    style="white", min_width=16)
    table.add_column("Side",      style="bold",  min_width=5)
    table.add_column("Mkt price", justify="right", min_width=9)
    table.add_column("NWS prob",  justify="right", min_width=9)
    table.add_column("Base rate", justify="right", min_width=10)
    table.add_column("Blended",   justify="right", min_width=9)
    table.add_column("Edge",      justify="right", style="green", min_width=7)
    table.add_column("Volume",    justify="right", min_width=7)

    for a in all_alerts:
        side_color = "green" if a["side"] == "YES" else "red"
        table.add_row(
            a["city"], a["ticker"],
            f"[{side_color}]{a['side']}[/{side_color}]",
            f"{a['market_price']:.0f}¢",
            f"{a['nws_prob']}%",
            f"{a['base_rate']}%",
            f"{a['blended_prob']}%",
            f"+{a['edge']}%",
            str(a["volume"]),
        )

    console.print(table)
    console.print(f"\n[bold]{len(all_alerts)}[/bold] alert(s) logged to [cyan]{DB_PATH}[/cyan]")
    send_email_alert(all_alerts)

# ─── Paper Trading Summary ────────────────────────────────────────────────────

def print_summary():
    with sqlite3.connect(DB_PATH) as con:
        rows = con.execute("""
            SELECT ticker, city, side, entry_price, model_prob, edge,
                   contracts, kelly_fraction, outcome, pnl_cents, ts_entered
            FROM paper_trades
            ORDER BY ts_entered DESC
        """).fetchall()

    if not rows:
        console.print("[yellow]No paper trades logged yet.[/yellow]")
        return

    table = Table(box=box.SIMPLE_HEAVY, header_style="bold")
    table.add_column("Ticker");  table.add_column("City")
    table.add_column("Side");    table.add_column("Entry")
    table.add_column("Model");   table.add_column("Edge")
    table.add_column("Qty");     table.add_column("Kelly")
    table.add_column("Outcome"); table.add_column("P&L", justify="right")
    table.add_column("Date",     style="dim")

    total_pnl = wins = losses = open_trades = 0

    for ticker, city, side, entry, model, edge, qty, kf, outcome, pnl, ts in rows:
        pnl_dollars = pnl / 100
        total_pnl  += pnl_dollars
        color = "green" if outcome == "win" else "red" if outcome == "loss" else "dim"
        if outcome == "win":    wins += 1
        elif outcome == "loss": losses += 1
        else:                   open_trades += 1

        table.add_row(
            ticker, city,
            f"[{'green' if side == 'YES' else 'red'}]{side}[/]",
            f"{entry:.0f}¢", f"{model:.0f}%", f"+{edge:.1f}%",
            str(qty), f"{(kf or 0):.1%}",
            f"[{color}]{outcome}[/{color}]",
            f"[{'green' if pnl_dollars >= 0 else 'red'}]{pnl_dollars:+.2f}[/]",
            ts[:10],
        )

    console.print(table)
    resolved = wins + losses
    wr = (wins / resolved * 100) if resolved else 0
    console.print(
        f"\nTotal P&L: [{'green' if total_pnl >= 0 else 'red'}]{total_pnl:+.2f}[/]  "
        f"| Win rate: {wr:.0f}% ({wins}W / {losses}L)  "
        f"| Open: {open_trades}"
    )

# ─── CLI ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Weather Prediction Market Bot")
    parser.add_argument("--watch",   action="store_true", help="Scan every 15 minutes")
    parser.add_argument("--summary", action="store_true", help="Print paper trade summary")
    parser.add_argument("--resolve", nargs=2, metavar=("TICKER", "OUTCOME"),
                        help="Resolve a paper trade: --resolve TICKER yes|no")
    parser.add_argument("--trade",      nargs=3, metavar=("TICKER", "SIDE", "PRICE"),
                        help="Log a paper trade: --trade TICKER YES 48")
    parser.add_argument("--auto-trade", action="store_true",
                        help="Auto-log paper trades for every new edge alert")
    args = parser.parse_args()

    AUTO_TRADE = args.auto_trade

    init_db()

    if args.summary:
        print_summary()
    elif args.resolve:
        resolve_paper_trade(args.resolve[0], args.resolve[1].lower())
    elif args.trade:
        ticker, side, price = args.trade
        with sqlite3.connect(DB_PATH) as con:
            row = con.execute(
                "SELECT city, blended_prob, edge FROM alerts "
                "WHERE ticker=? ORDER BY ts DESC LIMIT 1",
                (ticker,),
            ).fetchone()
        city  = row[0] if row else "Unknown"
        model = row[1] if row else 50.0
        edge  = row[2] if row else 0.0
        log_paper_trade(ticker, city, side.upper(), float(price), model, edge)
    elif args.watch:
        console.print("[bold green]Starting bot in watch mode (every 15 min)...[/bold green]")
        resolve_all_grids()
        run_scan()
        schedule.every(15).minutes.do(run_scan)
        while True:
            schedule.run_pending()
            time.sleep(60)
    else:
        resolve_all_grids()
        run_scan()
