# /// script
# requires-python = ">=3.12"
# dependencies = ["confluent-kafka>=2.5", "httpx>=0.27", "python-dotenv>=1.0"]
# ///
"""Publish OpenAlgo option chain data to Kafka, one message per strike leg.

Run:  uv run kafka/publisher.py

Every REFRESH_SECONDS the nearest-expiry chain (ATM +/- STRIKE_COUNT) is read
from the OpenAlgo REST API and each CE/PE leg is published as its own message,
keyed by the option symbol so one leg always lands in one partition, in order.
Nothing is published while the exchange is closed. Greeks are deliberately not
requested here; they belong on a separate topic.
"""

import json
import logging
import os
import signal
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import httpx
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

log = logging.getLogger("optionchain-kafka")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

IST = timezone(timedelta(hours=5, minutes=30))
API_KEY = os.environ["OPENALGO_API_KEY"]
HOST = os.getenv("HOST_SERVER", "http://127.0.0.1:5000").rstrip("/")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "optionchain.banknifty")
UNDERLYING = os.getenv("UNDERLYING", "BANKNIFTY")
UNDERLYING_EXCHANGE = os.getenv("UNDERLYING_EXCHANGE", "NSE_INDEX")
OPTIONS_EXCHANGE = os.getenv("OPTIONS_EXCHANGE", "NFO")
STRIKE_COUNT = int(os.getenv("STRIKE_COUNT", "10"))
REFRESH = float(os.getenv("REFRESH_SECONDS", "5"))
SKIP_UNCHANGED = os.getenv("SKIP_UNCHANGED", "true").lower() == "true"

LEG_FIELDS = ("ltp", "bid", "ask", "bid_qty", "ask_qty", "volume", "oi", "open", "high", "low", "prev_close")

_stop = False


def _on_signal(*_):
    global _stop
    _stop = True


def post(client: httpx.Client, path: str, body: dict) -> dict:
    r = client.post(f"{HOST}/api/v1/{path}", json={"apikey": API_KEY, **body}, timeout=15)
    data = r.json()
    if r.status_code != 200 or data.get("status") != "success":
        raise RuntimeError(f"{path}: {data.get('message', r.status_code)}")
    return data


def nearest_expiry(client: httpx.Client) -> str:
    """Nearest expiry on or after today, in the DDMMMYY form optionchain wants."""
    data = post(client, "expiry", {"symbol": UNDERLYING, "exchange": OPTIONS_EXCHANGE, "instrumenttype": "options"})
    today = datetime.now(IST).date()
    upcoming = sorted(d for d in (datetime.strptime(e, "%d-%b-%y").date() for e in data["data"]) if d >= today)
    if not upcoming:
        raise RuntimeError("no upcoming expiry found")
    return upcoming[0].strftime("%d%b%y").upper()


def market_window(client: httpx.Client, day: date) -> tuple[float, float] | None:
    """(open, close) as epoch seconds for the options exchange, or None on a holiday."""
    data = post(client, "market/timings", {"date": day.isoformat()})
    for row in data["data"]:
        if row["exchange"] == OPTIONS_EXCHANGE:
            return row["start_time"] / 1000, row["end_time"] / 1000
    return None


def sleep_until(ts: float) -> None:
    while not _stop and time.time() < ts:
        time.sleep(min(30, max(0.0, ts - time.time())))


def wait_for_market(client: httpx.Client) -> None:
    """Block until the exchange is open. Looks ahead over weekends and holidays."""
    while not _stop:
        now = time.time()
        today = datetime.now(IST).date()
        for offset in range(0, 8):
            window = market_window(client, today + timedelta(days=offset))
            if window and window[1] > now:
                if window[0] <= now:
                    return
                log.info("Market closed, resuming at %s", datetime.fromtimestamp(window[0], IST))
                sleep_until(window[0])
                return
        log.warning("No trading day found in the next week, rechecking in 1 hour")
        sleep_until(time.time() + 3600)


def rows_for(chain: dict) -> list[tuple[str, dict]]:
    ts = chain["server_ts"]
    out = []
    for strike in chain["chain"]:
        for side in ("ce", "pe"):
            leg = strike.get(side)
            if not leg:
                continue
            out.append(
                (
                    leg["symbol"],
                    {
                        "ts": ts,
                        "underlying": chain["underlying"],
                        "underlying_ltp": chain["underlying_ltp"],
                        "expiry": chain["expiry_date"],
                        "atm_strike": chain["atm_strike"],
                        "strike": strike["strike"],
                        "type": side.upper(),
                        "label": leg["label"],
                        "symbol": leg["symbol"],
                        "lotsize": leg["lotsize"],
                        "tick_size": leg["tick_size"],
                        **{f: leg[f] for f in LEG_FIELDS},
                    },
                )
            )
    return out


def main() -> None:
    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 20, "compression.type": "lz4"})

    def on_delivery(err, _msg):
        if err:
            log.error("Kafka delivery failed: %s", err)

    last: dict[str, tuple] = {}
    expiry = None
    failures = 0
    with httpx.Client() as client:
        while not _stop:
            try:
                wait_for_market(client)
                if _stop:
                    break
                started = time.time()
                if expiry is None or datetime.strptime(expiry, "%d%b%y").date() < datetime.now(IST).date():
                    expiry = nearest_expiry(client)
                    last.clear()
                    log.info("Publishing %s %s to topic %s", UNDERLYING, expiry, TOPIC)
                chain = post(
                    client,
                    "optionchain",
                    {
                        "underlying": UNDERLYING,
                        "exchange": UNDERLYING_EXCHANGE,
                        "expiry_date": expiry,
                        "strike_count": STRIKE_COUNT,
                    },
                )
                sent = 0
                for symbol, msg in rows_for(chain):
                    fingerprint = tuple(msg[f] for f in LEG_FIELDS)
                    if SKIP_UNCHANGED and last.get(symbol) == fingerprint:
                        continue
                    last[symbol] = fingerprint
                    producer.produce(TOPIC, key=symbol, value=json.dumps(msg), on_delivery=on_delivery)
                    sent += 1
                producer.poll(0)
                failures = 0
                log.info("Published %d legs (ATM %s, spot %s)", sent, chain["atm_strike"], chain["underlying_ltp"])
                sleep_until(started + REFRESH)
            except Exception as exc:
                failures += 1
                delay = min(60, 5 * failures)
                log.error("Cycle failed (%s); retrying in %ds. If this repeats, log in to OpenAlgo again.", exc, delay)
                sleep_until(time.time() + delay)
    producer.flush(10)


if __name__ == "__main__":
    main()
