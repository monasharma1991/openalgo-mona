# Option chain to Kafka

Publishes BANKNIFTY nearest-expiry option chain (ATM +/- 10 strikes) to Kafka,
one message per CE/PE leg, refreshed every 5 seconds while the market is open.

    docker compose -f kafka/docker-compose.yml up -d
    uv run kafka/publisher.py

Dependencies are declared inside `publisher.py`, so the main project is untouched.

Settings are read from the repo root `.env`. Required: `OPENALGO_API_KEY`.
`HOST_SERVER` is the OpenAlgo address. Optional, with defaults: `KAFKA_BOOTSTRAP`
(localhost:9092), `KAFKA_TOPIC` (optionchain.banknifty), `UNDERLYING` (BANKNIFTY),
`UNDERLYING_EXCHANGE` (NSE_INDEX), `OPTIONS_EXCHANGE` (NFO), `STRIKE_COUNT` (10),
`REFRESH_SECONDS` (5), `SKIP_UNCHANGED` (true).

Message key = option symbol. Value = JSON with ts, underlying, underlying_ltp,
expiry, atm_strike, strike, type, label, symbol, lotsize, tick_size, ltp, bid, ask,
bid_qty, ask_qty, volume, oi, open, high, low, prev_close.

Peek at the topic:

    docker exec openalgo-kafka /opt/kafka/bin/kafka-console-consumer.sh \
      --bootstrap-server localhost:9092 --topic optionchain.banknifty --from-beginning
