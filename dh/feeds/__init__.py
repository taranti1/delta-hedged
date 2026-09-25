"""dh.feeds: external BTC venues (BRTI spot constituents, perps, options) -> normalized events.

* ``base``      FeedClient (transport: reconnect/backoff/watchdog/markers) and the pure
                ``normalize(raw, recv_ns, state)`` contract with explicit NormalizerState.
* venues        coinbase, kraken, bitstamp, gemini, cryptocom, paxos (experimental),
                deribit, binance_futures, bybit, okx, hyperliquid; stubs: bullish, lmax,
                kalshi_perp.
* ``registry``  stream name -> normalizer; config -> FeedClient.
* ``books``     consumer-side book/state tracker built from events.
* ``composite`` BRTI nowcast inputs (median mid, depth-weighted mid, BRTI replica).

Every venue module starts with an "ASSUMED WIRE FORMAT" block listing the documented message
formats it relies on; each must be verified live with scripts/smoke_feeds.py.
"""
