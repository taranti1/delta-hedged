"""dh.kalshi — Kalshi exchange adapter (Trade API 3.30.0 / WebSocket API 2.0.0).

Modules (import them directly; this package imports nothing so it stays cheap):
    auth        KalshiSigner: RSA-PSS request signing (REST + WS handshake)
    rate_limit  KalshiRateLimiter: read/write token buckets, endpoint costs
    rest        KalshiRest: async REST client (safe retries, UnknownOutcome for writes, raw capture)
    ws          KalshiWS: async WebSocket client (subscriptions, gap resync, watchdog, reconnect)
    sequencer   KalshiWsState / normalize_ws_frame: the WS frame -> event state machine shared
                by live and replay (per-sid seq continuity, synthetic connection records)
    normalize   pure JSON -> dh.core.events mapping, REST records, MarketSpec construction
    orders      V2 order bodies from dh.core.actions; REST results -> order events
    fees        FeeEngine / FeeSchedule / OrderFeeAccumulator / reconcile_fill_fee
    metadata    market discovery, MarketRegistry, rules sanity checks, lifecycle tracking
    config      config/kalshi*.yaml loader
    wire        timestamp / fixed-point helpers
"""
