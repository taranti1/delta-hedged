"""delta-hedged: deterministic Kalshi BTC threshold market-making system.

Layering (imports only flow downward):

    dh.core        units, normalized events, actions, strategy contract, market specs
    dh.kalshi      Kalshi auth/REST/WS adapters, L2 book, fee engine, metadata
    dh.feeds       external BTC venues (spot, perp, options) -> normalized events
    dh.store       append-only raw capture + deterministic replay reader
    dh.settlement  settlement-benchmark state (BRTI window accumulation)
    dh.models      fair value, delta/gamma, volatility, fill/toxicity models
    dh.execution   order manager, queue tracking, fill simulation, exchange simulator
    dh.strategy    quote policy, inventory/risk engine, hedge engine, the Strategy
    dh.backtest    replay runner, P&L attribution, reports
    dh.live        live runner (paper/live), kill switches, reconciliation, monitoring
    dh.research    experiment scripts (never imported by live code)
"""

__version__ = "0.1.0"
