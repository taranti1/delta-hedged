"""dh.store: append-only raw capture (recorder), deterministic replay, Parquet compaction.

* ``recorder`` Recorder(root).write(stream, recv_ns, raw): zstd JSONL segments per stream per
               UTC hour, crash-tolerant; ClockSampler for the 'clock' stream.
* ``replay``   iter_raw / iter_events: k-way merge by (t, stream rank, q) + normalization.
* ``codec``    dh.core.events <-> JSON (status and events.* streams, Parquet).
* ``parquet``  compact_raw / compact_events per stream per day.
"""
