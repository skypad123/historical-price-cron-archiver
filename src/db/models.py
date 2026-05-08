"""SQLAlchemy models removed — QuestDB tables are schema-free via ILP.

ohlcv is created explicitly in scripts/init_db.py (WAL + dedup).
ticker and orderbook are auto-created by ILP on first write.
"""
