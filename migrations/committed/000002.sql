--! Previous: sha1:905011ed0354ae4351e473c742d967e734204691
--! Hash: sha1:f94cd2315367cd7e8fe145f382113c5ed94afee3

-- Enter migration here

DROP TABLE IF EXISTS symbol_true_mid_price_stream CASCADE;
DROP TABLE IF EXISTS symbol_spreads_stream CASCADE;
DROP TABLE IF EXISTS symbol_trades_stream CASCADE;

CREATE TABLE symbol_true_mid_price_stream (
    symbol_id INT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    true_mid_price NUMERIC(32, 18) NOT NULL,
    update_timestamp TIMESTAMPTZ NOT NULL,
    CONSTRAINT fk_symbol
        FOREIGN KEY(symbol_id) 
        REFERENCES symbols(id)
);

SELECT create_hypertable('symbol_true_mid_price_stream', 'timestamp');

ALTER TABLE symbol_true_mid_price_stream SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'timestamp DESC',
    timescaledb.compress_segmentby = 'symbol_id'
);
SELECT add_compression_policy('symbol_true_mid_price_stream', INTERVAL '7 day');


CREATE TABLE symbol_spreads_stream (
    connector TEXT NOT NULL,
    symbol_id INT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    best_bid NUMERIC(32, 18),
    best_ask NUMERIC(32, 18),
    update_timestamp TIMESTAMPTZ NOT NULL,
    fetch_timestamp TIMESTAMPTZ NOT NULL,
    CONSTRAINT fk_symbol
        FOREIGN KEY(symbol_id) 
        REFERENCES symbols(id)
);

SELECT create_hypertable('symbol_spreads_stream', 'timestamp');

ALTER TABLE symbol_spreads_stream SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'timestamp DESC',
    timescaledb.compress_segmentby = 'symbol_id'
);
SELECT add_compression_policy('symbol_spreads_stream', INTERVAL '7 day');


CREATE TABLE symbol_trades_stream (
    connector TEXT NOT NULL,
    symbol_id INT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    price NUMERIC(32, 18) NOT NULL,
    amount NUMERIC(32, 18) NOT NULL,
    is_buy BOOL NOT NULL,
    update_timestamp TIMESTAMPTZ NOT NULL,
    fetch_timestamp TIMESTAMPTZ NOT NULL,
    CONSTRAINT fk_symbol
        FOREIGN KEY(symbol_id) 
        REFERENCES symbols(id)
);

SELECT create_hypertable('symbol_trades_stream', 'timestamp');

ALTER TABLE symbol_trades_stream SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'timestamp DESC',
    timescaledb.compress_segmentby = 'symbol_id'
);
SELECT add_compression_policy('symbol_trades_stream', INTERVAL '7 day');
