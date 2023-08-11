--! Previous: -
--! Hash: sha1:905011ed0354ae4351e473c742d967e734204691

-- Enter migration here

CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

DROP TABLE IF EXISTS symbols_map CASCADE; 
DROP TABLE IF EXISTS symbols CASCADE;
DROP TABLE IF EXISTS symbol_spreads_stream CASCADE;
DROP TABLE IF EXISTS symbol_trades_stream CASCADE;

CREATE TABLE symbols (
    id SERIAL NOT NULL,
    symbol TEXT NOT NULL,
    base_asset TEXT NOT NULL,
    quote_asset TEXT NOT NULL,
    PRIMARY KEY (id),
    UNIQUE(symbol)
);

CREATE TABLE symbols_map (
    symbol_id SERIAL NOT NULL,
    connector TEXT NOT NULL,
    connector_symbol TEXT NOT NULL,
    is_unavailable BOOL,
    PRIMARY KEY (symbol_id, connector),
    CONSTRAINT fk_symbol
        FOREIGN KEY(symbol_id) 
        REFERENCES symbols(id)
);


CREATE TABLE symbol_spreads_stream (
    id SERIAL NOT NULL,
    connector TEXT NOT NULL,
    symbol_id INT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    best_bid NUMERIC(32, 18),
    best_ask NUMERIC(32, 18),
    update_timestamp TIMESTAMP NOT NULL,
    fetch_timestamp TIMESTAMP NOT NULL,
    PRIMARY KEY (id, timestamp),
    CONSTRAINT fk_symbol
        FOREIGN KEY(symbol_id) 
        REFERENCES symbols(id)
);

SELECT create_hypertable('symbol_spreads_stream', 'timestamp');

ALTER TABLE symbol_spreads_stream SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'timestamp DESC, id',
    timescaledb.compress_segmentby = 'symbol_id'
);
SELECT add_compression_policy('symbol_spreads_stream', INTERVAL '1 hour');


CREATE TABLE symbol_trades_stream (
    id SERIAL NOT NULL,
    connector TEXT NOT NULL,
    symbol_id INT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    price NUMERIC(32, 18) NOT NULL,
    amount NUMERIC(32, 18) NOT NULL,
    is_buy BOOL NOT NULL,
    update_timestamp TIMESTAMP NOT NULL,
    fetch_timestamp TIMESTAMP NOT NULL,
    PRIMARY KEY (id, timestamp),
    CONSTRAINT fk_symbol
        FOREIGN KEY(symbol_id) 
        REFERENCES symbols(id)
);

SELECT create_hypertable('symbol_trades_stream', 'timestamp');

ALTER TABLE symbol_trades_stream SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'timestamp DESC, id',
    timescaledb.compress_segmentby = 'symbol_id'
);
SELECT add_compression_policy('symbol_trades_stream', INTERVAL '1 hour');
