-- Enter migration here

CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

DROP TABLE IF EXISTS realtime.symbol_prices CASCADE;
DROP TABLE IF EXISTS realtime.symbols_map CASCADE; 
DROP TABLE IF EXISTS realtime.symbols CASCADE;
DROP TABLE IF EXISTS history.symbol_prices_history CASCADE;

DROP SCHEMA IF EXISTS realtime CASCADE;
DROP SCHEMA IF EXISTS history CASCADE;

CREATE SCHEMA realtime;
CREATE SCHEMA history;

CREATE TABLE realtime.symbols (
    id SERIAL NOT NULL,
    symbol TEXT NOT NULL,
    base_asset TEXT NOT NULL,
    quote_asset TEXT NOT NULL,
    PRIMARY KEY (id),
    UNIQUE(symbol)
);

CREATE TABLE realtime.symbols_map (
    symbol_id SERIAL NOT NULL,
    connector TEXT NOT NULL,
    connector_symbol TEXT NOT NULL,
    PRIMARY KEY (symbol_id, connector),
    CONSTRAINT fk_symbol
        FOREIGN KEY(symbol_id) 
        REFERENCES realtime.symbols(id)
);

CREATE TABLE realtime.symbol_prices (
    connector TEXT NOT NULL,
    symbol_id INT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    price NUMERIC(32, 18),
    update_timestamp TIMESTAMP NOT NULL,
    PRIMARY KEY (connector, symbol_id),
    CONSTRAINT fk_symbol
        FOREIGN KEY(symbol_id) 
        REFERENCES realtime.symbols(id)
);


CREATE TABLE history.symbol_prices_history (
    connector TEXT NOT NULL,
    symbol_id INT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    price NUMERIC(32, 18),
    update_timestamp TIMESTAMP NOT NULL,
    PRIMARY KEY (connector, symbol_id),
    CONSTRAINT fk_symbol
        FOREIGN KEY(symbol_id) 
        REFERENCES realtime.symbols(id)
);