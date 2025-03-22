CREATE TABLE IF NOT EXISTS stocks (
    stock_id          SERIAL          NOT NULL,
    stock_symbol      VARCHAR(64)     NOT NULL,
    stock_description VARCHAR(128),
    stock_identifier  VARCHAR(128),
    stock_active      BOOLEAN         DEFAULT TRUE,
    stock_created     TIMESTAMP       NOT NULL DEFAULT now(),
    stock_updated     TIMESTAMP       NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS stocks_idx01 ON stocks (stock_id); 
CREATE UNIQUE INDEX IF NOT EXISTS stocks_idx02 ON stocks (stock_symbol);



CREATE TABLE IF NOT EXISTS tickers (
    stock_id          INTEGER         NOT NULL,
    ticker_id         SERIAL          NOT NULL,
    ticker_date       DATE            NOT NULL,
    ticker_open       REAL            NOT NULL,
    ticker_high       REAL            NOT NULL,
    ticker_low        REAL            NOT NULL,
    ticker_close      REAL            NOT NULL,
    ticker_volume     REAL            NOT NULL,
    ticker_dividends  REAL            NOT NULL,
    ticker_splits     REAL            NOT NULL,

    FOREIGN KEY (stock_id) REFERENCES stocks (stock_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS tickers_idx01 ON tickers (stock_id, ticker_date); 
CREATE UNIQUE INDEX IF NOT EXISTS tickers_idx02 ON tickers (ticker_id); 



CREATE TABLE IF NOT EXISTS transactions (
    stock_id          INTEGER         NOT NULL,
    transaction_id    SERIAL          NOT NULL,
    transaction_date  DATE            NOT NULL,
    transaction_buy   BOOLEAN         DEFAULT TRUE,
    transaction_share REAL            NOT NULL,
    transaction_price REAL            NOT NULL,

    FOREIGN KEY (stock_id) REFERENCES stocks (stock_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS transactions_idx01 ON transactions (stock_id, transaction_date);
CREATE UNIQUE INDEX IF NOT EXISTS transactions_idx02 ON transactions (transaction_id); 



CREATE TABLE IF NOT EXISTS portfolios (
    stock_id          INTEGER         NOT NULL,
    ticker_id         INTEGER         NOT NULL,
    portfolio_id      SERIAL          NOT NULL,
    portfolio_date    DATE            NOT NULL,
    portfolio_share   REAL            NOT NULL,
    portfolio_price   REAL            NOT NULL,
    portfolio_value   REAL            NOT NULL,

    FOREIGN KEY (stock_id) REFERENCES stocks (stock_id),
    FOREIGN KEY (ticker_id) REFERENCES tickers (ticker_id)
);

--
-- The same ticker_id for multiple days is possible in for example a weekend when a ticker is not updated.
-- CREATE UNIQUE INDEX IF NOT EXISTS portfolios_idx01 ON portfolios (stock_id, ticker_id);
CREATE UNIQUE INDEX IF NOT EXISTS portfolios_idx02 ON portfolios (stock_id, portfolio_date);
CREATE UNIQUE INDEX IF NOT EXISTS portfolios_idx03 ON portfolios (portfolio_id); 
