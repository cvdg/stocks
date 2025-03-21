--
-- Find last ticker price before a date
--
  SELECT MAX(ticker_id) AS ticker_id,
         stock_id,
         MAX(ticker_date) AS ticker_date
    FROM tickers
   WHERE ticker_date <= '2025-03-17'
GROUP BY stock_id
ORDER BY stock_id;

--
-- Portfolio
--
  SELECT stock_id,
         SUM(transaction_share) AS transaction_share,
         SUM(transaction_price) AS transaction_price
    FROM transactions
   WHERE transaction_date <= '2025-03-17'
GROUP BY stock_id
ORDER BY stock_id;

--
--
--
  SELECT stock_id,
         ticker_id,
         ticker_date
    FROM tickers
   WHERE ticker_date <= '2025-03-17'
     AND stock_id = 2
ORDER BY ticker_date DESC
   LIMIT 3;

--
--
--
  SELECT p1.portfolio_date,
         '__Total__'             AS stock_symbol,
         0.0                     AS portfolio_share,
         SUM(p1.portfolio_value) AS portfolio_value,
         SUM(p1.portfolio_price) AS portfolio_price,
         100.0 * SUM(p1.portfolio_value) / SUM(p1.portfolio_price) AS percent
    FROM portfolios p1
GROUP BY p1.portfolio_date
   UNION 
  SELECT p2.portfolio_date,
         (SELECT s.stock_symbol FROM stocks s WHERE s.stock_id = p2.stock_id) AS stock_symbol,
         p2.portfolio_share      AS portfolio_share,
         p2.portfolio_value      AS portfolio_value,
         p2.portfolio_price      AS portfolio_price,
         100.0 * p2.portfolio_value / p2.portfolio_price AS percent
    FROM portfolios p2
ORDER BY portfolio_date, stock_symbol;