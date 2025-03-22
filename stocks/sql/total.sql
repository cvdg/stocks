  SELECT portfolio_date,
         '__Total__' AS stock_symbol,
         SUM(portfolio_value),
         SUM(portfolio_price),
         100.0 * SUM(portfolio_value) / SUM(portfolio_price) AS percent
    FROM portfolios
GROUP BY portfolio_date
ORDER BY portfolio_date DESC
   LIMIT 5;
