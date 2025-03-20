  SELECT p1.day                                AS day,
         '__TOTAL__'                           AS symbol,
         0.0                                   AS shares,
         SUM(p1.value)                         AS value,
         SUM(p1.costs)                         AS costs,
         100.0 * SUM(p1.value) / SUM(p1.costs) AS percent 
    FROM portfolio p1 
GROUP BY day
   UNION
  SELECT p2.day                                AS day,
         p2.symbol                             AS symbol,
         p2.shares                             AS shares,
         p2.value                              AS value,
         p2.costs                              AS costs,
         100.0 * p2.value / p2.costs           AS percent
    FROM portfolio p2
ORDER BY day,
         value,
         shares DESC;