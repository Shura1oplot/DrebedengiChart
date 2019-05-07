-- drebedengi chart
-- Доход
-- Расход

SELECT ABS(SUM(r.[sum] * c.course)) AS [sum]
  FROM records AS r
       JOIN objects AS o
         ON o.[id] = r.object_id
       JOIN currency AS c
         ON c.[id] = r.currency_id
 WHERE o.[type] = CASE ? WHEN 'Доход' THEN 2
                         WHEN 'Расход' THEN 3
                         ELSE NULL
                  END
   AND r.[date] < ?
   AND r.[date] > ?
