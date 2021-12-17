INSERT INTO grover_dwh.dim_date
SELECT TO_CHAR(datum, 'yyyymmdd')::INT AS date_dim_id,
    datum AS date_actual,
    TO_CHAR(datum, 'IYYY"M"MM') AS month_name
FROM 
(
    SELECT '2015-01-01'::DATE + SEQUENCE.DAY AS datum
    FROM GENERATE_SERIES(0, 29219) AS SEQUENCE (DAY)
    GROUP BY SEQUENCE.DAY
) DQ
ORDER BY 1;