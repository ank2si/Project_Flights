--Total number of flights by airline and airport on a monthly basis 
-- The question does not specify whether it is by ORIGIN Airport or by DESTINATION Airport.
--This query gives u by ORIGIN Airport. We can change it to DESTINATION Airport if needed.

CREATE OR REPLACE VIEW TOTAL_NUM_FLIGTHS as
SELECT COUNT(*) TOTAL_FLIGHTS, f.AIRLINE, f.ORIGIN_AIRPORT, f.YEAR, f.MONTH
FROM FLIGHTS f
JOIN AIRLINES a ON a.IATA_CODE = f.AIRLINE
JOIN AIRPORTS p ON p.IATA_CODE = f.ORIGIN_AIRPORT
GROUP BY f.AIRLINE, f.ORIGIN_AIRPORT, f.YEAR, f.MONTH
order by f.MONTH asc;