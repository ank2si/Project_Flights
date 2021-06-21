-- Cancellation reasons by airport 
create or replace view Cancellation_Reason as
SELECT f.ORIGIN_AIRPORT, f.CANCELLATION_REASON, COUNT(*) TOTAL
FROM FLIGHTS f
WHERE f.CANCELLED = 1
GROUP BY f.ORIGIN_AIRPORT, f.CANCELLATION_REASON