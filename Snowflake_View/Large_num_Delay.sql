create or replace view Larg_Num_Delay as 
SELECT COUNT(*) TOTAL_DELAY, f.AIRLINE
FROM FLIGHTS f
WHERE f.ARRIVAL_DELAY > 0
GROUP BY f.AIRLINE
HAVING COUNT(*) =
( 
	SELECT MAX(TOTAL_DELAY) 
	FROM 
	( 
		SELECT COUNT(*) TOTAL_DELAY, f.AIRLINE
		FROM FLIGHTS f
		WHERE f.ARRIVAL_DELAY > 0
		GROUP BY f.AIRLINE
	) b
)