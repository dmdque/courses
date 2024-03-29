SELECT N_NAME, NUM_ORDERS
FROM
	(SELECT N_NAME, N_NATIONKEY, O_ORDERKEY, C_CUSTKEY, COUNT(*) AS NUM_ORDERS
	FROM ORDERS O, CUSTOMER C, NATION N
	WHERE O_CUSTKEY=C_CUSTKEY and C_NATIONKEY=N_NATIONKEY
	GROUP BY N_NATIONKEY) TEMP
GROUP BY N_NATIONKEY
HAVING TEMP.NUM_ORDERS < 
	(SELECT AVG(NUM_ORDERS)
	FROM
		(SELECT N_NAME, N_NATIONKEY, O_ORDERKEY, C_CUSTKEY, COUNT(*) AS NUM_ORDERS
		FROM ORDERS O, CUSTOMER C, NATION N
		WHERE O_CUSTKEY=C_CUSTKEY and C_NATIONKEY=N_NATIONKEY
		GROUP BY N_NATIONKEY) TEMP)
;
