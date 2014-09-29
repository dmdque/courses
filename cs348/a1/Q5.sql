SELECT R_NAME, N_NAME
FROM REGION R LEFT OUTER JOIN NATION N
ON R.R_REGIONKEY=N.N_REGIONKEY
ORDER BY R_NAME, N_NAME ASC
;
