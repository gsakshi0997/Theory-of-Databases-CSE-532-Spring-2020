WITH NEW_ZIPOP AS(SELECT ZIP, MAX(ZPOP) AS ZPOP FROM CSE532.ZIPPOP GROUP BY ZIP),
SUM_MME AS(SELECT BUYER_ZIP AS ZIP, SUM(MME) AS TOTAL_MME FROM CSE532.DEA_NY GROUP BY BUYER_ZIP),
NORM_MME AS(SELECT SUM_MME.ZIP AS NZIP,
 	CASE
 	WHEN NEW_ZIPOP.ZPOP!=0
 	THEN (SUM_MME.TOTAL_MME/NEW_ZIPOP.ZPOP)
 	ELSE 0
 	END
AS NORMALIZED_MME FROM NEW_ZIPOP INNER JOIN SUM_MME ON NEW_ZIPOP.ZIP=SUM_MME.ZIP),
FINAL_RANK AS(SELECT NZIP ,NORMALIZED_MME, RANK() OVER (ORDER BY (NORMALIZED_MME) DESC) RANK FROM NORM_MME)
SELECT * FROM FINAL_RANK LIMIT 5;