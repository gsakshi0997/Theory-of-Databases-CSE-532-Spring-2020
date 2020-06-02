WITH NY_INFO AS (SELECT GEOID10 AS ZIP_NY,SHAPE FROM cse532.uszip,cse532.facility WHERE GEOID10=left(ZipCode,5)),
	 ER_INFO AS (SELECT DISTINCT left(ZipCode,5) AS ZIP FROM cse532.facility INNER JOIN cse532.facilitycertification ON cse532.facility.facilityID= cse532.facilitycertification.facilityID WHERE cse532.facilitycertification.AttributeValue = 'Emergency Department'),
	 TAB AS (SELECT SHAPE,ER_INFO.ZIP FROM cse532.uszip,ER_INFO WHERE GEOID10=ER_INFO.ZIP),
	 NEIGH AS (SELECT ZIP_NY AS ZIP_C FROM NY_INFO,TAB WHERE db2gse.st_intersects(NY_INFO.SHAPE, TAB.SHAPE)=1),
	 RES AS (SELECT ZIP_NY FROM NY_INFO MINUS SELECT ZIP_C FROM NEIGH)

SELECT * FROM RES;
