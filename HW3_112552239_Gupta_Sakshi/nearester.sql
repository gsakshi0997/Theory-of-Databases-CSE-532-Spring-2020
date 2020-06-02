WITH TAB1 AS (SELECT FacilityID,FacilityName,AttributeValue FROM cse532.facilitycertification),
	 TAB2 AS (SELECT FacilityID, DB2GSE.ST_ASTEXT(Geolocation) AS Hospital_location FROM cse532.facility)
SELECT TAB1.FacilityID,TAB1.FacilityName as Hospital_name,TAB2.Hospital_location,
	 CAST(DB2GSE.ST_DISTANCE(DB2GSE.ST_POINT(TAB2.Hospital_location, 1),
	 DB2GSE.ST_POINT(-72.993983, 40.824369, 1), 'KILOMETER') AS DECIMAL(5,2)) AS Dist_from_hospital
FROM TAB1,TAB2
WHERE TAB1.FacilityID=TAB2.FacilityID AND TAB1.AttributeValue='Emergency Department' AND
 	 DB2GSE.ST_CONTAINS(DB2GSE.ST_Buffer(DB2GSE.ST_Point(-72.993983, 40.824369, 1), 0.25),
  	 DB2GSE.ST_POINT(TAB2.Hospital_location, 1)) = 1 
ORDER BY Dist_from_hospital LIMIT 1;

