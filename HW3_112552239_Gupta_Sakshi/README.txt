https://sites.google.com/a/stonybrook.edu/cse532-s20/homework/homework3-2

					CSE 532 HW 3

Q2: I have used Kilometer here as the distance parameter in ST_POINT and type casted the result to a decimal. 

Q3: I get 208 rows in my result set. I used Left() to get the 5 digit GEOIDs from the unzip table to get the NY zips in NY_INFO. Then I used distinct to get the unique 5 digit zip codes who have an emergency department in ER_INFO. Then I fetched the shape info from the zip codes from ER_INFO. And then I checked the neighbors for each zip code in NEIGH. And then finally in RES I subtracted the er_zips from the ny_zips to get the result.

If I didn't use left() in NY_INFO i.e I remove the only 5 digit zip code constraint, then the result set contains 206 rows as it doesn't consider only the 5 digit zip codes in this case.  

Q4: Apart from the given script for indexing I added two more indexes on AttributeValue from cse532.facilitycertification and ZipCode from cse532.facility.
I used the command time db2 -tf abc.sql for getting the timestamp info.

The following are the time stamps before and after indexing:

Q2: Without Indexes
real	0m0.337s
user	0m0.018s
sys	0m0.014s

Q2:With Indexes			
real	0m0.303s
user	0m0.016s
sys	0m0.016s

Q3: Without Indexes
real	0m0.807s
user	0m0.010s
sys	0m0.033s

Q3: With Indexes
real	0m0.798s
user	0m0.016s
sys	0m0.027s

There is little difference in time with and without indexing which I feel is because of a comparatively small dataset.


