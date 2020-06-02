drop index cse532.facilityidx;
drop index cse532.zipidx;
drop index cse532.zip;
drop index cse532.attr;

create index cse532.facilityidx on cse532.facility(geolocation) extend using db2gse.spatial_index(0.85, 2, 5);

create index cse532.zipidx on cse532.uszip(shape) extend using db2gse.spatial_index(0.85, 2, 5);

--create index cse532.geo on cse532.uszip(GEOID10);

create index cse532.zip on cse532.facility(ZipCode);

create index cse532.attr on cse532.facilitycertification(AttributeValue);

runstats on table cse532.facility and indexes all;

runstats on table cse532.uszip and indexes all;