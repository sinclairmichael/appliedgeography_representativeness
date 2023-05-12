---- Assessing the socio-demographic representativeness of mobile phone application data (Date: 12/05/2023)
---- Section 1: relates to the method 'Using activity heuristics only to estimate home location'
---- Section 2: relates to the method 'Using activity heuristics and land use to estimate home location'

---- Pre-processing steps for both methods
---- Section 1.1 and 1.2 are the pre-processing steps required for huq/tamoco before standard home detection approach
---- Section 2.1 and 2.1 are the pre-processing steps required for huq/tamoco before advanced home detection approach

---- Applying both home detection methods (IN R)
---- Section 1.3 and 1.4 are reference to the R scripts to performs the standard home detection approach for huq/tamoco
---- Section 2.3 and 2.4 are reference to the R scripts to performs the advanced home detection approach for huq/tamoco

---- Extraction of mobile and population samples for both methods
---- Section 1.5 - 1.9 are the extraction of population comparisons for the standard home detection approach
---- Section 2.5 - 2.9 are the extraction of population comparisons for the advanced home detection approach

---Data sources used
--A. Huq data (2019-2021) [tables: huq_2019; huq_2020; huq_2021]
--B. Tamoco data (2019-2021) [tables: tamoco_2019; tamoco_2020; tamoco_2021]
--C. Scottish Datazone boundaries and adult populations for 2020 [table: datazonesgcr_homelocation_count]
--D. Postcode boundaries for Glasgow City Region with population for 2020 [table: caci_homelocation_count]
--E. Geomni UKBuildings land use data [table: ukbuildingsgcr_4326]
--F. SIMD 2020 data (joined to datazones [C])
--G. CACI Acorn classification data (joined to postcodes [D])


---1. Method: Using activity heuristics only to estimate home location

--1.1 Huq

--1.1.1: Subset evenings only for each year of mobile data (2019-2021)
CREATE TABLE huq_2019_evening AS
SELECT *
FROM public.huq_2019
where CAST(timestamp as time) not between '06:00:00' and '20:00:00';

CREATE TABLE huq_2020_evening AS
SELECT *
FROM public.huq_2020
where CAST(timestamp as time) not between '06:00:00' and '20:00:00';

CREATE TABLE huq_2021_evening AS
SELECT *
FROM public.huq_2021
where CAST(timestamp as time) not between '06:00:00' and '20:00:00';

--1.1.2 Join datazones to evening data for each year (2019-2021)
CREATE TABLE huq_2019_datazone_evening AS
SELECT 
huq.*,
zones.datazone, zones.name
FROM huq_2019_evening huq
JOIN public.datazonesgcr_homelocation_count zones
ON ST_Intersects(huq.geom, zones.geom4326);

CREATE TABLE huq_2020_datazone_evening AS
SELECT 
huq.*,
zones.datazone, zones.name
FROM huq_2020_evening huq
JOIN public.datazonesgcr_homelocation_count zones
ON ST_Intersects(huq.geom, zones.geom4326);

CREATE TABLE huq_2021_datazone_evening AS
SELECT 
huq.*,
zones.datazone, zones.name
FROM huq_2021_evening huq
JOIN public.datazonesgcr_homelocation_count zones
ON ST_Intersects(huq.geom, zones.geom4326);

--1.1.3 Create a spatial index for each year
CREATE INDEX huq_geom_idx2019_datazone_evening
  ON huq_2019_datazone_evening
  USING GIST (geom);
  
CREATE INDEX huq_geom_idx2020_datazone_evening
  ON huq_2020_datazone_evening
  USING GIST (geom);
  
 CREATE INDEX huq_geom_idx2021_datazone_evening
  ON huq_2021_datazone_evening
  USING GIST (geom);

--1.1.4 Create index on user id for each year
CREATE INDEX IDindex_2019 ON huq_2019_datazone_evening (device_iid_hash);
CREATE INDEX IDindex_2020 ON huq_2020_datazone_evening (device_iid_hash);
CREATE INDEX IDindex_2021 ON huq_2021_datazone_evening (device_iid_hash);

--1.1.5 Create index on date (all year)
CREATE INDEX date_idx_huq_2019 ON huq_2019_datazone_evening ((timestamp::DATE));
CREATE INDEX date_idx_huq_2020 ON huq_2020_datazone_evening ((timestamp::DATE));
CREATE INDEX date_idx_huq_2021 ON huq_2021_datazone_evening ((timestamp::DATE));

--1.2 Tamoco

--1.2.1 Subset evenings only for each year of mobile data (2019-2021)
CREATE TABLE tamoco_2019_evening AS
SELECT *
FROM public.tamoco_2019
where CAST(sdk_ts as time) not between '06:00:00' and '20:00:00';

CREATE TABLE tamoco_2020_evening AS
SELECT *
FROM public.tamoco_2020
where CAST(sdk_ts as time) not between '06:00:00' and '20:00:00';

CREATE TABLE tamoco_2021_evening AS
SELECT *
FROM public.tamoco_2021
where CAST(sdk_ts as time) not between '06:00:00' and '20:00:00';

--1.2.2 Join datazones to evening data for each year (2019-2021)
CREATE TABLE tamoco_2019_datazone_evening AS
SELECT 
tamoco.*,
zones.datazone, zones.name as dz_name
FROM tamoco_2019_evening tamoco
JOIN public.datazonesgcr_homelocation_count zones
ON ST_Intersects(tamoco.geom, zones.geom4326);

CREATE TABLE tamoco_2020_datazone_evening AS
SELECT 
tamoco.*,
zones.datazone, zones.name as dz_name
FROM tamoco_2020_evening tamoco
JOIN public.datazonesgcr_homelocation_count zones
ON ST_Intersects(tamoco.geom, zones.geom4326);

CREATE TABLE tamoco_2021_datazone_evening AS
SELECT 
tamoco.*,
zones.datazone, zones.name as dz_name
FROM tamoco_2021_evening tamoco
JOIN public.datazonesgcr_homelocation_count zones
ON ST_Intersects(tamoco.geom, zones.geom4326);

--1.2.3 Create index on date (all year)
CREATE INDEX date_idx_2019_std ON tamoco_2019_datazone_evening ((sdk_ts::DATE));

CREATE INDEX date_idx_2020_std ON tamoco_2020_datazone_evening ((sdk_ts::DATE));

CREATE INDEX date_idx_2021_std ON tamoco_2021_datazone_evening ((sdk_ts::DATE));

--1.2.4 Subset each month into a table (Tamoco user ID is rehashed monthly so home locations need to be estimated monthly) 
CREATE TABLE tamoco_2019_jan_std AS
SELECT * FROM public.tamoco_2019_datazone_evening WHERE CAST(sdk_ts AS date) between '2019-01-01' and '2019-01-31';
CREATE TABLE tamoco_2019_feb_std AS
SELECT * FROM public.tamoco_2019_datazone_evening WHERE CAST(sdk_ts AS date) between '2019-02-01' and '2019-02-28';
CREATE TABLE tamoco_2019_mar_std AS
SELECT * FROM public.tamoco_2019_datazone_evening WHERE CAST(sdk_ts AS date) between '2019-03-01' and '2019-03-31';
CREATE TABLE tamoco_2019_apr_std AS
SELECT * FROM public.tamoco_2019_datazone_evening WHERE CAST(sdk_ts AS date) between '2019-04-01' and '2019-04-30';
CREATE TABLE tamoco_2019_may_std AS
SELECT * FROM public.tamoco_2019_datazone_evening WHERE CAST(sdk_ts AS date) between '2019-05-01' and '2019-05-31';
CREATE TABLE tamoco_2019_jun_std AS
SELECT * FROM public.tamoco_2019_datazone_evening WHERE CAST(sdk_ts AS date) between '2019-06-01' and '2019-06-30';
CREATE TABLE tamoco_2019_jul_std AS
SELECT * FROM public.tamoco_2019_datazone_evening WHERE CAST(sdk_ts AS date) between '2019-07-01' and '2019-07-31';
CREATE TABLE tamoco_2019_aug_std AS
SELECT * FROM public.tamoco_2019_datazone_evening WHERE CAST(sdk_ts AS date) between '2019-08-01' and '2019-08-31';
CREATE TABLE tamoco_2019_sep_std AS
SELECT * FROM public.tamoco_2019_datazone_evening WHERE CAST(sdk_ts AS date) between '2019-09-01' and '2019-09-30';
CREATE TABLE tamoco_2019_oct_std AS
SELECT * FROM public.tamoco_2019_datazone_evening WHERE CAST(sdk_ts AS date) between '2019-10-01' and '2019-10-31';
CREATE TABLE tamoco_2019_nov_std AS
SELECT * FROM public.tamoco_2019_datazone_evening WHERE CAST(sdk_ts AS date) between '2019-11-01' and '2019-11-30';
CREATE TABLE tamoco_2019_dec_std AS
SELECT * FROM public.tamoco_2019_datazone_evening WHERE CAST(sdk_ts AS date) between '2019-12-01' and '2019-12-31';

CREATE TABLE tamoco_2020_jan_std AS
SELECT * FROM public.tamoco_2020_datazone_evening WHERE CAST(sdk_ts AS date) between '2020-01-01' and '2020-01-31';
CREATE TABLE tamoco_2020_feb_std AS
SELECT * FROM public.tamoco_2020_datazone_evening WHERE CAST(sdk_ts AS date) between '2020-02-01' and '2020-02-28';
CREATE TABLE tamoco_2020_mar_std AS
SELECT * FROM public.tamoco_2020_datazone_evening WHERE CAST(sdk_ts AS date) between '2020-03-01' and '2020-03-31';
CREATE TABLE tamoco_2020_apr_std AS
SELECT * FROM public.tamoco_2020_datazone_evening WHERE CAST(sdk_ts AS date) between '2020-04-01' and '2020-04-30';
CREATE TABLE tamoco_2020_may_std AS
SELECT * FROM public.tamoco_2020_datazone_evening WHERE CAST(sdk_ts AS date) between '2020-05-01' and '2020-05-31';
CREATE TABLE tamoco_2020_jun_std AS
SELECT * FROM public.tamoco_2020_datazone_evening WHERE CAST(sdk_ts AS date) between '2020-06-01' and '2020-06-30';
CREATE TABLE tamoco_2020_jul_std AS
SELECT * FROM public.tamoco_2020_datazone_evening WHERE CAST(sdk_ts AS date) between '2020-07-01' and '2020-07-31';
CREATE TABLE tamoco_2020_aug_std AS
SELECT * FROM public.tamoco_2020_datazone_evening WHERE CAST(sdk_ts AS date) between '2020-08-01' and '2020-08-31';
CREATE TABLE tamoco_2020_sep_std AS
SELECT * FROM public.tamoco_2020_datazone_evening WHERE CAST(sdk_ts AS date) between '2020-09-01' and '2020-09-30';
CREATE TABLE tamoco_2020_oct_std AS
SELECT * FROM public.tamoco_2020_datazone_evening WHERE CAST(sdk_ts AS date) between '2020-10-01' and '2020-10-31';
CREATE TABLE tamoco_2020_nov_std AS
SELECT * FROM public.tamoco_2020_datazone_evening WHERE CAST(sdk_ts AS date) between '2020-11-01' and '2020-11-30';
CREATE TABLE tamoco_2020_dec_std AS
SELECT * FROM public.tamoco_2020_datazone_evening WHERE CAST(sdk_ts AS date) between '2020-12-01' and '2020-12-31';

CREATE TABLE tamoco_2021_jan_std AS
SELECT * FROM public.tamoco_2021_datazone_evening WHERE CAST(sdk_ts AS date) between '2021-01-01' and '2021-01-31';
CREATE TABLE tamoco_2021_feb_std AS
SELECT * FROM public.tamoco_2021_datazone_evening WHERE CAST(sdk_ts AS date) between '2021-02-01' and '2021-02-28';
CREATE TABLE tamoco_2021_mar_std AS
SELECT * FROM public.tamoco_2021_datazone_evening WHERE CAST(sdk_ts AS date) between '2021-03-01' and '2021-03-31';
CREATE TABLE tamoco_2021_apr_std AS
SELECT * FROM public.tamoco_2021_datazone_evening WHERE CAST(sdk_ts AS date) between '2021-04-01' and '2021-04-30';
CREATE TABLE tamoco_2021_may_std AS
SELECT * FROM public.tamoco_2021_datazone_evening WHERE CAST(sdk_ts AS date) between '2021-05-01' and '2021-05-31';
CREATE TABLE tamoco_2021_jun_std AS
SELECT * FROM public.tamoco_2021_datazone_evening WHERE CAST(sdk_ts AS date) between '2021-06-01' and '2021-06-30';
CREATE TABLE tamoco_2021_jul_std AS
SELECT * FROM public.tamoco_2021_datazone_evening WHERE CAST(sdk_ts AS date) between '2021-07-01' and '2021-07-31';
CREATE TABLE tamoco_2021_aug_std AS
SELECT * FROM public.tamoco_2021_datazone_evening WHERE CAST(sdk_ts AS date) between '2021-08-01' and '2021-08-31';
CREATE TABLE tamoco_2021_sep_std AS
SELECT * FROM public.tamoco_2021_datazone_evening WHERE CAST(sdk_ts AS date) between '2021-09-01' and '2021-09-30';
CREATE TABLE tamoco_2021_oct_std AS
SELECT * FROM public.tamoco_2021_datazone_evening WHERE CAST(sdk_ts AS date) between '2021-10-01' and '2021-10-31';
CREATE TABLE tamoco_2021_nov_std AS
SELECT * FROM public.tamoco_2021_datazone_evening WHERE CAST(sdk_ts AS date) between '2021-11-01' and '2021-11-30';
CREATE TABLE tamoco_2021_dec_std AS
SELECT * FROM public.tamoco_2021_datazone_evening WHERE CAST(sdk_ts AS date) between '2021-12-01' and '2021-12-31';

--1.2.5 Create index on date
CREATE INDEX date_idx_2019_jan_std ON tamoco_2019_jan_std ((sdk_ts::DATE));
CREATE INDEX date_idx_2019_feb_std ON tamoco_2019_feb_std ((sdk_ts::DATE));
CREATE INDEX date_idx_2019_mar_std ON tamoco_2019_mar_std ((sdk_ts::DATE));
CREATE INDEX date_idx_2019_apr_std ON tamoco_2019_apr_std ((sdk_ts::DATE));
CREATE INDEX date_idx_2019_may_std ON tamoco_2019_may_std ((sdk_ts::DATE));
CREATE INDEX date_idx_2019_jun_std ON tamoco_2019_jun_std ((sdk_ts::DATE));
CREATE INDEX date_idx_2019_jul_std ON tamoco_2019_jul_std ((sdk_ts::DATE));
CREATE INDEX date_idx_2019_aug_std ON tamoco_2019_aug_std ((sdk_ts::DATE));
CREATE INDEX date_idx_2019_sep_std ON tamoco_2019_sep_std ((sdk_ts::DATE));
CREATE INDEX date_idx_2019_oct_std ON tamoco_2019_oct_std ((sdk_ts::DATE));
CREATE INDEX date_idx_2019_nov_std ON tamoco_2019_nov_std ((sdk_ts::DATE));
CREATE INDEX date_idx_2019_dec_std ON tamoco_2019_dec_std ((sdk_ts::DATE));

CREATE INDEX date_idx_2020_jan_std ON tamoco_2020_jan_std ((sdk_ts::DATE));
CREATE INDEX date_idx_2020_feb_std ON tamoco_2020_feb_std ((sdk_ts::DATE));
CREATE INDEX date_idx_2020_mar_std ON tamoco_2020_mar_std ((sdk_ts::DATE));
CREATE INDEX date_idx_2020_apr_std ON tamoco_2020_apr_std ((sdk_ts::DATE));
CREATE INDEX date_idx_2020_may_std ON tamoco_2020_may_std ((sdk_ts::DATE));
CREATE INDEX date_idx_2020_jun_std ON tamoco_2020_jun_std ((sdk_ts::DATE));
CREATE INDEX date_idx_2020_jul_std ON tamoco_2020_jul_std ((sdk_ts::DATE));
CREATE INDEX date_idx_2020_aug_std ON tamoco_2020_aug_std ((sdk_ts::DATE));
CREATE INDEX date_idx_2020_sep_std ON tamoco_2020_sep_std ((sdk_ts::DATE));
CREATE INDEX date_idx_2020_oct_std ON tamoco_2020_oct_std ((sdk_ts::DATE));
CREATE INDEX date_idx_2020_nov_std ON tamoco_2020_nov_std ((sdk_ts::DATE));
CREATE INDEX date_idx_2020_dec_std ON tamoco_2020_dec_std ((sdk_ts::DATE));

CREATE INDEX date_idx_2021_jan_std ON tamoco_2021_jan_std ((sdk_ts::DATE));
CREATE INDEX date_idx_2021_feb_std ON tamoco_2021_feb_std ((sdk_ts::DATE));
CREATE INDEX date_idx_2021_mar_std ON tamoco_2021_mar_std ((sdk_ts::DATE));
CREATE INDEX date_idx_2021_apr_std ON tamoco_2021_apr_std ((sdk_ts::DATE));
CREATE INDEX date_idx_2021_may_std ON tamoco_2021_may_std ((sdk_ts::DATE));
CREATE INDEX date_idx_2021_jun_std ON tamoco_2021_jun_std ((sdk_ts::DATE));
CREATE INDEX date_idx_2021_jul_std ON tamoco_2021_jul_std ((sdk_ts::DATE));
CREATE INDEX date_idx_2021_aug_std ON tamoco_2021_aug_std ((sdk_ts::DATE));
CREATE INDEX date_idx_2021_sep_std ON tamoco_2021_sep_std ((sdk_ts::DATE));
CREATE INDEX date_idx_2021_oct_std ON tamoco_2021_oct_std ((sdk_ts::DATE));
CREATE INDEX date_idx_2021_nov_std ON tamoco_2021_nov_std ((sdk_ts::DATE));
CREATE INDEX date_idx_2021_dec_std ON tamoco_2021_dec_std ((sdk_ts::DATE));

--1.2.6 Create index on user id 
CREATE INDEX ID_index_2019_std_jan ON tamoco_2019_jan_std (hashed_user_id);
CREATE INDEX ID_index_2019_std_feb ON tamoco_2019_feb_std (hashed_user_id);
CREATE INDEX ID_index_2019_std_mar ON tamoco_2019_mar_std (hashed_user_id);
CREATE INDEX ID_index_2019_std_apr ON tamoco_2019_apr_std (hashed_user_id);
CREATE INDEX ID_index_2019_std_may ON tamoco_2019_may_std (hashed_user_id);
CREATE INDEX ID_index_2019_std_jun ON tamoco_2019_jun_std (hashed_user_id);
CREATE INDEX ID_index_2019_std_jul ON tamoco_2019_jul_std (hashed_user_id);
CREATE INDEX ID_index_2019_std_aug ON tamoco_2019_aug_std (hashed_user_id);
CREATE INDEX ID_index_2019_std_sep ON tamoco_2019_sep_std (hashed_user_id);
CREATE INDEX ID_index_2019_std_oct ON tamoco_2019_oct_std (hashed_user_id);
CREATE INDEX ID_index_2019_std_nov ON tamoco_2019_nov_std (hashed_user_id);
CREATE INDEX ID_index_2019_std_dec ON tamoco_2019_dec_std (hashed_user_id);
 
CREATE INDEX ID_index_2020_std_jan ON tamoco_2020_jan_std (hashed_user_id);
CREATE INDEX ID_index_2020_std_feb ON tamoco_2020_feb_std (hashed_user_id);
CREATE INDEX ID_index_2020_std_mar ON tamoco_2020_mar_std (hashed_user_id);
CREATE INDEX ID_index_2020_std_apr ON tamoco_2020_apr_std (hashed_user_id);
CREATE INDEX ID_index_2020_std_may ON tamoco_2020_may_std (hashed_user_id);
CREATE INDEX ID_index_2020_std_jun ON tamoco_2020_jun_std (hashed_user_id);
CREATE INDEX ID_index_2020_std_jul ON tamoco_2020_jul_std (hashed_user_id);
CREATE INDEX ID_index_2020_std_aug ON tamoco_2020_aug_std (hashed_user_id);
CREATE INDEX ID_index_2020_std_sep ON tamoco_2020_sep_std (hashed_user_id);
CREATE INDEX ID_index_2020_std_oct ON tamoco_2020_oct_std (hashed_user_id);
CREATE INDEX ID_index_2020_std_nov ON tamoco_2020_nov_std (hashed_user_id);
CREATE INDEX ID_index_2020_std_dec ON tamoco_2020_dec_std (hashed_user_id);

CREATE INDEX ID_index_2021_std_jan ON tamoco_2021_jan_std (hashed_user_id);
CREATE INDEX ID_index_2021_std_feb ON tamoco_2021_feb_std (hashed_user_id);
CREATE INDEX ID_index_2021_std_mar ON tamoco_2021_mar_std (hashed_user_id);
CREATE INDEX ID_index_2021_std_apr ON tamoco_2021_apr_std (hashed_user_id);
CREATE INDEX ID_index_2021_std_may ON tamoco_2021_may_std (hashed_user_id);
CREATE INDEX ID_index_2021_std_jun ON tamoco_2021_jun_std (hashed_user_id);
CREATE INDEX ID_index_2021_std_jul ON tamoco_2021_jul_std (hashed_user_id);
CREATE INDEX ID_index_2021_std_aug ON tamoco_2021_aug_std (hashed_user_id);
CREATE INDEX ID_index_2021_std_sep ON tamoco_2021_sep_std (hashed_user_id);
CREATE INDEX ID_index_2021_std_oct ON tamoco_2021_oct_std (hashed_user_id);
CREATE INDEX ID_index_2021_std_nov ON tamoco_2021_nov_std (hashed_user_id);
CREATE INDEX ID_index_2021_std_dec ON tamoco_2021_dec_std (hashed_user_id);

--1.3 In R perform home location analysis at a user level (2019-2021)
--[file name Home location estimates_standard.Rmd]

--1.4 In R save home location results back into tables in database for post processing
--[file name Home location estimates_standard.Rmd]

--1.5 Post processing

--1.5.1 convert home locations into spatial point object using lat/lon
--[table: std_homelocations_huq/tamoco_YEAR contains all home locations which could be determined by method]
--[table: std_homelocations_huq/tamoco_YEAR_subset subsets the prior table to only users with 2 or more active evenings]

--1.5.1.1 Huq 

ALTER TABLE std_homelocations_huq_2019 ADD COLUMN geom geometry(Point, 4326);
UPDATE std_homelocations_huq_2019 SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);
ALTER TABLE std_homelocations_huq_2019_subset ADD COLUMN geom geometry(Point, 4326);
UPDATE std_homelocations_huq_2019_subset SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE std_homelocations_huq_2020 ADD COLUMN geom geometry(Point, 4326);
UPDATE std_homelocations_huq_2020 SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);
ALTER TABLE std_homelocations_huq_2020_subset ADD COLUMN geom geometry(Point, 4326);
UPDATE std_homelocations_huq_2020_subset SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE std_homelocations_huq_2021 ADD COLUMN geom geometry(Point, 4326);
UPDATE std_homelocations_huq_2021 SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);
ALTER TABLE std_homelocations_huq_2021_subset ADD COLUMN geom geometry(Point, 4326);
UPDATE std_homelocations_huq_2021_subset SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

--1.5.1.2 Tamoco 
ALTER TABLE "std_homelocations_tamoco_2019_Jan" ADD COLUMN geom geometry(Point, 4326);
UPDATE "std_homelocations_tamoco_2019_Jan" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "std_homelocations_tamoco_2019_Feb" ADD COLUMN geom geometry(Point, 4326);
UPDATE "std_homelocations_tamoco_2019_Feb" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "std_homelocations_tamoco_2019_Mar" ADD COLUMN geom geometry(Point, 4326);
UPDATE "std_homelocations_tamoco_2019_Mar" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "std_homelocations_tamoco_2019_Apr" ADD COLUMN geom geometry(Point, 4326);
UPDATE "std_homelocations_tamoco_2019_Apr" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "std_homelocations_tamoco_2019_May" ADD COLUMN geom geometry(Point, 4326);
UPDATE "std_homelocations_tamoco_2019_May" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "std_homelocations_tamoco_2019_Jun" ADD COLUMN geom geometry(Point, 4326);
UPDATE "std_homelocations_tamoco_2019_Jun" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "std_homelocations_tamoco_2019_Jul" ADD COLUMN geom geometry(Point, 4326);
UPDATE "std_homelocations_tamoco_2019_Jul" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "std_homelocations_tamoco_2019_Aug" ADD COLUMN geom geometry(Point, 4326);
UPDATE "std_homelocations_tamoco_2019_Aug" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "std_homelocations_tamoco_2019_Sep" ADD COLUMN geom geometry(Point, 4326);
UPDATE "std_homelocations_tamoco_2019_Sep" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "std_homelocations_tamoco_2019_Oct" ADD COLUMN geom geometry(Point, 4326);
UPDATE "std_homelocations_tamoco_2019_Oct" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "std_homelocations_tamoco_2019_Nov" ADD COLUMN geom geometry(Point, 4326);
UPDATE "std_homelocations_tamoco_2019_Nov" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "std_homelocations_tamoco_2019_Dec" ADD COLUMN geom geometry(Point, 4326);
UPDATE "std_homelocations_tamoco_2019_Dec" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);


ALTER TABLE "std_homelocations_tamoco_2020_Jan" ADD COLUMN geom geometry(Point, 4326);
UPDATE "std_homelocations_tamoco_2020_Jan" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "std_homelocations_tamoco_2020_Feb" ADD COLUMN geom geometry(Point, 4326);
UPDATE "std_homelocations_tamoco_2020_Feb" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "std_homelocations_tamoco_2020_Mar" ADD COLUMN geom geometry(Point, 4326);
UPDATE "std_homelocations_tamoco_2020_Mar" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "std_homelocations_tamoco_2020_Apr" ADD COLUMN geom geometry(Point, 4326);
UPDATE "std_homelocations_tamoco_2020_Apr" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "std_homelocations_tamoco_2020_May" ADD COLUMN geom geometry(Point, 4326);
UPDATE "std_homelocations_tamoco_2020_May" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "std_homelocations_tamoco_2020_Jun" ADD COLUMN geom geometry(Point, 4326);
UPDATE "std_homelocations_tamoco_2020_Jun" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "std_homelocations_tamoco_2020_Jul" ADD COLUMN geom geometry(Point, 4326);
UPDATE "std_homelocations_tamoco_2020_Jul" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "std_homelocations_tamoco_2020_Aug" ADD COLUMN geom geometry(Point, 4326);
UPDATE "std_homelocations_tamoco_2020_Aug" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "std_homelocations_tamoco_2020_Sep" ADD COLUMN geom geometry(Point, 4326);
UPDATE "std_homelocations_tamoco_2020_Sep" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "std_homelocations_tamoco_2020_Oct" ADD COLUMN geom geometry(Point, 4326);
UPDATE "std_homelocations_tamoco_2020_Oct" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "std_homelocations_tamoco_2020_Nov" ADD COLUMN geom geometry(Point, 4326);
UPDATE "std_homelocations_tamoco_2020_Nov" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "std_homelocations_tamoco_2020_Dec" ADD COLUMN geom geometry(Point, 4326);
UPDATE "std_homelocations_tamoco_2020_Dec" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);


ALTER TABLE "std_homelocations_tamoco_2021_Jan" ADD COLUMN geom geometry(Point, 4326);
UPDATE "std_homelocations_tamoco_2021_Jan" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "std_homelocations_tamoco_2021_Feb" ADD COLUMN geom geometry(Point, 4326);
UPDATE "std_homelocations_tamoco_2021_Feb" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "std_homelocations_tamoco_2021_Mar" ADD COLUMN geom geometry(Point, 4326);
UPDATE "std_homelocations_tamoco_2021_Mar" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "std_homelocations_tamoco_2021_Apr" ADD COLUMN geom geometry(Point, 4326);
UPDATE "std_homelocations_tamoco_2021_Apr" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "std_homelocations_tamoco_2021_May" ADD COLUMN geom geometry(Point, 4326);
UPDATE "std_homelocations_tamoco_2021_May" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "std_homelocations_tamoco_2021_Jun" ADD COLUMN geom geometry(Point, 4326);
UPDATE "std_homelocations_tamoco_2021_Jun" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "std_homelocations_tamoco_2021_Jul" ADD COLUMN geom geometry(Point, 4326);
UPDATE "std_homelocations_tamoco_2021_Jul" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "std_homelocations_tamoco_2021_Aug" ADD COLUMN geom geometry(Point, 4326);
UPDATE "std_homelocations_tamoco_2021_Aug" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "std_homelocations_tamoco_2021_Sep" ADD COLUMN geom geometry(Point, 4326);
UPDATE "std_homelocations_tamoco_2021_Sep" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "std_homelocations_tamoco_2021_Oct" ADD COLUMN geom geometry(Point, 4326);
UPDATE "std_homelocations_tamoco_2021_Oct" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "std_homelocations_tamoco_2021_Nov" ADD COLUMN geom geometry(Point, 4326);
UPDATE "std_homelocations_tamoco_2021_Nov" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "std_homelocations_tamoco_2021_Dec" ADD COLUMN geom geometry(Point, 4326);
UPDATE "std_homelocations_tamoco_2021_Dec" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

--1.6 Count the volume of data generated by users 
--1.6.1 With home locations 
--1.6.1.1 Huq
SELECT COUNT(*) 
FROM huq_2019  h
WHERE EXISTS (SELECT 1 FROM std_homelocations_huq_2019 p WHERE p."Device_iid_hash" = h.device_iid_hash);

SELECT COUNT(*) 
FROM huq_2020 h
WHERE EXISTS (SELECT 1 FROM std_homelocations_huq_2020 p WHERE p."Device_iid_hash" = h.device_iid_hash);

SELECT COUNT(*) 
FROM huq_2021  h
WHERE EXISTS (SELECT 1 FROM std_homelocations_huq_2021 p WHERE p."Device_iid_hash" = h.device_iid_hash);

--1.6.1.2 Tamoco
SELECT COUNT(*) 
FROM tamoco_2019  h
WHERE EXISTS (SELECT 1 FROM std_homelocations_tamoco_2019 p WHERE p."Device_iid_hash" = h.hashed_user_id);

SELECT COUNT(*) 
FROM tamoco_2021  h
WHERE EXISTS (SELECT 1 FROM std_homelocations_tamoco_2020 p WHERE p."Device_iid_hash" = h.hashed_user_id);

SELECT COUNT(*) 
FROM tamoco_2019  h
WHERE EXISTS (SELECT 1 FROM std_homelocations_tamoco_2021 p WHERE p."Device_iid_hash" = h.hashed_user_id);

--1.6.2 With home locations subset to 2+ active evenings
--1.6.2.1 Huq
SELECT COUNT(*) 
FROM huq_2019  h
WHERE EXISTS (SELECT 1 FROM std_homelocations_huq_2019_subset p WHERE p."Device_iid_hash" = h.device_iid_hash);

SELECT COUNT(*) 
FROM huq_2020 h
WHERE EXISTS (SELECT 1 FROM std_homelocations_huq_2020_subset p WHERE p."Device_iid_hash" = h.device_iid_hash);

SELECT COUNT(*) 
FROM huq_2021  h
WHERE EXISTS (SELECT 1 FROM std_homelocations_huq_2021_subset p WHERE p."Device_iid_hash" = h.device_iid_hash);

--1.6.2.2 Tamoco
SELECT COUNT(*) 
FROM tamoco_2019  h
WHERE EXISTS (SELECT 1 FROM std_homelocations_tamoco_2019_subset p WHERE p."Device_iid_hash" = h.hashed_user_id);

SELECT COUNT(*) 
FROM tamoco_2020  h
WHERE EXISTS (SELECT 1 FROM std_homelocations_tamoco_2020_subset p WHERE p."Device_iid_hash" = h.hashed_user_id);

SELECT COUNT(*) 
FROM tamoco_2020  h
WHERE EXISTS (SELECT 1 FROM std_homelocations_tamoco_2020_subset p WHERE p."Device_iid_hash" = h.hashed_user_id);


--1.7 Counting the number of huq/tamoco users in each Datazone and each Postcode
--1.7.1 Datazone (SIMD)
--1.7.1.1 Huq
create table datazonesgcr_homelocation_count_std as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public.std_homelocations_huq_2019_subset h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS huq2019std
FROM public.datazonesgcr_homelocation_count d;

create table datazonesgcr_homelocation_count_std_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public.std_homelocations_huq_2020_subset h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS huq2020std
FROM public.datazonesgcr_homelocation_count_std d;

DROP TABLE datazonesgcr_homelocation_count_std; 
ALTER TABLE datazonesgcr_homelocation_count_std_1
RENAME TO datazonesgcr_homelocation_count_std;

create table datazonesgcr_homelocation_count_std_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public.std_homelocations_huq_2021_subset h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS huq2021std
FROM public.datazonesgcr_homelocation_count_std d;

DROP TABLE datazonesgcr_homelocation_count_std; 
ALTER TABLE datazonesgcr_homelocation_count_std_1
RENAME TO datazonesgcr_homelocation_count_std;

--1.7.2.2 Tamoco (monthly because re-hashed monthly)
create table datazonesgcr_homelocation_count_std_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2019_Jan" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2019jan_std
FROM public.datazonesgcr_homelocation_count_std d;

DROP TABLE datazonesgcr_homelocation_count_std; 
ALTER TABLE datazonesgcr_homelocation_count_std_1
RENAME TO datazonesgcr_homelocation_count_std;

create table datazonesgcr_homelocation_count_std_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2019_Feb" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2019feb_std
FROM public.datazonesgcr_homelocation_count_std d;

DROP TABLE datazonesgcr_homelocation_count_std; 
ALTER TABLE datazonesgcr_homelocation_count_std_1
RENAME TO datazonesgcr_homelocation_count_std;

create table datazonesgcr_homelocation_count_std_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2019_Mar" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2019mar_std
FROM public.datazonesgcr_homelocation_count_std d;

DROP TABLE datazonesgcr_homelocation_count_std; 
ALTER TABLE datazonesgcr_homelocation_count_std_1
RENAME TO datazonesgcr_homelocation_count_std;

create table datazonesgcr_homelocation_count_std_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2019_Apr" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2019apr_std
FROM public.datazonesgcr_homelocation_count_std d;

DROP TABLE datazonesgcr_homelocation_count_std; 
ALTER TABLE datazonesgcr_homelocation_count_std_1
RENAME TO datazonesgcr_homelocation_count_std;

create table datazonesgcr_homelocation_count_std_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2019_May" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2019may_std
FROM public.datazonesgcr_homelocation_count_std_6 d;

DROP TABLE datazonesgcr_homelocation_count_std; 
ALTER TABLE datazonesgcr_homelocation_count_std_1
RENAME TO datazonesgcr_homelocation_count_std;

create table datazonesgcr_homelocation_count_std_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2019_Jun" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2019jun_std
FROM public.datazonesgcr_homelocation_count_std d;

DROP TABLE datazonesgcr_homelocation_count_std; 
ALTER TABLE datazonesgcr_homelocation_count_std_1
RENAME TO datazonesgcr_homelocation_count_std;

create table datazonesgcr_homelocation_count_std_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2019_Jul" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2019jul_std
FROM public.datazonesgcr_homelocation_count_std d;

DROP TABLE datazonesgcr_homelocation_count_std; 
ALTER TABLE datazonesgcr_homelocation_count_std_1
RENAME TO datazonesgcr_homelocation_count_std;

create table datazonesgcr_homelocation_count_std_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2019_Aug" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2019aug_std
FROM public.datazonesgcr_homelocation_count_std d;

DROP TABLE datazonesgcr_homelocation_count_std; 
ALTER TABLE datazonesgcr_homelocation_count_std_1
RENAME TO datazonesgcr_homelocation_count_std;

create table datazonesgcr_homelocation_count_std_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2019_Sep" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2019sep_std
FROM public.datazonesgcr_homelocation_count_std d;

DROP TABLE datazonesgcr_homelocation_count_std; 
ALTER TABLE datazonesgcr_homelocation_count_std_1
RENAME TO datazonesgcr_homelocation_count_std;

create table datazonesgcr_homelocation_count_std_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2019_Oct" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2019oct_std
FROM public.datazonesgcr_homelocation_count_std d;

DROP TABLE datazonesgcr_homelocation_count_std; 
ALTER TABLE datazonesgcr_homelocation_count_std_1
RENAME TO datazonesgcr_homelocation_count_std;

create table datazonesgcr_homelocation_count_std_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2019_Nov" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2019nov_std
FROM public.datazonesgcr_homelocation_count_std d;

DROP TABLE datazonesgcr_homelocation_count_std; 
ALTER TABLE datazonesgcr_homelocation_count_std_1
RENAME TO datazonesgcr_homelocation_count_std;

create table datazonesgcr_homelocation_count_std_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2019_Dec" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2019dec_std
FROM public.datazonesgcr_homelocation_count_std d;

DROP TABLE datazonesgcr_homelocation_count_std; 
ALTER TABLE datazonesgcr_homelocation_count_std_1
RENAME TO datazonesgcr_homelocation_count_std;

create table datazonesgcr_homelocation_count_std_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2020_Jan" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2020jan_std
FROM public.datazonesgcr_homelocation_count_std d;

DROP TABLE datazonesgcr_homelocation_count_std; 
ALTER TABLE datazonesgcr_homelocation_count_std_1
RENAME TO datazonesgcr_homelocation_count_std;

create table datazonesgcr_homelocation_count_std_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2020_Feb" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2020feb_std
FROM public.datazonesgcr_homelocation_count_std d;

DROP TABLE datazonesgcr_homelocation_count_std; 
ALTER TABLE datazonesgcr_homelocation_count_std_1
RENAME TO datazonesgcr_homelocation_count_std;

create table datazonesgcr_homelocation_count_std_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2020_Mar" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2020mar_std
FROM public.datazonesgcr_homelocation_count_std d;

DROP TABLE datazonesgcr_homelocation_count_std; 
ALTER TABLE datazonesgcr_homelocation_count_std_1
RENAME TO datazonesgcr_homelocation_count_std;

create table datazonesgcr_homelocation_count_std_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2020_Apr" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2020apr_std
FROM public.datazonesgcr_homelocation_count_std d;

DROP TABLE datazonesgcr_homelocation_count_std; 
ALTER TABLE datazonesgcr_homelocation_count_std_1
RENAME TO datazonesgcr_homelocation_count_std;

create table datazonesgcr_homelocation_count_std_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2020_May" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2020may_std
FROM public.datazonesgcr_homelocation_count_std d;

DROP TABLE datazonesgcr_homelocation_count_std; 
ALTER TABLE datazonesgcr_homelocation_count_std_1
RENAME TO datazonesgcr_homelocation_count_std;

create table datazonesgcr_homelocation_count_std_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2020_Jun" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2020jun_std
FROM public.datazonesgcr_homelocation_count_std d;

DROP TABLE datazonesgcr_homelocation_count_std; 
ALTER TABLE datazonesgcr_homelocation_count_std_1
RENAME TO datazonesgcr_homelocation_count_std;

create table datazonesgcr_homelocation_count_std_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2020_Jul" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2020jul_std
FROM public.datazonesgcr_homelocation_count_std d;

DROP TABLE datazonesgcr_homelocation_count_std; 
ALTER TABLE datazonesgcr_homelocation_count_std_1
RENAME TO datazonesgcr_homelocation_count_std;

create table datazonesgcr_homelocation_count_std_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2020_Aug" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2020aug_std
FROM public.datazonesgcr_homelocation_count_std d;

DROP TABLE datazonesgcr_homelocation_count_std; 
ALTER TABLE datazonesgcr_homelocation_count_std_1
RENAME TO datazonesgcr_homelocation_count_std;

create table datazonesgcr_homelocation_count_std_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2020_Sep" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2020sep_std
FROM public.datazonesgcr_homelocation_count_std d;

DROP TABLE datazonesgcr_homelocation_count_std; 
ALTER TABLE datazonesgcr_homelocation_count_std_1
RENAME TO datazonesgcr_homelocation_count_std;

create table datazonesgcr_homelocation_count_std_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2020_Oct" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2020oct_std
FROM public.datazonesgcr_homelocation_count_std d;

DROP TABLE datazonesgcr_homelocation_count_std; 
ALTER TABLE datazonesgcr_homelocation_count_std_1
RENAME TO datazonesgcr_homelocation_count_std;

create table datazonesgcr_homelocation_count_std_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2020_Nov" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2020nov_std
FROM public.datazonesgcr_homelocation_count_std d;

DROP TABLE datazonesgcr_homelocation_count_std; 
ALTER TABLE datazonesgcr_homelocation_count_std_1
RENAME TO datazonesgcr_homelocation_count_std;

create table datazonesgcr_homelocation_count_std_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2020_Dec" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2020dec_std
FROM public.datazonesgcr_homelocation_count_std d;

DROP TABLE datazonesgcr_homelocation_count_std; 
ALTER TABLE datazonesgcr_homelocation_count_std_1
RENAME TO datazonesgcr_homelocation_count_std;

create table datazonesgcr_homelocation_count_std_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2021_Jan" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2021jan_std
FROM public.datazonesgcr_homelocation_count_std d;

DROP TABLE datazonesgcr_homelocation_count_std; 
ALTER TABLE datazonesgcr_homelocation_count_std_1
RENAME TO datazonesgcr_homelocation_count_std;

create table datazonesgcr_homelocation_count_std_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2021_Feb" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2021feb_std
FROM public.datazonesgcr_homelocation_count_std d;

DROP TABLE datazonesgcr_homelocation_count_std; 
ALTER TABLE datazonesgcr_homelocation_count_std_1
RENAME TO datazonesgcr_homelocation_count_std;

create table datazonesgcr_homelocation_count_std_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2021_Mar" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2021mar_std
FROM public.datazonesgcr_homelocation_count_std d;

DROP TABLE datazonesgcr_homelocation_count_std; 
ALTER TABLE datazonesgcr_homelocation_count_std_1
RENAME TO datazonesgcr_homelocation_count_std;

create table datazonesgcr_homelocation_count_std_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2021_Apr" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2021apr_std
FROM public.datazonesgcr_homelocation_count_std d;

DROP TABLE datazonesgcr_homelocation_count_std; 
ALTER TABLE datazonesgcr_homelocation_count_std_1
RENAME TO datazonesgcr_homelocation_count_std;

create table datazonesgcr_homelocation_count_std_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2021_May" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2021may_std
FROM public.datazonesgcr_homelocation_count_std d;

DROP TABLE datazonesgcr_homelocation_count_std; 
ALTER TABLE datazonesgcr_homelocation_count_std_1
RENAME TO datazonesgcr_homelocation_count_std;

create table datazonesgcr_homelocation_count_std_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2021_Jun" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2021jun_std
FROM public.datazonesgcr_homelocation_count_std d;

DROP TABLE datazonesgcr_homelocation_count_std; 
ALTER TABLE datazonesgcr_homelocation_count_std_1
RENAME TO datazonesgcr_homelocation_count_std;

create table datazonesgcr_homelocation_count_std_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2021_Jul" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2021jul_std
FROM public.datazonesgcr_homelocation_count_std d;

DROP TABLE datazonesgcr_homelocation_count_std; 
ALTER TABLE datazonesgcr_homelocation_count_std_1
RENAME TO datazonesgcr_homelocation_count_std;

create table datazonesgcr_homelocation_count_std_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2021_Aug" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2021aug_std
FROM public.datazonesgcr_homelocation_count_std d;

DROP TABLE datazonesgcr_homelocation_count_std; 
ALTER TABLE datazonesgcr_homelocation_count_std_1
RENAME TO datazonesgcr_homelocation_count_std;

create table datazonesgcr_homelocation_count_std_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2021_Sep" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2021sep_std
FROM public.datazonesgcr_homelocation_count_std d;

DROP TABLE datazonesgcr_homelocation_count_std; 
ALTER TABLE datazonesgcr_homelocation_count_std_1
RENAME TO datazonesgcr_homelocation_count_std;

create table datazonesgcr_homelocation_count_std_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2021_Oct" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2021oct_std
FROM public.datazonesgcr_homelocation_count_std d;

DROP TABLE datazonesgcr_homelocation_count_std; 
ALTER TABLE datazonesgcr_homelocation_count_std_1
RENAME TO datazonesgcr_homelocation_count_std;

create table datazonesgcr_homelocation_count_std_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2021_Nov" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2021nov_std
FROM public.datazonesgcr_homelocation_count_std d;

DROP TABLE datazonesgcr_homelocation_count_std; 
ALTER TABLE datazonesgcr_homelocation_count_std_1
RENAME TO datazonesgcr_homelocation_count_std;

create table datazonesgcr_homelocation_count_std_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2021_Dec" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2021dec_std
FROM public.datazonesgcr_homelocation_count_std d;

DROP TABLE datazonesgcr_homelocation_count_std; 
ALTER TABLE datazonesgcr_homelocation_count_std_1
RENAME TO datazonesgcr_homelocation_count_std;


--1.7.2 Postcode (CACI)
--1.7.2.1 Huq
create table caci_homelocation_count_std as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."std_homelocations_huq_2019_subset" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS huq2019std
FROM public.caci_homelocation_count c

create table caci_homelocation_count_std_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."std_homelocations_huq_2020_subset" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS huq2020std
FROM public.caci_homelocation_count_std c

DROP TABLE caci_homelocation_count_std; 
ALTER TABLE caci_homelocation_count_std_1
RENAME TO caci_homelocation_count_std;

create table caci_homelocation_count_std_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."std_homelocations_huq_2021_subset" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS huq2021std
FROM public.caci_homelocation_count_std c

DROP TABLE caci_homelocation_count_std; 
ALTER TABLE caci_homelocation_count_std_1
RENAME TO caci_homelocation_count_std;

--1.7.2.2 Tamoco (monthly because re-hashed monthly)
create table caci_homelocation_count_std_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2019_Jan" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2019jan_std
FROM public.caci_homelocation_count_std c;

DROP TABLE caci_homelocation_count_std; 
ALTER TABLE caci_homelocation_count_std_1
RENAME TO caci_homelocation_count_std;

create table caci_homelocation_count_std_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2019_Feb" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2019feb_std
FROM public.caci_homelocation_count_std c;

DROP TABLE caci_homelocation_count_std; 
ALTER TABLE caci_homelocation_count_std_1
RENAME TO caci_homelocation_count_std;

create table caci_homelocation_count_std_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2019_Mar" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2019mar_std
FROM public.caci_homelocation_count_std c;

DROP TABLE caci_homelocation_count_std; 
ALTER TABLE caci_homelocation_count_std_1
RENAME TO caci_homelocation_count_std;

create table caci_homelocation_count_std_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2019_Apr" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2019apr_std
FROM public.caci_homelocation_count_std c;

DROP TABLE caci_homelocation_count_std; 
ALTER TABLE caci_homelocation_count_std_1
RENAME TO caci_homelocation_count_std;

create table caci_homelocation_count_std_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2019_May" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2019may_std
FROM public.caci_homelocation_count_std c;

DROP TABLE caci_homelocation_count_std; 
ALTER TABLE caci_homelocation_count_std_1
RENAME TO caci_homelocation_count_std;

create table caci_homelocation_count_std_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2019_Jun" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2019jun_std
FROM public.caci_homelocation_count_std c;

DROP TABLE caci_homelocation_count_std; 
ALTER TABLE caci_homelocation_count_std_1
RENAME TO caci_homelocation_count_std;

create table caci_homelocation_count_std_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2019_Jul" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2019jul_std
FROM public.caci_homelocation_count_std c;

DROP TABLE caci_homelocation_count_std; 
ALTER TABLE caci_homelocation_count_std_1
RENAME TO caci_homelocation_count_std;

create table caci_homelocation_count_std_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2019_Aug" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2019aug_std
FROM public.caci_homelocation_count_std c;

DROP TABLE caci_homelocation_count_std; 
ALTER TABLE caci_homelocation_count_std_1
RENAME TO caci_homelocation_count_std;

create table caci_homelocation_count_std_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2019_Sep" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2019sep_std
FROM public.caci_homelocation_count_std c;

DROP TABLE caci_homelocation_count_std; 
ALTER TABLE caci_homelocation_count_std_1
RENAME TO caci_homelocation_count_std;

create table caci_homelocation_count_std_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2019_Oct" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2019oct_std
FROM public.caci_homelocation_count_std c;

DROP TABLE caci_homelocation_count_std; 
ALTER TABLE caci_homelocation_count_std_1
RENAME TO caci_homelocation_count_std;

create table caci_homelocation_count_std_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2019_Nov" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2019nov_std
FROM public.caci_homelocation_count_std c;

create table caci_homelocation_count_std_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2019_Dec" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2019dec_std
FROM public.caci_homelocation_count_std c;

DROP TABLE caci_homelocation_count_std; 
ALTER TABLE caci_homelocation_count_std_1
RENAME TO caci_homelocation_count_std;

create table caci_homelocation_count_std_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2020_Jan" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2020jan_std
FROM public.caci_homelocation_count_std c;

DROP TABLE caci_homelocation_count_std; 
ALTER TABLE caci_homelocation_count_std_1
RENAME TO caci_homelocation_count_std;

create table caci_homelocation_count_std_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2020_Feb" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2020feb_std
FROM public.caci_homelocation_count_std c;

DROP TABLE caci_homelocation_count_std; 
ALTER TABLE caci_homelocation_count_std_1
RENAME TO caci_homelocation_count_std;

create table caci_homelocation_count_std_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2020_Mar" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2020mar_std
FROM public.caci_homelocation_count_std c;

DROP TABLE caci_homelocation_count_std; 
ALTER TABLE caci_homelocation_count_std_1
RENAME TO caci_homelocation_count_std;

create table caci_homelocation_count_std_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2020_Apr" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2020apr_std
FROM public.caci_homelocation_count_std c;

DROP TABLE caci_homelocation_count_std; 
ALTER TABLE caci_homelocation_count_std_1
RENAME TO caci_homelocation_count_std;

create table caci_homelocation_count_std_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2020_May" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2020may_std
FROM public.caci_homelocation_count_std c;

DROP TABLE caci_homelocation_count_std; 
ALTER TABLE caci_homelocation_count_std_1
RENAME TO caci_homelocation_count_std;

create table caci_homelocation_count_std_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2020_Jun" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2020jun_std
FROM public.caci_homelocation_count_std c;

DROP TABLE caci_homelocation_count_std; 
ALTER TABLE caci_homelocation_count_std_1
RENAME TO caci_homelocation_count_std;

create table caci_homelocation_count_std_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2020_Jul" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2020jul_std
FROM public.caci_homelocation_count_std c;

DROP TABLE caci_homelocation_count_std; 
ALTER TABLE caci_homelocation_count_std_1
RENAME TO caci_homelocation_count_std;

create table caci_homelocation_count_std_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2020_Aug" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2020aug_std
FROM public.caci_homelocation_count_std c;

DROP TABLE caci_homelocation_count_std; 
ALTER TABLE caci_homelocation_count_std_1
RENAME TO caci_homelocation_count_std;

create table caci_homelocation_count_std_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2020_Sep" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2020sep_std
FROM public.caci_homelocation_count_std c;

DROP TABLE caci_homelocation_count_std; 
ALTER TABLE caci_homelocation_count_std_1
RENAME TO caci_homelocation_count_std;

create table caci_homelocation_count_std_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2020_Oct" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2020oct_std
FROM public.caci_homelocation_count_std c;

DROP TABLE caci_homelocation_count_std; 
ALTER TABLE caci_homelocation_count_std_1
RENAME TO caci_homelocation_count_std;

create table caci_homelocation_count_std_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2020_Nov" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2020nov_std
FROM public.caci_homelocation_count_std c;

DROP TABLE caci_homelocation_count_std; 
ALTER TABLE caci_homelocation_count_std_1
RENAME TO caci_homelocation_count_std;

create table caci_homelocation_count_std_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2020_Dec" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2020dec_std
FROM public.caci_homelocation_count_std c;

DROP TABLE caci_homelocation_count_std; 
ALTER TABLE caci_homelocation_count_std_1
RENAME TO caci_homelocation_count_std;

create table caci_homelocation_count_std_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2021_Jan" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2021jan_std
FROM public.caci_homelocation_count_std c;

DROP TABLE caci_homelocation_count_std; 
ALTER TABLE caci_homelocation_count_std_1
RENAME TO caci_homelocation_count_std;

create table caci_homelocation_count_std_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2021_Feb" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2021feb_std
FROM public.caci_homelocation_count_std c;

DROP TABLE caci_homelocation_count_std; 
ALTER TABLE caci_homelocation_count_std_1
RENAME TO caci_homelocation_count_std;

create table caci_homelocation_count_std_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2021_Mar" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2021mar_std
FROM public.caci_homelocation_count_std c;

DROP TABLE caci_homelocation_count_std; 
ALTER TABLE caci_homelocation_count_std_1
RENAME TO caci_homelocation_count_std;

create table caci_homelocation_count_std_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2020_Apr" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2020apr_std
FROM public.caci_homelocation_count_std c;

DROP TABLE caci_homelocation_count_std; 
ALTER TABLE caci_homelocation_count_std_1
RENAME TO caci_homelocation_count_std;

create table caci_homelocation_count_std_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2021_May" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2021may_std
FROM public.caci_homelocation_count_std c;

DROP TABLE caci_homelocation_count_std; 
ALTER TABLE caci_homelocation_count_std_1
RENAME TO caci_homelocation_count_std;

create table caci_homelocation_count_std_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2021_Jun" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2021jun_std
FROM public.caci_homelocation_count_std c;

DROP TABLE caci_homelocation_count_std; 
ALTER TABLE caci_homelocation_count_std_1
RENAME TO caci_homelocation_count_std;

create table caci_homelocation_count_std_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2021_Jul" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2021jul_std
FROM public.caci_homelocation_count_std c;

DROP TABLE caci_homelocation_count_std; 
ALTER TABLE caci_homelocation_count_std_1
RENAME TO caci_homelocation_count_std;

create table caci_homelocation_count_std_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2021_Aug" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2021aug_std
FROM public.caci_homelocation_count_std c;

DROP TABLE caci_homelocation_count_std; 
ALTER TABLE caci_homelocation_count_std_1
RENAME TO caci_homelocation_count_std;

create table caci_homelocation_count_std_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2021_Sep" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2021sep_std
FROM public.caci_homelocation_count_std c;

DROP TABLE caci_homelocation_count_std; 
ALTER TABLE caci_homelocation_count_std_1
RENAME TO caci_homelocation_count_std;

create table caci_homelocation_count_std_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2021_Oct" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2021oct_std
FROM public.caci_homelocation_count_std c;

DROP TABLE caci_homelocation_count_std; 
ALTER TABLE caci_homelocation_count_std_1
RENAME TO caci_homelocation_count_std;

create table caci_homelocation_count_std_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2021_Nov" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2021nov_std
FROM public.caci_homelocation_count_std c;

DROP TABLE caci_homelocation_count_std; 
ALTER TABLE caci_homelocation_count_std_1
RENAME TO caci_homelocation_count_std;

create table caci_homelocation_count_std_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."std_homelocations_tamoco_2021_Dec" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2021dec_std
FROM public.caci_homelocation_count_std c;

DROP TABLE caci_homelocation_count_std; 
ALTER TABLE caci_homelocation_count_std_1
RENAME TO caci_homelocation_count_std;

--1.7 Geographic analysis
--1.7.1 Sum populations in each council area
SELECT "council_area" as council_area, sum(adultpop_2020) as adult_pop_2020, 
sum(huq2019std) as huq2019std, sum(huq2020std) as huq2020std, sum(huq2021std) as huq2021std, sum(tamoco2019jan_std) as tamoco2019jan_std, sum(tamoco2019feb_std) as tamoco2019feb_std,
sum(tamoco2019mar_std) as tamoco2019mar_std, sum(tamoco2019apr_std) as tamoco2019apr_std,
sum(tamoco2019may_std) as tamoco2019may_std, sum(tamoco2019jun_std) as tamoco2019jun_std,
sum(tamoco2019jul_std) as tamoco2019jul_std, sum(tamoco2019aug_std) as tamoco2019aug_std,
sum(tamoco2019sep_std) as tamoco2019sep_std, sum(tamoco2019oct_std) as tamoco2019oct_std,
sum(tamoco2019nov_std) as tamoco2019nov_std, sum(tamoco2019dec_std) as tamoco2019dec_std,
sum(tamoco2020jan_std) as tamoco2020jan_std, sum(tamoco2020feb_std) as tamoco2020feb_std,
sum(tamoco2020mar_std) as tamoco2020mar_std, sum(tamoco2020apr_std) as tamoco2020apr_std,
sum(tamoco2020may_std) as tamoco2020may_std, sum(tamoco2020jun_std) as tamoco2020jun_std,
sum(tamoco2020jul_std) as tamoco2020jul_std, sum(tamoco2020aug_std) as tamoco2020aug_std,
sum(tamoco2020sep_std) as tamoco2020sep_std, sum(tamoco2020oct_std) as tamoco2020oct_std,
sum(tamoco2020nov_std) as tamoco2020nov_std, sum(tamoco2020dec_std) as tamoco2020dec_std,
sum(tamoco2021jan_std) as tamoco2021jan_std, sum(tamoco2021feb_std) as tamoco2021feb_std,
sum(tamoco2021mar_std) as tamoco2021mar_std, sum(tamoco2021apr_std) as tamoco2021apr_std,
sum(tamoco2021may_std) as tamoco2021may_std, sum(tamoco2021jun_std) as tamoco2021jun_std,
sum(tamoco2021jul_std) as tamoco2021jul_std, sum(tamoco2021aug_std) as tamoco2021aug_std,
sum(tamoco2021sep_std) as tamoco2021sep_std, sum(tamoco2021oct_std) as tamoco2021oct_std,
sum(tamoco2021nov_std) as tamoco2021nov_std, sum(tamoco2021dec_std) as tamoco2021dec_std
FROM datazonesgcr_homelocation_count_std
GROUP BY "council_area";

--1.7.2 Sum populations in each Intermediate zone
SELECT "intermediate_zone" as intermediate_area, sum(adultpop_2020) as adult_pop_2020, 
sum(huq2019std) as huq2019std, sum(huq2020std) as huq2020std, sum(huq2021std) as huq2021std, sum(tamoco2019jan_std) as tamoco2019jan_std, sum(tamoco2019feb_std) as tamoco2019feb_std,
sum(tamoco2019mar_std) as tamoco2019mar_std, sum(tamoco2019apr_std) as tamoco2019apr_std,
sum(tamoco2019may_std) as tamoco2019may_std, sum(tamoco2019jun_std) as tamoco2019jun_std,
sum(tamoco2019jul_std) as tamoco2019jul_std, sum(tamoco2019aug_std) as tamoco2019aug_std,
sum(tamoco2019sep_std) as tamoco2019sep_std, sum(tamoco2019oct_std) as tamoco2019oct_std,
sum(tamoco2019nov_std) as tamoco2019nov_std, sum(tamoco2019dec_std) as tamoco2019dec_std,
sum(tamoco2020jan_std) as tamoco2020jan_std, sum(tamoco2020feb_std) as tamoco2020feb_std,
sum(tamoco2020mar_std) as tamoco2020mar_std, sum(tamoco2020apr_std) as tamoco2020apr_std,
sum(tamoco2020may_std) as tamoco2020may_std, sum(tamoco2020jun_std) as tamoco2020jun_std,
sum(tamoco2020jul_std) as tamoco2020jul_std, sum(tamoco2020aug_std) as tamoco2020aug_std,
sum(tamoco2020sep_std) as tamoco2020sep_std, sum(tamoco2020oct_std) as tamoco2020oct_std,
sum(tamoco2020nov_std) as tamoco2020nov_std, sum(tamoco2020dec_std) as tamoco2020dec_std,
sum(tamoco2021jan_std) as tamoco2021jan_std, sum(tamoco2021feb_std) as tamoco2021feb_std,
sum(tamoco2021mar_std) as tamoco2021mar_std, sum(tamoco2021apr_std) as tamoco2021apr_std,
sum(tamoco2021may_std) as tamoco2021may_std, sum(tamoco2021jun_std) as tamoco2021jun_std,
sum(tamoco2021jul_std) as tamoco2021jul_std, sum(tamoco2021aug_std) as tamoco2021aug_std,
sum(tamoco2021sep_std) as tamoco2021sep_std, sum(tamoco2021oct_std) as tamoco2021oct_std,
sum(tamoco2021nov_std) as tamoco2021nov_std, sum(tamoco2021dec_std) as tamoco2021dec_std
FROM datazonesgcr_homelocation_count_std
GROUP BY "intermediate_zone";


--1.8 Sociodemographic analysis accross study area
--1.8.1 SIMD
--1.8.1.1 Mobile phone populations by SIMD percentile
SELECT "simd2020_withinds_SIMD_2020v2_Percentile" as simd_percentile, sum(adultpop_2020) as adult_pop_2020, 
sum(huq2019std) as huq2019std, sum(huq2020std) as huq2020std, sum(huq2021std) as huq2021std, sum(tamoco2019jan_std) as tamoco2019jan_std, sum(tamoco2019feb_std) as tamoco2019feb_std,
sum(tamoco2019mar_std) as tamoco2019mar_std, sum(tamoco2019apr_std) as tamoco2019apr_std,
sum(tamoco2019may_std) as tamoco2019may_std, sum(tamoco2019jun_std) as tamoco2019jun_std,
sum(tamoco2019jul_std) as tamoco2019jul_std, sum(tamoco2019aug_std) as tamoco2019aug_std,
sum(tamoco2019sep_std) as tamoco2019sep_std, sum(tamoco2019oct_std) as tamoco2019oct_std,
sum(tamoco2019nov_std) as tamoco2019nov_std, sum(tamoco2019dec_std) as tamoco2019dec_std,
sum(tamoco2020jan_std) as tamoco2020jan_std, sum(tamoco2020feb_std) as tamoco2020feb_std,
sum(tamoco2020mar_std) as tamoco2020mar_std, sum(tamoco2020apr_std) as tamoco2020apr_std,
sum(tamoco2020may_std) as tamoco2020may_std, sum(tamoco2020jun_std) as tamoco2020jun_std,
sum(tamoco2020jul_std) as tamoco2020jul_std, sum(tamoco2020aug_std) as tamoco2020aug_std,
sum(tamoco2020sep_std) as tamoco2020sep_std, sum(tamoco2020oct_std) as tamoco2020oct_std,
sum(tamoco2020nov_std) as tamoco2020nov_std, sum(tamoco2020dec_std) as tamoco2020dec_std,
sum(tamoco2021jan_std) as tamoco2021jan_std, sum(tamoco2021feb_std) as tamoco2021feb_std,
sum(tamoco2021mar_std) as tamoco2021mar_std, sum(tamoco2021apr_std) as tamoco2021apr_std,
sum(tamoco2021may_std) as tamoco2021may_std, sum(tamoco2021jun_std) as tamoco2021jun_std,
sum(tamoco2021jul_std) as tamoco2021jul_std, sum(tamoco2021aug_std) as tamoco2021aug_std,
sum(tamoco2021sep_std) as tamoco2021sep_std, sum(tamoco2021oct_std) as tamoco2021oct_std,
sum(tamoco2021nov_std) as tamoco2021nov_std, sum(tamoco2021dec_std) as tamoco2021dec_std
FROM datazonesgcr_homelocation_count_std
GROUP BY "simd2020_withinds_SIMD_2020v2_Percentile";

--1.8.1.2 Mobile phone populations by SIMD decile
SELECT "simd2020_withinds_SIMD2020v2_Decile" as simd_decile, sum(adultpop_2020) as adult_pop_2020, 
sum(huq2019std) as huq2019std, sum(huq2020std) as huq2020std, sum(huq2021std) as huq2021std, sum(tamoco2019jan_std) as tamoco2019jan_std, sum(tamoco2019feb_std) as tamoco2019feb_std,
sum(tamoco2019mar_std) as tamoco2019mar_std, sum(tamoco2019apr_std) as tamoco2019apr_std,
sum(tamoco2019may_std) as tamoco2019may_std, sum(tamoco2019jun_std) as tamoco2019jun_std,
sum(tamoco2019jul_std) as tamoco2019jul_std, sum(tamoco2019aug_std) as tamoco2019aug_std,
sum(tamoco2019sep_std) as tamoco2019sep_std, sum(tamoco2019oct_std) as tamoco2019oct_std,
sum(tamoco2019nov_std) as tamoco2019nov_std, sum(tamoco2019dec_std) as tamoco2019dec_std,
sum(tamoco2020jan_std) as tamoco2020jan_std, sum(tamoco2020feb_std) as tamoco2020feb_std,
sum(tamoco2020mar_std) as tamoco2020mar_std, sum(tamoco2020apr_std) as tamoco2020apr_std,
sum(tamoco2020may_std) as tamoco2020may_std, sum(tamoco2020jun_std) as tamoco2020jun_std,
sum(tamoco2020jul_std) as tamoco2020jul_std, sum(tamoco2020aug_std) as tamoco2020aug_std,
sum(tamoco2020sep_std) as tamoco2020sep_std, sum(tamoco2020oct_std) as tamoco2020oct_std,
sum(tamoco2020nov_std) as tamoco2020nov_std, sum(tamoco2020dec_std) as tamoco2020dec_std,
sum(tamoco2021jan_std) as tamoco2021jan_std, sum(tamoco2021feb_std) as tamoco2021feb_std,
sum(tamoco2021mar_std) as tamoco2021mar_std, sum(tamoco2021apr_std) as tamoco2021apr_std,
sum(tamoco2021may_std) as tamoco2021may_std, sum(tamoco2021jun_std) as tamoco2021jun_std,
sum(tamoco2021jul_std) as tamoco2021jul_std, sum(tamoco2021aug_std) as tamoco2021aug_std,
sum(tamoco2021sep_std) as tamoco2021sep_std, sum(tamoco2021oct_std) as tamoco2021oct_std,
sum(tamoco2021nov_std) as tamoco2021nov_std, sum(tamoco2021dec_std) as tamoco2021dec_std
FROM datazonesgcr_homelocation_count_std
GROUP BY "simd2020_withinds_SIMD2020v2_Decile";

--1.8.1.3 Mobile phone populations by SIMD quintile
SELECT "simd2020_withinds_SIMD2020v2_Quintile" as simd_quintile, sum(adultpop_2020) as adult_pop_2020, 
sum(huq2019std) as huq2019std, sum(huq2020std) as huq2020std, sum(huq2021std) as huq2021std, sum(tamoco2019jan_std) as tamoco2019jan_std, sum(tamoco2019feb_std) as tamoco2019feb_std,
sum(tamoco2019mar_std) as tamoco2019mar_std, sum(tamoco2019apr_std) as tamoco2019apr_std,
sum(tamoco2019may_std) as tamoco2019may_std, sum(tamoco2019jun_std) as tamoco2019jun_std,
sum(tamoco2019jul_std) as tamoco2019jul_std, sum(tamoco2019aug_std) as tamoco2019aug_std,
sum(tamoco2019sep_std) as tamoco2019sep_std, sum(tamoco2019oct_std) as tamoco2019oct_std,
sum(tamoco2019nov_std) as tamoco2019nov_std, sum(tamoco2019dec_std) as tamoco2019dec_std,
sum(tamoco2020jan_std) as tamoco2020jan_std, sum(tamoco2020feb_std) as tamoco2020feb_std,
sum(tamoco2020mar_std) as tamoco2020mar_std, sum(tamoco2020apr_std) as tamoco2020apr_std,
sum(tamoco2020may_std) as tamoco2020may_std, sum(tamoco2020jun_std) as tamoco2020jun_std,
sum(tamoco2020jul_std) as tamoco2020jul_std, sum(tamoco2020aug_std) as tamoco2020aug_std,
sum(tamoco2020sep_std) as tamoco2020sep_std, sum(tamoco2020oct_std) as tamoco2020oct_std,
sum(tamoco2020nov_std) as tamoco2020nov_std, sum(tamoco2020dec_std) as tamoco2020dec_std,
sum(tamoco2021jan_std) as tamoco2021jan_std, sum(tamoco2021feb_std) as tamoco2021feb_std,
sum(tamoco2021mar_std) as tamoco2021mar_std, sum(tamoco2021apr_std) as tamoco2021apr_std,
sum(tamoco2021may_std) as tamoco2021may_std, sum(tamoco2021jun_std) as tamoco2021jun_std,
sum(tamoco2021jul_std) as tamoco2021jul_std, sum(tamoco2021aug_std) as tamoco2021aug_std,
sum(tamoco2021sep_std) as tamoco2021sep_std, sum(tamoco2021oct_std) as tamoco2021oct_std,
sum(tamoco2021nov_std) as tamoco2021nov_std, sum(tamoco2021dec_std) as tamoco2021dec_std
FROM datazonesgcr_homelocation_count_std
GROUP BY "simd2020_withinds_SIMD2020v2_Quintile";

--1.8.2 CACI
--1.8.2.1 Mobile phone populations by CACI category
SELECT category as caci_category, sum(population) as population, 
sum(huq2019std) as huq2019std, sum(huq2020std) as huq2020std, sum(huq2021std) as huq2021std,
sum(tamoco2019jan_std) as tamoco2019jan_std, sum(tamoco2019feb_std) as tamoco2019feb_std,
sum(tamoco2019mar_std) as tamoco2019mar_std, sum(tamoco2019apr_std) as tamoco2019apr_std,
sum(tamoco2019may_std) as tamoco2019may_std, sum(tamoco2019jun_std) as tamoco2019jun_std,
sum(tamoco2019jul_std) as tamoco2019jul_std, sum(tamoco2019aug_std) as tamoco2019aug_std,
sum(tamoco2019sep_std) as tamoco2019sep_std, sum(tamoco2019oct_std) as tamoco2019oct_std,
sum(tamoco2019nov_std) as tamoco2019nov_std, sum(tamoco2019dec_std) as tamoco2019dec_std,
sum(tamoco2020jan_std) as tamoco2020jan_std, sum(tamoco2020feb_std) as tamoco2020feb_std,
sum(tamoco2020mar_std) as tamoco2020mar_std, sum(tamoco2020apr_std) as tamoco2020apr_std,
sum(tamoco2020may_std) as tamoco2020may_std, sum(tamoco2020jun_std) as tamoco2020jun_std,
sum(tamoco2020jul_std) as tamoco2020jul_std, sum(tamoco2020aug_std) as tamoco2020aug_std,
sum(tamoco2020sep_std) as tamoco2020sep_std, sum(tamoco2020oct_std) as tamoco2020oct_std,
sum(tamoco2020nov_std) as tamoco2020nov_std, sum(tamoco2020dec_std) as tamoco2020dec_std,
sum(tamoco2021jan_std) as tamoco2021jan_std, sum(tamoco2021feb_std) as tamoco2021feb_std,
sum(tamoco2021mar_std) as tamoco2021mar_std, sum(tamoco2021apr_std) as tamoco2021apr_std,
sum(tamoco2021may_std) as tamoco2021may_std, sum(tamoco2021jun_std) as tamoco2021jun_std,
sum(tamoco2021jul_std) as tamoco2021jul_std, sum(tamoco2021aug_std) as tamoco2021aug_std,
sum(tamoco2021sep_std) as tamoco2021sep_std, sum(tamoco2021oct_std) as tamoco2021oct_std,
sum(tamoco2021nov_std) as tamoco2021nov_std, sum(tamoco2021dec_std) as tamoco2021dec_std
FROM caci_homelocation_count_std
GROUP BY category;

--1.8.2.2 Mobile phone populations by CACI group
SELECT "group" as caci_group, sum(population) as population, 
sum(huq2019std) as huq2019std, sum(huq2020std) as huq2020std, sum(huq2021std) as huq2021std,
sum(tamoco2019jan_std) as tamoco2019jan_std, sum(tamoco2019feb_std) as tamoco2019feb_std,
sum(tamoco2019mar_std) as tamoco2019mar_std, sum(tamoco2019apr_std) as tamoco2019apr_std,
sum(tamoco2019may_std) as tamoco2019may_std, sum(tamoco2019jun_std) as tamoco2019jun_std,
sum(tamoco2019jul_std) as tamoco2019jul_std, sum(tamoco2019aug_std) as tamoco2019aug_std,
sum(tamoco2019sep_std) as tamoco2019sep_std, sum(tamoco2019oct_std) as tamoco2019oct_std,
sum(tamoco2019nov_std) as tamoco2019nov_std, sum(tamoco2019dec_std) as tamoco2019dec_std,
sum(tamoco2020jan_std) as tamoco2020jan_std, sum(tamoco2020feb_std) as tamoco2020feb_std,
sum(tamoco2020mar_std) as tamoco2020mar_std, sum(tamoco2020apr_std) as tamoco2020apr_std,
sum(tamoco2020may_std) as tamoco2020may_std, sum(tamoco2020jun_std) as tamoco2020jun_std,
sum(tamoco2020jul_std) as tamoco2020jul_std, sum(tamoco2020aug_std) as tamoco2020aug_std,
sum(tamoco2020sep_std) as tamoco2020sep_std, sum(tamoco2020oct_std) as tamoco2020oct_std,
sum(tamoco2020nov_std) as tamoco2020nov_std, sum(tamoco2020dec_std) as tamoco2020dec_std,
sum(tamoco2021jan_std) as tamoco2021jan_std, sum(tamoco2021feb_std) as tamoco2021feb_std,
sum(tamoco2021mar_std) as tamoco2021mar_std, sum(tamoco2021apr_std) as tamoco2021apr_std,
sum(tamoco2021may_std) as tamoco2021may_std, sum(tamoco2021jun_std) as tamoco2021jun_std,
sum(tamoco2021jul_std) as tamoco2021jul_std, sum(tamoco2021aug_std) as tamoco2021aug_std,
sum(tamoco2021sep_std) as tamoco2021sep_std, sum(tamoco2021oct_std) as tamoco2021oct_std,
sum(tamoco2021nov_std) as tamoco2021nov_std, sum(tamoco2021dec_std) as tamoco2021dec_std
FROM caci_homelocation_count_std
GROUP BY "group";

--1.8.2.3 Mobile phone populations by CACI type
SELECT "type" as caci_type, sum(population) as population, 
sum(huq2019std) as huq2019std, sum(huq2020std) as huq2020std, sum(huq2021std) as huq2021std,
sum(tamoco2019jan_std) as tamoco2019jan_std, sum(tamoco2019feb_std) as tamoco2019feb_std,
sum(tamoco2019mar_std) as tamoco2019mar_std, sum(tamoco2019apr_std) as tamoco2019apr_std,
sum(tamoco2019may_std) as tamoco2019may_std, sum(tamoco2019jun_std) as tamoco2019jun_std,
sum(tamoco2019jul_std) as tamoco2019jul_std, sum(tamoco2019aug_std) as tamoco2019aug_std,
sum(tamoco2019sep_std) as tamoco2019sep_std, sum(tamoco2019oct_std) as tamoco2019oct_std,
sum(tamoco2019nov_std) as tamoco2019nov_std, sum(tamoco2019dec_std) as tamoco2019dec_std,
sum(tamoco2020jan_std) as tamoco2020jan_std, sum(tamoco2020feb_std) as tamoco2020feb_std,
sum(tamoco2020mar_std) as tamoco2020mar_std, sum(tamoco2020apr_std) as tamoco2020apr_std,
sum(tamoco2020may_std) as tamoco2020may_std, sum(tamoco2020jun_std) as tamoco2020jun_std,
sum(tamoco2020jul_std) as tamoco2020jul_std, sum(tamoco2020aug_std) as tamoco2020aug_std,
sum(tamoco2020sep_std) as tamoco2020sep_std, sum(tamoco2020oct_std) as tamoco2020oct_std,
sum(tamoco2020nov_std) as tamoco2020nov_std, sum(tamoco2020dec_std) as tamoco2020dec_std,
sum(tamoco2021jan_std) as tamoco2021jan_std, sum(tamoco2021feb_std) as tamoco2021feb_std,
sum(tamoco2021mar_std) as tamoco2021mar_std, sum(tamoco2021apr_std) as tamoco2021apr_std,
sum(tamoco2021may_std) as tamoco2021may_std, sum(tamoco2021jun_std) as tamoco2021jun_std,
sum(tamoco2021jul_std) as tamoco2021jul_std, sum(tamoco2021aug_std) as tamoco2021aug_std,
sum(tamoco2021sep_std) as tamoco2021sep_std, sum(tamoco2021oct_std) as tamoco2021oct_std,
sum(tamoco2021nov_std) as tamoco2021nov_std, sum(tamoco2021dec_std) as tamoco2021dec_std
FROM caci_homelocation_count_std
GROUP BY "type";

--1.9 Sociodemographic analysis within councils
--1.9.0 Codes:
'S12000029' = 'South Lanarkshire'
'S12000049' = 'Glasgow City'
'S12000045' = 'East Dunbartonshire'	
'S12000018' = 'Inverclyde'
'S12000038' = 'Renfrewshire'	
'S12000050' = 'North Lanarkshire'	
'S12000039' = 'West Dunbartonshire'
'S12000011' = 'East Renfrewshire'	

--1.9.1 SIMD
--1.9.1.1 Mobile phone populations by SIMD percentile for each council
SELECT "simd2020_withinds_SIMD_2020v2_Percentile" as simd_percentile, sum(adultpop_2020) as adult_pop_2020, 
sum(huq2019std) as huq2019std, sum(huq2020std) as huq2020std, sum(huq2021std) as huq2021std, sum(tamoco2019jan_std) as tamoco2019jan_std, sum(tamoco2019feb_std) as tamoco2019feb_std,
sum(tamoco2019mar_std) as tamoco2019mar_std, sum(tamoco2019apr_std) as tamoco2019apr_std,
sum(tamoco2019may_std) as tamoco2019may_std, sum(tamoco2019jun_std) as tamoco2019jun_std,
sum(tamoco2019jul_std) as tamoco2019jul_std, sum(tamoco2019aug_std) as tamoco2019aug_std,
sum(tamoco2019sep_std) as tamoco2019sep_std, sum(tamoco2019oct_std) as tamoco2019oct_std,
sum(tamoco2019nov_std) as tamoco2019nov_std, sum(tamoco2019dec_std) as tamoco2019dec_std,
sum(tamoco2020jan_std) as tamoco2020jan_std, sum(tamoco2020feb_std) as tamoco2020feb_std,
sum(tamoco2020mar_std) as tamoco2020mar_std, sum(tamoco2020apr_std) as tamoco2020apr_std,
sum(tamoco2020may_std) as tamoco2020may_std, sum(tamoco2020jun_std) as tamoco2020jun_std,
sum(tamoco2020jul_std) as tamoco2020jul_std, sum(tamoco2020aug_std) as tamoco2020aug_std,
sum(tamoco2020sep_std) as tamoco2020sep_std, sum(tamoco2020oct_std) as tamoco2020oct_std,
sum(tamoco2020nov_std) as tamoco2020nov_std, sum(tamoco2020dec_std) as tamoco2020dec_std,
sum(tamoco2021jan_std) as tamoco2021jan_std, sum(tamoco2021feb_std) as tamoco2021feb_std,
sum(tamoco2021mar_std) as tamoco2021mar_std, sum(tamoco2021apr_std) as tamoco2021apr_std,
sum(tamoco2021may_std) as tamoco2021may_std, sum(tamoco2021jun_std) as tamoco2021jun_std,
sum(tamoco2021jul_std) as tamoco2021jul_std, sum(tamoco2021aug_std) as tamoco2021aug_std,
sum(tamoco2021sep_std) as tamoco2021sep_std, sum(tamoco2021oct_std) as tamoco2021oct_std,
sum(tamoco2021nov_std) as tamoco2021nov_std, sum(tamoco2021dec_std) as tamoco2021dec_std
FROM datazonesgcr_homelocation_count_std
WHERE council_area = 'South Lanarkshire'
GROUP BY "simd2020_withinds_SIMD_2020v2_Percentile";

SELECT "simd2020_withinds_SIMD_2020v2_Percentile" as simd_percentile, sum(adultpop_2020) as adult_pop_2020, 
sum(huq2019std) as huq2019std, sum(huq2020std) as huq2020std, sum(huq2021std) as huq2021std, sum(tamoco2019jan_std) as tamoco2019jan_std, sum(tamoco2019feb_std) as tamoco2019feb_std,
sum(tamoco2019mar_std) as tamoco2019mar_std, sum(tamoco2019apr_std) as tamoco2019apr_std,
sum(tamoco2019may_std) as tamoco2019may_std, sum(tamoco2019jun_std) as tamoco2019jun_std,
sum(tamoco2019jul_std) as tamoco2019jul_std, sum(tamoco2019aug_std) as tamoco2019aug_std,
sum(tamoco2019sep_std) as tamoco2019sep_std, sum(tamoco2019oct_std) as tamoco2019oct_std,
sum(tamoco2019nov_std) as tamoco2019nov_std, sum(tamoco2019dec_std) as tamoco2019dec_std,
sum(tamoco2020jan_std) as tamoco2020jan_std, sum(tamoco2020feb_std) as tamoco2020feb_std,
sum(tamoco2020mar_std) as tamoco2020mar_std, sum(tamoco2020apr_std) as tamoco2020apr_std,
sum(tamoco2020may_std) as tamoco2020may_std, sum(tamoco2020jun_std) as tamoco2020jun_std,
sum(tamoco2020jul_std) as tamoco2020jul_std, sum(tamoco2020aug_std) as tamoco2020aug_std,
sum(tamoco2020sep_std) as tamoco2020sep_std, sum(tamoco2020oct_std) as tamoco2020oct_std,
sum(tamoco2020nov_std) as tamoco2020nov_std, sum(tamoco2020dec_std) as tamoco2020dec_std,
sum(tamoco2021jan_std) as tamoco2021jan_std, sum(tamoco2021feb_std) as tamoco2021feb_std,
sum(tamoco2021mar_std) as tamoco2021mar_std, sum(tamoco2021apr_std) as tamoco2021apr_std,
sum(tamoco2021may_std) as tamoco2021may_std, sum(tamoco2021jun_std) as tamoco2021jun_std,
sum(tamoco2021jul_std) as tamoco2021jul_std, sum(tamoco2021aug_std) as tamoco2021aug_std,
sum(tamoco2021sep_std) as tamoco2021sep_std, sum(tamoco2021oct_std) as tamoco2021oct_std,
sum(tamoco2021nov_std) as tamoco2021nov_std, sum(tamoco2021dec_std) as tamoco2021dec_std
FROM datazonesgcr_homelocation_count_std
WHERE council_area = 'Glasgow City'
GROUP BY "simd2020_withinds_SIMD_2020v2_Percentile";

SELECT "simd2020_withinds_SIMD_2020v2_Percentile" as simd_percentile, sum(adultpop_2020) as adult_pop_2020, 
sum(huq2019std) as huq2019std, sum(huq2020std) as huq2020std, sum(huq2021std) as huq2021std, sum(tamoco2019jan_std) as tamoco2019jan_std, sum(tamoco2019feb_std) as tamoco2019feb_std,
sum(tamoco2019mar_std) as tamoco2019mar_std, sum(tamoco2019apr_std) as tamoco2019apr_std,
sum(tamoco2019may_std) as tamoco2019may_std, sum(tamoco2019jun_std) as tamoco2019jun_std,
sum(tamoco2019jul_std) as tamoco2019jul_std, sum(tamoco2019aug_std) as tamoco2019aug_std,
sum(tamoco2019sep_std) as tamoco2019sep_std, sum(tamoco2019oct_std) as tamoco2019oct_std,
sum(tamoco2019nov_std) as tamoco2019nov_std, sum(tamoco2019dec_std) as tamoco2019dec_std,
sum(tamoco2020jan_std) as tamoco2020jan_std, sum(tamoco2020feb_std) as tamoco2020feb_std,
sum(tamoco2020mar_std) as tamoco2020mar_std, sum(tamoco2020apr_std) as tamoco2020apr_std,
sum(tamoco2020may_std) as tamoco2020may_std, sum(tamoco2020jun_std) as tamoco2020jun_std,
sum(tamoco2020jul_std) as tamoco2020jul_std, sum(tamoco2020aug_std) as tamoco2020aug_std,
sum(tamoco2020sep_std) as tamoco2020sep_std, sum(tamoco2020oct_std) as tamoco2020oct_std,
sum(tamoco2020nov_std) as tamoco2020nov_std, sum(tamoco2020dec_std) as tamoco2020dec_std,
sum(tamoco2021jan_std) as tamoco2021jan_std, sum(tamoco2021feb_std) as tamoco2021feb_std,
sum(tamoco2021mar_std) as tamoco2021mar_std, sum(tamoco2021apr_std) as tamoco2021apr_std,
sum(tamoco2021may_std) as tamoco2021may_std, sum(tamoco2021jun_std) as tamoco2021jun_std,
sum(tamoco2021jul_std) as tamoco2021jul_std, sum(tamoco2021aug_std) as tamoco2021aug_std,
sum(tamoco2021sep_std) as tamoco2021sep_std, sum(tamoco2021oct_std) as tamoco2021oct_std,
sum(tamoco2021nov_std) as tamoco2021nov_std, sum(tamoco2021dec_std) as tamoco2021dec_std
FROM datazonesgcr_homelocation_count_std
WHERE council_area = 'East Dunbartonshire'
GROUP BY "simd2020_withinds_SIMD_2020v2_Percentile";

SELECT "simd2020_withinds_SIMD_2020v2_Percentile" as simd_percentile, sum(adultpop_2020) as adult_pop_2020, 
sum(huq2019std) as huq2019std, sum(huq2020std) as huq2020std, sum(huq2021std) as huq2021std, sum(tamoco2019jan_std) as tamoco2019jan_std, sum(tamoco2019feb_std) as tamoco2019feb_std,
sum(tamoco2019mar_std) as tamoco2019mar_std, sum(tamoco2019apr_std) as tamoco2019apr_std,
sum(tamoco2019may_std) as tamoco2019may_std, sum(tamoco2019jun_std) as tamoco2019jun_std,
sum(tamoco2019jul_std) as tamoco2019jul_std, sum(tamoco2019aug_std) as tamoco2019aug_std,
sum(tamoco2019sep_std) as tamoco2019sep_std, sum(tamoco2019oct_std) as tamoco2019oct_std,
sum(tamoco2019nov_std) as tamoco2019nov_std, sum(tamoco2019dec_std) as tamoco2019dec_std,
sum(tamoco2020jan_std) as tamoco2020jan_std, sum(tamoco2020feb_std) as tamoco2020feb_std,
sum(tamoco2020mar_std) as tamoco2020mar_std, sum(tamoco2020apr_std) as tamoco2020apr_std,
sum(tamoco2020may_std) as tamoco2020may_std, sum(tamoco2020jun_std) as tamoco2020jun_std,
sum(tamoco2020jul_std) as tamoco2020jul_std, sum(tamoco2020aug_std) as tamoco2020aug_std,
sum(tamoco2020sep_std) as tamoco2020sep_std, sum(tamoco2020oct_std) as tamoco2020oct_std,
sum(tamoco2020nov_std) as tamoco2020nov_std, sum(tamoco2020dec_std) as tamoco2020dec_std,
sum(tamoco2021jan_std) as tamoco2021jan_std, sum(tamoco2021feb_std) as tamoco2021feb_std,
sum(tamoco2021mar_std) as tamoco2021mar_std, sum(tamoco2021apr_std) as tamoco2021apr_std,
sum(tamoco2021may_std) as tamoco2021may_std, sum(tamoco2021jun_std) as tamoco2021jun_std,
sum(tamoco2021jul_std) as tamoco2021jul_std, sum(tamoco2021aug_std) as tamoco2021aug_std,
sum(tamoco2021sep_std) as tamoco2021sep_std, sum(tamoco2021oct_std) as tamoco2021oct_std,
sum(tamoco2021nov_std) as tamoco2021nov_std, sum(tamoco2021dec_std) as tamoco2021dec_std
FROM datazonesgcr_homelocation_count_std
WHERE council_area = 'Inverclyde'
GROUP BY "simd2020_withinds_SIMD_2020v2_Percentile";


SELECT "simd2020_withinds_SIMD_2020v2_Percentile" as simd_percentile, sum(adultpop_2020) as adult_pop_2020, 
sum(huq2019std) as huq2019std, sum(huq2020std) as huq2020std, sum(huq2021std) as huq2021std, sum(tamoco2019jan_std) as tamoco2019jan_std, sum(tamoco2019feb_std) as tamoco2019feb_std,
sum(tamoco2019mar_std) as tamoco2019mar_std, sum(tamoco2019apr_std) as tamoco2019apr_std,
sum(tamoco2019may_std) as tamoco2019may_std, sum(tamoco2019jun_std) as tamoco2019jun_std,
sum(tamoco2019jul_std) as tamoco2019jul_std, sum(tamoco2019aug_std) as tamoco2019aug_std,
sum(tamoco2019sep_std) as tamoco2019sep_std, sum(tamoco2019oct_std) as tamoco2019oct_std,
sum(tamoco2019nov_std) as tamoco2019nov_std, sum(tamoco2019dec_std) as tamoco2019dec_std,
sum(tamoco2020jan_std) as tamoco2020jan_std, sum(tamoco2020feb_std) as tamoco2020feb_std,
sum(tamoco2020mar_std) as tamoco2020mar_std, sum(tamoco2020apr_std) as tamoco2020apr_std,
sum(tamoco2020may_std) as tamoco2020may_std, sum(tamoco2020jun_std) as tamoco2020jun_std,
sum(tamoco2020jul_std) as tamoco2020jul_std, sum(tamoco2020aug_std) as tamoco2020aug_std,
sum(tamoco2020sep_std) as tamoco2020sep_std, sum(tamoco2020oct_std) as tamoco2020oct_std,
sum(tamoco2020nov_std) as tamoco2020nov_std, sum(tamoco2020dec_std) as tamoco2020dec_std,
sum(tamoco2021jan_std) as tamoco2021jan_std, sum(tamoco2021feb_std) as tamoco2021feb_std,
sum(tamoco2021mar_std) as tamoco2021mar_std, sum(tamoco2021apr_std) as tamoco2021apr_std,
sum(tamoco2021may_std) as tamoco2021may_std, sum(tamoco2021jun_std) as tamoco2021jun_std,
sum(tamoco2021jul_std) as tamoco2021jul_std, sum(tamoco2021aug_std) as tamoco2021aug_std,
sum(tamoco2021sep_std) as tamoco2021sep_std, sum(tamoco2021oct_std) as tamoco2021oct_std,
sum(tamoco2021nov_std) as tamoco2021nov_std, sum(tamoco2021dec_std) as tamoco2021dec_std
FROM datazonesgcr_homelocation_count_std
WHERE council_area = 'Renfrewshire'
GROUP BY "simd2020_withinds_SIMD_2020v2_Percentile";

SELECT "simd2020_withinds_SIMD_2020v2_Percentile" as simd_percentile, sum(adultpop_2020) as adult_pop_2020, 
sum(huq2019std) as huq2019std, sum(huq2020std) as huq2020std, sum(huq2021std) as huq2021std, sum(tamoco2019jan_std) as tamoco2019jan_std, sum(tamoco2019feb_std) as tamoco2019feb_std,
sum(tamoco2019mar_std) as tamoco2019mar_std, sum(tamoco2019apr_std) as tamoco2019apr_std,
sum(tamoco2019may_std) as tamoco2019may_std, sum(tamoco2019jun_std) as tamoco2019jun_std,
sum(tamoco2019jul_std) as tamoco2019jul_std, sum(tamoco2019aug_std) as tamoco2019aug_std,
sum(tamoco2019sep_std) as tamoco2019sep_std, sum(tamoco2019oct_std) as tamoco2019oct_std,
sum(tamoco2019nov_std) as tamoco2019nov_std, sum(tamoco2019dec_std) as tamoco2019dec_std,
sum(tamoco2020jan_std) as tamoco2020jan_std, sum(tamoco2020feb_std) as tamoco2020feb_std,
sum(tamoco2020mar_std) as tamoco2020mar_std, sum(tamoco2020apr_std) as tamoco2020apr_std,
sum(tamoco2020may_std) as tamoco2020may_std, sum(tamoco2020jun_std) as tamoco2020jun_std,
sum(tamoco2020jul_std) as tamoco2020jul_std, sum(tamoco2020aug_std) as tamoco2020aug_std,
sum(tamoco2020sep_std) as tamoco2020sep_std, sum(tamoco2020oct_std) as tamoco2020oct_std,
sum(tamoco2020nov_std) as tamoco2020nov_std, sum(tamoco2020dec_std) as tamoco2020dec_std,
sum(tamoco2021jan_std) as tamoco2021jan_std, sum(tamoco2021feb_std) as tamoco2021feb_std,
sum(tamoco2021mar_std) as tamoco2021mar_std, sum(tamoco2021apr_std) as tamoco2021apr_std,
sum(tamoco2021may_std) as tamoco2021may_std, sum(tamoco2021jun_std) as tamoco2021jun_std,
sum(tamoco2021jul_std) as tamoco2021jul_std, sum(tamoco2021aug_std) as tamoco2021aug_std,
sum(tamoco2021sep_std) as tamoco2021sep_std, sum(tamoco2021oct_std) as tamoco2021oct_std,
sum(tamoco2021nov_std) as tamoco2021nov_std, sum(tamoco2021dec_std) as tamoco2021dec_std
FROM datazonesgcr_homelocation_count_std
WHERE council_area = 'North Lanarkshire'
GROUP BY "simd2020_withinds_SIMD_2020v2_Percentile";

SELECT "simd2020_withinds_SIMD_2020v2_Percentile" as simd_percentile, sum(adultpop_2020) as adult_pop_2020, 
sum(huq2019std) as huq2019std, sum(huq2020std) as huq2020std, sum(huq2021std) as huq2021std, sum(tamoco2019jan_std) as tamoco2019jan_std, sum(tamoco2019feb_std) as tamoco2019feb_std,
sum(tamoco2019mar_std) as tamoco2019mar_std, sum(tamoco2019apr_std) as tamoco2019apr_std,
sum(tamoco2019may_std) as tamoco2019may_std, sum(tamoco2019jun_std) as tamoco2019jun_std,
sum(tamoco2019jul_std) as tamoco2019jul_std, sum(tamoco2019aug_std) as tamoco2019aug_std,
sum(tamoco2019sep_std) as tamoco2019sep_std, sum(tamoco2019oct_std) as tamoco2019oct_std,
sum(tamoco2019nov_std) as tamoco2019nov_std, sum(tamoco2019dec_std) as tamoco2019dec_std,
sum(tamoco2020jan_std) as tamoco2020jan_std, sum(tamoco2020feb_std) as tamoco2020feb_std,
sum(tamoco2020mar_std) as tamoco2020mar_std, sum(tamoco2020apr_std) as tamoco2020apr_std,
sum(tamoco2020may_std) as tamoco2020may_std, sum(tamoco2020jun_std) as tamoco2020jun_std,
sum(tamoco2020jul_std) as tamoco2020jul_std, sum(tamoco2020aug_std) as tamoco2020aug_std,
sum(tamoco2020sep_std) as tamoco2020sep_std, sum(tamoco2020oct_std) as tamoco2020oct_std,
sum(tamoco2020nov_std) as tamoco2020nov_std, sum(tamoco2020dec_std) as tamoco2020dec_std,
sum(tamoco2021jan_std) as tamoco2021jan_std, sum(tamoco2021feb_std) as tamoco2021feb_std,
sum(tamoco2021mar_std) as tamoco2021mar_std, sum(tamoco2021apr_std) as tamoco2021apr_std,
sum(tamoco2021may_std) as tamoco2021may_std, sum(tamoco2021jun_std) as tamoco2021jun_std,
sum(tamoco2021jul_std) as tamoco2021jul_std, sum(tamoco2021aug_std) as tamoco2021aug_std,
sum(tamoco2021sep_std) as tamoco2021sep_std, sum(tamoco2021oct_std) as tamoco2021oct_std,
sum(tamoco2021nov_std) as tamoco2021nov_std, sum(tamoco2021dec_std) as tamoco2021dec_std
FROM datazonesgcr_homelocation_count_std
WHERE council_area = 'West Dunbartonshire'
GROUP BY "simd2020_withinds_SIMD_2020v2_Percentile";


SELECT "simd2020_withinds_SIMD_2020v2_Percentile" as simd_percentile, sum(adultpop_2020) as adult_pop_2020, 
sum(huq2019std) as huq2019std, sum(huq2020std) as huq2020std, sum(huq2021std) as huq2021std, sum(tamoco2019jan_std) as tamoco2019jan_std, sum(tamoco2019feb_std) as tamoco2019feb_std,
sum(tamoco2019mar_std) as tamoco2019mar_std, sum(tamoco2019apr_std) as tamoco2019apr_std,
sum(tamoco2019may_std) as tamoco2019may_std, sum(tamoco2019jun_std) as tamoco2019jun_std,
sum(tamoco2019jul_std) as tamoco2019jul_std, sum(tamoco2019aug_std) as tamoco2019aug_std,
sum(tamoco2019sep_std) as tamoco2019sep_std, sum(tamoco2019oct_std) as tamoco2019oct_std,
sum(tamoco2019nov_std) as tamoco2019nov_std, sum(tamoco2019dec_std) as tamoco2019dec_std,
sum(tamoco2020jan_std) as tamoco2020jan_std, sum(tamoco2020feb_std) as tamoco2020feb_std,
sum(tamoco2020mar_std) as tamoco2020mar_std, sum(tamoco2020apr_std) as tamoco2020apr_std,
sum(tamoco2020may_std) as tamoco2020may_std, sum(tamoco2020jun_std) as tamoco2020jun_std,
sum(tamoco2020jul_std) as tamoco2020jul_std, sum(tamoco2020aug_std) as tamoco2020aug_std,
sum(tamoco2020sep_std) as tamoco2020sep_std, sum(tamoco2020oct_std) as tamoco2020oct_std,
sum(tamoco2020nov_std) as tamoco2020nov_std, sum(tamoco2020dec_std) as tamoco2020dec_std,
sum(tamoco2021jan_std) as tamoco2021jan_std, sum(tamoco2021feb_std) as tamoco2021feb_std,
sum(tamoco2021mar_std) as tamoco2021mar_std, sum(tamoco2021apr_std) as tamoco2021apr_std,
sum(tamoco2021may_std) as tamoco2021may_std, sum(tamoco2021jun_std) as tamoco2021jun_std,
sum(tamoco2021jul_std) as tamoco2021jul_std, sum(tamoco2021aug_std) as tamoco2021aug_std,
sum(tamoco2021sep_std) as tamoco2021sep_std, sum(tamoco2021oct_std) as tamoco2021oct_std,
sum(tamoco2021nov_std) as tamoco2021nov_std, sum(tamoco2021dec_std) as tamoco2021dec_std
FROM datazonesgcr_homelocation_count_std
WHERE council_area = 'East Renfrewshire'
GROUP BY "simd2020_withinds_SIMD_2020v2_Percentile";


--1.9.2 CACI
--1.9.2.1 Mobile phone populations by CACI type for each council
SELECT "type" as caci_type, sum(population) as population, 
sum(huq2019std) as huq2019std, sum(huq2020std) as huq2020std, sum(huq2021std) as huq2021std,
sum(tamoco2019jan_std) as tamoco2019jan_std, sum(tamoco2019feb_std) as tamoco2019feb_std,
sum(tamoco2019mar_std) as tamoco2019mar_std, sum(tamoco2019apr_std) as tamoco2019apr_std,
sum(tamoco2019may_std) as tamoco2019may_std, sum(tamoco2019jun_std) as tamoco2019jun_std,
sum(tamoco2019jul_std) as tamoco2019jul_std, sum(tamoco2019aug_std) as tamoco2019aug_std,
sum(tamoco2019sep_std) as tamoco2019sep_std, sum(tamoco2019oct_std) as tamoco2019oct_std,
sum(tamoco2019nov_std) as tamoco2019nov_std, sum(tamoco2019dec_std) as tamoco2019dec_std,
sum(tamoco2020jan_std) as tamoco2020jan_std, sum(tamoco2020feb_std) as tamoco2020feb_std,
sum(tamoco2020mar_std) as tamoco2020mar_std, sum(tamoco2020apr_std) as tamoco2020apr_std,
sum(tamoco2020may_std) as tamoco2020may_std, sum(tamoco2020jun_std) as tamoco2020jun_std,
sum(tamoco2020jul_std) as tamoco2020jul_std, sum(tamoco2020aug_std) as tamoco2020aug_std,
sum(tamoco2020sep_std) as tamoco2020sep_std, sum(tamoco2020oct_std) as tamoco2020oct_std,
sum(tamoco2020nov_std) as tamoco2020nov_std, sum(tamoco2020dec_std) as tamoco2020dec_std,
sum(tamoco2021jan_std) as tamoco2021jan_std, sum(tamoco2021feb_std) as tamoco2021feb_std,
sum(tamoco2021mar_std) as tamoco2021mar_std, sum(tamoco2021apr_std) as tamoco2021apr_std,
sum(tamoco2021may_std) as tamoco2021may_std, sum(tamoco2021jun_std) as tamoco2021jun_std,
sum(tamoco2021jul_std) as tamoco2021jul_std, sum(tamoco2021aug_std) as tamoco2021aug_std,
sum(tamoco2021sep_std) as tamoco2021sep_std, sum(tamoco2021oct_std) as tamoco2021oct_std,
sum(tamoco2021nov_std) as tamoco2021nov_std, sum(tamoco2021dec_std) as tamoco2021dec_std
FROM caci_homelocation_count_std
WHERE council = 'S12000029'
GROUP BY "type";

SELECT "type" as caci_type, sum(population) as population, 
sum(huq2019std) as huq2019std, sum(huq2020std) as huq2020std, sum(huq2021std) as huq2021std,
sum(tamoco2019jan_std) as tamoco2019jan_std, sum(tamoco2019feb_std) as tamoco2019feb_std,
sum(tamoco2019mar_std) as tamoco2019mar_std, sum(tamoco2019apr_std) as tamoco2019apr_std,
sum(tamoco2019may_std) as tamoco2019may_std, sum(tamoco2019jun_std) as tamoco2019jun_std,
sum(tamoco2019jul_std) as tamoco2019jul_std, sum(tamoco2019aug_std) as tamoco2019aug_std,
sum(tamoco2019sep_std) as tamoco2019sep_std, sum(tamoco2019oct_std) as tamoco2019oct_std,
sum(tamoco2019nov_std) as tamoco2019nov_std, sum(tamoco2019dec_std) as tamoco2019dec_std,
sum(tamoco2020jan_std) as tamoco2020jan_std, sum(tamoco2020feb_std) as tamoco2020feb_std,
sum(tamoco2020mar_std) as tamoco2020mar_std, sum(tamoco2020apr_std) as tamoco2020apr_std,
sum(tamoco2020may_std) as tamoco2020may_std, sum(tamoco2020jun_std) as tamoco2020jun_std,
sum(tamoco2020jul_std) as tamoco2020jul_std, sum(tamoco2020aug_std) as tamoco2020aug_std,
sum(tamoco2020sep_std) as tamoco2020sep_std, sum(tamoco2020oct_std) as tamoco2020oct_std,
sum(tamoco2020nov_std) as tamoco2020nov_std, sum(tamoco2020dec_std) as tamoco2020dec_std,
sum(tamoco2021jan_std) as tamoco2021jan_std, sum(tamoco2021feb_std) as tamoco2021feb_std,
sum(tamoco2021mar_std) as tamoco2021mar_std, sum(tamoco2021apr_std) as tamoco2021apr_std,
sum(tamoco2021may_std) as tamoco2021may_std, sum(tamoco2021jun_std) as tamoco2021jun_std,
sum(tamoco2021jul_std) as tamoco2021jul_std, sum(tamoco2021aug_std) as tamoco2021aug_std,
sum(tamoco2021sep_std) as tamoco2021sep_std, sum(tamoco2021oct_std) as tamoco2021oct_std,
sum(tamoco2021nov_std) as tamoco2021nov_std, sum(tamoco2021dec_std) as tamoco2021dec_std
FROM caci_homelocation_count_std
WHERE council = 'S12000049'
GROUP BY "type";

SELECT "type" as caci_type, sum(population) as population, 
sum(huq2019std) as huq2019std, sum(huq2020std) as huq2020std, sum(huq2021std) as huq2021std,
sum(tamoco2019jan_std) as tamoco2019jan_std, sum(tamoco2019feb_std) as tamoco2019feb_std,
sum(tamoco2019mar_std) as tamoco2019mar_std, sum(tamoco2019apr_std) as tamoco2019apr_std,
sum(tamoco2019may_std) as tamoco2019may_std, sum(tamoco2019jun_std) as tamoco2019jun_std,
sum(tamoco2019jul_std) as tamoco2019jul_std, sum(tamoco2019aug_std) as tamoco2019aug_std,
sum(tamoco2019sep_std) as tamoco2019sep_std, sum(tamoco2019oct_std) as tamoco2019oct_std,
sum(tamoco2019nov_std) as tamoco2019nov_std, sum(tamoco2019dec_std) as tamoco2019dec_std,
sum(tamoco2020jan_std) as tamoco2020jan_std, sum(tamoco2020feb_std) as tamoco2020feb_std,
sum(tamoco2020mar_std) as tamoco2020mar_std, sum(tamoco2020apr_std) as tamoco2020apr_std,
sum(tamoco2020may_std) as tamoco2020may_std, sum(tamoco2020jun_std) as tamoco2020jun_std,
sum(tamoco2020jul_std) as tamoco2020jul_std, sum(tamoco2020aug_std) as tamoco2020aug_std,
sum(tamoco2020sep_std) as tamoco2020sep_std, sum(tamoco2020oct_std) as tamoco2020oct_std,
sum(tamoco2020nov_std) as tamoco2020nov_std, sum(tamoco2020dec_std) as tamoco2020dec_std,
sum(tamoco2021jan_std) as tamoco2021jan_std, sum(tamoco2021feb_std) as tamoco2021feb_std,
sum(tamoco2021mar_std) as tamoco2021mar_std, sum(tamoco2021apr_std) as tamoco2021apr_std,
sum(tamoco2021may_std) as tamoco2021may_std, sum(tamoco2021jun_std) as tamoco2021jun_std,
sum(tamoco2021jul_std) as tamoco2021jul_std, sum(tamoco2021aug_std) as tamoco2021aug_std,
sum(tamoco2021sep_std) as tamoco2021sep_std, sum(tamoco2021oct_std) as tamoco2021oct_std,
sum(tamoco2021nov_std) as tamoco2021nov_std, sum(tamoco2021dec_std) as tamoco2021dec_std
FROM caci_homelocation_count_std
WHERE council = 'S12000045'
GROUP BY "type";

SELECT "type" as caci_type, sum(population) as population, 
sum(huq2019std) as huq2019std, sum(huq2020std) as huq2020std, sum(huq2021std) as huq2021std,
sum(tamoco2019jan_std) as tamoco2019jan_std, sum(tamoco2019feb_std) as tamoco2019feb_std,
sum(tamoco2019mar_std) as tamoco2019mar_std, sum(tamoco2019apr_std) as tamoco2019apr_std,
sum(tamoco2019may_std) as tamoco2019may_std, sum(tamoco2019jun_std) as tamoco2019jun_std,
sum(tamoco2019jul_std) as tamoco2019jul_std, sum(tamoco2019aug_std) as tamoco2019aug_std,
sum(tamoco2019sep_std) as tamoco2019sep_std, sum(tamoco2019oct_std) as tamoco2019oct_std,
sum(tamoco2019nov_std) as tamoco2019nov_std, sum(tamoco2019dec_std) as tamoco2019dec_std,
sum(tamoco2020jan_std) as tamoco2020jan_std, sum(tamoco2020feb_std) as tamoco2020feb_std,
sum(tamoco2020mar_std) as tamoco2020mar_std, sum(tamoco2020apr_std) as tamoco2020apr_std,
sum(tamoco2020may_std) as tamoco2020may_std, sum(tamoco2020jun_std) as tamoco2020jun_std,
sum(tamoco2020jul_std) as tamoco2020jul_std, sum(tamoco2020aug_std) as tamoco2020aug_std,
sum(tamoco2020sep_std) as tamoco2020sep_std, sum(tamoco2020oct_std) as tamoco2020oct_std,
sum(tamoco2020nov_std) as tamoco2020nov_std, sum(tamoco2020dec_std) as tamoco2020dec_std,
sum(tamoco2021jan_std) as tamoco2021jan_std, sum(tamoco2021feb_std) as tamoco2021feb_std,
sum(tamoco2021mar_std) as tamoco2021mar_std, sum(tamoco2021apr_std) as tamoco2021apr_std,
sum(tamoco2021may_std) as tamoco2021may_std, sum(tamoco2021jun_std) as tamoco2021jun_std,
sum(tamoco2021jul_std) as tamoco2021jul_std, sum(tamoco2021aug_std) as tamoco2021aug_std,
sum(tamoco2021sep_std) as tamoco2021sep_std, sum(tamoco2021oct_std) as tamoco2021oct_std,
sum(tamoco2021nov_std) as tamoco2021nov_std, sum(tamoco2021dec_std) as tamoco2021dec_std
FROM caci_homelocation_count_std
WHERE council = 'S12000018'
GROUP BY "type";

SELECT "type" as caci_type, sum(population) as population, 
sum(huq2019std) as huq2019std, sum(huq2020std) as huq2020std, sum(huq2021std) as huq2021std,
sum(tamoco2019jan_std) as tamoco2019jan_std, sum(tamoco2019feb_std) as tamoco2019feb_std,
sum(tamoco2019mar_std) as tamoco2019mar_std, sum(tamoco2019apr_std) as tamoco2019apr_std,
sum(tamoco2019may_std) as tamoco2019may_std, sum(tamoco2019jun_std) as tamoco2019jun_std,
sum(tamoco2019jul_std) as tamoco2019jul_std, sum(tamoco2019aug_std) as tamoco2019aug_std,
sum(tamoco2019sep_std) as tamoco2019sep_std, sum(tamoco2019oct_std) as tamoco2019oct_std,
sum(tamoco2019nov_std) as tamoco2019nov_std, sum(tamoco2019dec_std) as tamoco2019dec_std,
sum(tamoco2020jan_std) as tamoco2020jan_std, sum(tamoco2020feb_std) as tamoco2020feb_std,
sum(tamoco2020mar_std) as tamoco2020mar_std, sum(tamoco2020apr_std) as tamoco2020apr_std,
sum(tamoco2020may_std) as tamoco2020may_std, sum(tamoco2020jun_std) as tamoco2020jun_std,
sum(tamoco2020jul_std) as tamoco2020jul_std, sum(tamoco2020aug_std) as tamoco2020aug_std,
sum(tamoco2020sep_std) as tamoco2020sep_std, sum(tamoco2020oct_std) as tamoco2020oct_std,
sum(tamoco2020nov_std) as tamoco2020nov_std, sum(tamoco2020dec_std) as tamoco2020dec_std,
sum(tamoco2021jan_std) as tamoco2021jan_std, sum(tamoco2021feb_std) as tamoco2021feb_std,
sum(tamoco2021mar_std) as tamoco2021mar_std, sum(tamoco2021apr_std) as tamoco2021apr_std,
sum(tamoco2021may_std) as tamoco2021may_std, sum(tamoco2021jun_std) as tamoco2021jun_std,
sum(tamoco2021jul_std) as tamoco2021jul_std, sum(tamoco2021aug_std) as tamoco2021aug_std,
sum(tamoco2021sep_std) as tamoco2021sep_std, sum(tamoco2021oct_std) as tamoco2021oct_std,
sum(tamoco2021nov_std) as tamoco2021nov_std, sum(tamoco2021dec_std) as tamoco2021dec_std
FROM caci_homelocation_count_std
WHERE council = 'S12000038'
GROUP BY "type";

SELECT "type" as caci_type, sum(population) as population, 
sum(huq2019std) as huq2019std, sum(huq2020std) as huq2020std, sum(huq2021std) as huq2021std,
sum(tamoco2019jan_std) as tamoco2019jan_std, sum(tamoco2019feb_std) as tamoco2019feb_std,
sum(tamoco2019mar_std) as tamoco2019mar_std, sum(tamoco2019apr_std) as tamoco2019apr_std,
sum(tamoco2019may_std) as tamoco2019may_std, sum(tamoco2019jun_std) as tamoco2019jun_std,
sum(tamoco2019jul_std) as tamoco2019jul_std, sum(tamoco2019aug_std) as tamoco2019aug_std,
sum(tamoco2019sep_std) as tamoco2019sep_std, sum(tamoco2019oct_std) as tamoco2019oct_std,
sum(tamoco2019nov_std) as tamoco2019nov_std, sum(tamoco2019dec_std) as tamoco2019dec_std,
sum(tamoco2020jan_std) as tamoco2020jan_std, sum(tamoco2020feb_std) as tamoco2020feb_std,
sum(tamoco2020mar_std) as tamoco2020mar_std, sum(tamoco2020apr_std) as tamoco2020apr_std,
sum(tamoco2020may_std) as tamoco2020may_std, sum(tamoco2020jun_std) as tamoco2020jun_std,
sum(tamoco2020jul_std) as tamoco2020jul_std, sum(tamoco2020aug_std) as tamoco2020aug_std,
sum(tamoco2020sep_std) as tamoco2020sep_std, sum(tamoco2020oct_std) as tamoco2020oct_std,
sum(tamoco2020nov_std) as tamoco2020nov_std, sum(tamoco2020dec_std) as tamoco2020dec_std,
sum(tamoco2021jan_std) as tamoco2021jan_std, sum(tamoco2021feb_std) as tamoco2021feb_std,
sum(tamoco2021mar_std) as tamoco2021mar_std, sum(tamoco2021apr_std) as tamoco2021apr_std,
sum(tamoco2021may_std) as tamoco2021may_std, sum(tamoco2021jun_std) as tamoco2021jun_std,
sum(tamoco2021jul_std) as tamoco2021jul_std, sum(tamoco2021aug_std) as tamoco2021aug_std,
sum(tamoco2021sep_std) as tamoco2021sep_std, sum(tamoco2021oct_std) as tamoco2021oct_std,
sum(tamoco2021nov_std) as tamoco2021nov_std, sum(tamoco2021dec_std) as tamoco2021dec_std
FROM caci_homelocation_count_std
WHERE council = 'S12000050'
GROUP BY "type";

SELECT "type" as caci_type, sum(population) as population, 
sum(huq2019std) as huq2019std, sum(huq2020std) as huq2020std, sum(huq2021std) as huq2021std,
sum(tamoco2019jan_std) as tamoco2019jan_std, sum(tamoco2019feb_std) as tamoco2019feb_std,
sum(tamoco2019mar_std) as tamoco2019mar_std, sum(tamoco2019apr_std) as tamoco2019apr_std,
sum(tamoco2019may_std) as tamoco2019may_std, sum(tamoco2019jun_std) as tamoco2019jun_std,
sum(tamoco2019jul_std) as tamoco2019jul_std, sum(tamoco2019aug_std) as tamoco2019aug_std,
sum(tamoco2019sep_std) as tamoco2019sep_std, sum(tamoco2019oct_std) as tamoco2019oct_std,
sum(tamoco2019nov_std) as tamoco2019nov_std, sum(tamoco2019dec_std) as tamoco2019dec_std,
sum(tamoco2020jan_std) as tamoco2020jan_std, sum(tamoco2020feb_std) as tamoco2020feb_std,
sum(tamoco2020mar_std) as tamoco2020mar_std, sum(tamoco2020apr_std) as tamoco2020apr_std,
sum(tamoco2020may_std) as tamoco2020may_std, sum(tamoco2020jun_std) as tamoco2020jun_std,
sum(tamoco2020jul_std) as tamoco2020jul_std, sum(tamoco2020aug_std) as tamoco2020aug_std,
sum(tamoco2020sep_std) as tamoco2020sep_std, sum(tamoco2020oct_std) as tamoco2020oct_std,
sum(tamoco2020nov_std) as tamoco2020nov_std, sum(tamoco2020dec_std) as tamoco2020dec_std,
sum(tamoco2021jan_std) as tamoco2021jan_std, sum(tamoco2021feb_std) as tamoco2021feb_std,
sum(tamoco2021mar_std) as tamoco2021mar_std, sum(tamoco2021apr_std) as tamoco2021apr_std,
sum(tamoco2021may_std) as tamoco2021may_std, sum(tamoco2021jun_std) as tamoco2021jun_std,
sum(tamoco2021jul_std) as tamoco2021jul_std, sum(tamoco2021aug_std) as tamoco2021aug_std,
sum(tamoco2021sep_std) as tamoco2021sep_std, sum(tamoco2021oct_std) as tamoco2021oct_std,
sum(tamoco2021nov_std) as tamoco2021nov_std, sum(tamoco2021dec_std) as tamoco2021dec_std
FROM caci_homelocation_count_std
WHERE council = 'S12000039'
GROUP BY "type";

SELECT "type" as caci_type, sum(population) as population, 
sum(huq2019std) as huq2019std, sum(huq2020std) as huq2020std, sum(huq2021std) as huq2021std,
sum(tamoco2019jan_std) as tamoco2019jan_std, sum(tamoco2019feb_std) as tamoco2019feb_std,
sum(tamoco2019mar_std) as tamoco2019mar_std, sum(tamoco2019apr_std) as tamoco2019apr_std,
sum(tamoco2019may_std) as tamoco2019may_std, sum(tamoco2019jun_std) as tamoco2019jun_std,
sum(tamoco2019jul_std) as tamoco2019jul_std, sum(tamoco2019aug_std) as tamoco2019aug_std,
sum(tamoco2019sep_std) as tamoco2019sep_std, sum(tamoco2019oct_std) as tamoco2019oct_std,
sum(tamoco2019nov_std) as tamoco2019nov_std, sum(tamoco2019dec_std) as tamoco2019dec_std,
sum(tamoco2020jan_std) as tamoco2020jan_std, sum(tamoco2020feb_std) as tamoco2020feb_std,
sum(tamoco2020mar_std) as tamoco2020mar_std, sum(tamoco2020apr_std) as tamoco2020apr_std,
sum(tamoco2020may_std) as tamoco2020may_std, sum(tamoco2020jun_std) as tamoco2020jun_std,
sum(tamoco2020jul_std) as tamoco2020jul_std, sum(tamoco2020aug_std) as tamoco2020aug_std,
sum(tamoco2020sep_std) as tamoco2020sep_std, sum(tamoco2020oct_std) as tamoco2020oct_std,
sum(tamoco2020nov_std) as tamoco2020nov_std, sum(tamoco2020dec_std) as tamoco2020dec_std,
sum(tamoco2021jan_std) as tamoco2021jan_std, sum(tamoco2021feb_std) as tamoco2021feb_std,
sum(tamoco2021mar_std) as tamoco2021mar_std, sum(tamoco2021apr_std) as tamoco2021apr_std,
sum(tamoco2021may_std) as tamoco2021may_std, sum(tamoco2021jun_std) as tamoco2021jun_std,
sum(tamoco2021jul_std) as tamoco2021jul_std, sum(tamoco2021aug_std) as tamoco2021aug_std,
sum(tamoco2021sep_std) as tamoco2021sep_std, sum(tamoco2021oct_std) as tamoco2021oct_std,
sum(tamoco2021nov_std) as tamoco2021nov_std, sum(tamoco2021dec_std) as tamoco2021dec_std
FROM caci_homelocation_count_std
WHERE council = 'S12000011'
GROUP BY "type";



---2. Method: Using activity heuristics and land use to estimate home location

--2.1 Huq

--2.1.1: Subset residential and mixed residential evenings only for each year of mobile data (2019-2021)
CREATE TABLE huq_2019_residential_datazones_evening AS
SELECT 
huq.*,
zones.use, zones.data_level
FROM huq_2019_datazone_evening huq
JOIN public.ukbuildingsgcr_4326 zones
ON ST_Intersects(tam.geom, zones.geom4326)
where zones.use = 'RESIDENTIAL ONLY' 
or zones.use = 'RETAIL WITH OFFICE/RESIDENTIAL ABOVE'
or zones.use = 'RESIDENTIAL WITH RETAIL ON GROUND FLOOR';

CREATE TABLE huq_2020_residential_datazones_evening AS
SELECT 
huq.*,
zones.use, zones.data_level
FROM huq_2020_datazone_evening huq
JOIN public.ukbuildingsgcr_4326 zones
ON ST_Intersects(tam.geom, zones.geom4326)
where zones.use = 'RESIDENTIAL ONLY' 
or zones.use = 'RETAIL WITH OFFICE/RESIDENTIAL ABOVE'
or zones.use = 'RESIDENTIAL WITH RETAIL ON GROUND FLOOR';

CREATE TABLE huq_2021_residential_datazones_evening AS
SELECT 
huq.*,
zones.use, zones.data_level
FROM huq_2021_datazone_evening huq
JOIN public.ukbuildingsgcr_4326 zones
ON ST_Intersects(tam.geom, zones.geom4326)
where zones.use = 'RESIDENTIAL ONLY' 
or zones.use = 'RETAIL WITH OFFICE/RESIDENTIAL ABOVE'
or zones.use = 'RESIDENTIAL WITH RETAIL ON GROUND FLOOR';

--2.1.2 Create index on user id for each year
CREATE INDEX IDindex_2019_res ON huq_2019_residential_datazones_evening (device_iid_hash);
CREATE INDEX IDindex_2020_res ON huq_2019_residential_datazones_evening (device_iid_hash);
CREATE INDEX IDindex_2021_res ON huq_2019_residential_datazones_evening (device_iid_hash);

--2.1.3 Create index on date (all year)
CREATE INDEX date_idx_huq_2019_res ON huq_2019_residential_datazones_evening ((timestamp::DATE));
CREATE INDEX date_idx_huq_2020_res ON huq_2019_residential_datazones_evening ((timestamp::DATE));
CREATE INDEX date_idx_huq_2021_res ON huq_2019_residential_datazones_evening ((timestamp::DATE));


--2.2 Tamoco

--2.2.1: Subset residential and mixed residential evenings only for each year of mobile data (2019-2021)
CREATE TABLE tamoco_2019_residential_datazones_evening AS
SELECT 
tam.*,
zones.use, zones.data_level
FROM tamoco_2019_datazone_evening tam
JOIN public.ukbuildingsgcr_4326 zones
ON ST_Intersects(tam.geom, zones.geom4326)
where zones.use = 'RESIDENTIAL ONLY' 
or zones.use = 'RETAIL WITH OFFICE/RESIDENTIAL ABOVE'
or zones.use = 'RESIDENTIAL WITH RETAIL ON GROUND FLOOR';

CREATE TABLE tamoco_2020_residential_datazones_evening AS
SELECT 
tam.*,
zones.use, zones.data_level
FROM tamoco_2020_datazone_evening tam
JOIN public.ukbuildingsgcr_4326 zones
ON ST_Intersects(tam.geom, zones.geom4326)
where zones.use = 'RESIDENTIAL ONLY' 
or zones.use = 'RETAIL WITH OFFICE/RESIDENTIAL ABOVE'
or zones.use = 'RESIDENTIAL WITH RETAIL ON GROUND FLOOR';

CREATE TABLE tamoco_2021_residential_datazones_evening AS
SELECT 
tam.*,
zones.use, zones.data_level
FROM tamoco_2021_datazone_evening tam
JOIN public.ukbuildingsgcr_4326 zones
ON ST_Intersects(tam.geom, zones.geom4326)
where zones.use = 'RESIDENTIAL ONLY' 
or zones.use = 'RETAIL WITH OFFICE/RESIDENTIAL ABOVE'
or zones.use = 'RESIDENTIAL WITH RETAIL ON GROUND FLOOR';

--2.2.2 Create index on date (all year)
CREATE INDEX date_idx_2019 ON tamoco_2019_residential_datazones_evening ((sdk_ts::DATE));

CREATE INDEX date_idx_2020 ON tamoco_2020_residential_datazones_evening ((sdk_ts::DATE));

CREATE INDEX date_idx_2021 ON tamoco_2021_residential_datazones_evening ((sdk_ts::DATE));

--2.2.4 Subset each month into a table (Tamoco user ID is rehashed monthly so home locations need to be estimated monthly)
CREATE TABLE tamoco_2019_jan AS
SELECT * FROM public.tamoco_2019_residential_datazones_evening WHERE CAST(sdk_ts AS date) between '2019-01-01' and '2019-01-31';
CREATE TABLE tamoco_2019_feb AS
SELECT * FROM public.tamoco_2019_residential_datazones_evening WHERE CAST(sdk_ts AS date) between '2019-02-01' and '2019-02-28';
CREATE TABLE tamoco_2019_mar AS
SELECT * FROM public.tamoco_2019_residential_datazones_evening WHERE CAST(sdk_ts AS date) between '2019-03-01' and '2019-03-31';
CREATE TABLE tamoco_2019_apr AS
SELECT * FROM public.tamoco_2019_residential_datazones_evening WHERE CAST(sdk_ts AS date) between '2019-04-01' and '2019-04-30';
CREATE TABLE tamoco_2019_may AS
SELECT * FROM public.tamoco_2019_residential_datazones_evening WHERE CAST(sdk_ts AS date) between '2019-05-01' and '2019-05-31';
CREATE TABLE tamoco_2019_jun AS
SELECT * FROM public.tamoco_2019_residential_datazones_evening WHERE CAST(sdk_ts AS date) between '2019-06-01' and '2019-06-30';
CREATE TABLE tamoco_2019_jul AS
SELECT * FROM public.tamoco_2019_residential_datazones_evening WHERE CAST(sdk_ts AS date) between '2019-07-01' and '2019-07-31';
CREATE TABLE tamoco_2019_aug AS
SELECT * FROM public.tamoco_2019_residential_datazones_evening WHERE CAST(sdk_ts AS date) between '2019-08-01' and '2019-08-31';
CREATE TABLE tamoco_2019_sep AS
SELECT * FROM public.tamoco_2019_residential_datazones_evening WHERE CAST(sdk_ts AS date) between '2019-09-01' and '2019-09-30';
CREATE TABLE tamoco_2019_oct AS
SELECT * FROM public.tamoco_2019_residential_datazones_evening WHERE CAST(sdk_ts AS date) between '2019-10-01' and '2019-10-31';
CREATE TABLE tamoco_2019_nov AS
SELECT * FROM public.tamoco_2019_residential_datazones_evening WHERE CAST(sdk_ts AS date) between '2019-11-01' and '2019-11-30';
CREATE TABLE tamoco_2019_dec AS
SELECT * FROM public.tamoco_2019_residential_datazones_evening WHERE CAST(sdk_ts AS date) between '2019-12-01' and '2019-12-31';

CREATE TABLE tamoco_2020_jan AS
SELECT * FROM public.tamoco_2020_residential_datazones_evening WHERE CAST(sdk_ts AS date) between '2020-01-01' and '2020-01-31';
CREATE TABLE tamoco_2020_feb AS
SELECT * FROM public.tamoco_2020_residential_datazones_evening WHERE CAST(sdk_ts AS date) between '2020-02-01' and '2020-02-28';
CREATE TABLE tamoco_2020_mar AS
SELECT * FROM public.tamoco_2020_residential_datazones_evening WHERE CAST(sdk_ts AS date) between '2020-03-01' and '2020-03-31';
CREATE TABLE tamoco_2020_apr AS
SELECT * FROM public.tamoco_2020_residential_datazones_evening WHERE CAST(sdk_ts AS date) between '2020-04-01' and '2020-04-30';
CREATE TABLE tamoco_2020_may AS
SELECT * FROM public.tamoco_2020_residential_datazones_evening WHERE CAST(sdk_ts AS date) between '2020-05-01' and '2020-05-31';
CREATE TABLE tamoco_2020_jun AS
SELECT * FROM public.tamoco_2020_residential_datazones_evening WHERE CAST(sdk_ts AS date) between '2020-06-01' and '2020-06-30';
CREATE TABLE tamoco_2020_jul AS
SELECT * FROM public.tamoco_2020_residential_datazones_evening WHERE CAST(sdk_ts AS date) between '2020-07-01' and '2020-07-31';
CREATE TABLE tamoco_2020_aug AS
SELECT * FROM public.tamoco_2020_residential_datazones_evening WHERE CAST(sdk_ts AS date) between '2020-08-01' and '2020-08-31';
CREATE TABLE tamoco_2020_sep AS
SELECT * FROM public.tamoco_2020_residential_datazones_evening WHERE CAST(sdk_ts AS date) between '2020-09-01' and '2020-09-30';
CREATE TABLE tamoco_2020_oct AS
SELECT * FROM public.tamoco_2020_residential_datazones_evening WHERE CAST(sdk_ts AS date) between '2020-10-01' and '2020-10-31';
CREATE TABLE tamoco_2020_nov AS
SELECT * FROM public.tamoco_2020_residential_datazones_evening WHERE CAST(sdk_ts AS date) between '2020-11-01' and '2020-11-30';
CREATE TABLE tamoco_2020_dec AS
SELECT * FROM public.tamoco_2020_residential_datazones_evening WHERE CAST(sdk_ts AS date) between '2020-12-01' and '2020-12-31';

CREATE TABLE tamoco_2021_jan AS
SELECT * FROM public.tamoco_2021_residential_datazones_evening WHERE CAST(sdk_ts AS date) between '2021-01-01' and '2021-01-31';
CREATE TABLE tamoco_2021_feb AS
SELECT * FROM public.tamoco_2021_residential_datazones_evening WHERE CAST(sdk_ts AS date) between '2021-02-01' and '2021-02-28';
CREATE TABLE tamoco_2021_mar AS
SELECT * FROM public.tamoco_2021_residential_datazones_evening WHERE CAST(sdk_ts AS date) between '2021-03-01' and '2021-03-31';
CREATE TABLE tamoco_2021_apr AS
SELECT * FROM public.tamoco_2021_residential_datazones_evening WHERE CAST(sdk_ts AS date) between '2021-04-01' and '2021-04-30';
CREATE TABLE tamoco_2021_may AS
SELECT * FROM public.tamoco_2021_residential_datazones_evening WHERE CAST(sdk_ts AS date) between '2021-05-01' and '2021-05-31';
CREATE TABLE tamoco_2021_jun AS
SELECT * FROM public.tamoco_2021_residential_datazones_evening WHERE CAST(sdk_ts AS date) between '2021-06-01' and '2021-06-30';
CREATE TABLE tamoco_2021_jul AS
SELECT * FROM public.tamoco_2021_residential_datazones_evening WHERE CAST(sdk_ts AS date) between '2021-07-01' and '2021-07-31';
CREATE TABLE tamoco_2021_aug AS
SELECT * FROM public.tamoco_2021_residential_datazones_evening WHERE CAST(sdk_ts AS date) between '2021-08-01' and '2021-08-31';
CREATE TABLE tamoco_2021_sep AS
SELECT * FROM public.tamoco_2021_residential_datazones_evening WHERE CAST(sdk_ts AS date) between '2021-09-01' and '2021-09-30';
CREATE TABLE tamoco_2021_oct AS
SELECT * FROM public.tamoco_2021_residential_datazones_evening WHERE CAST(sdk_ts AS date) between '2021-10-01' and '2021-10-31';
CREATE TABLE tamoco_2021_nov AS
SELECT * FROM public.tamoco_2021_residential_datazones_evening WHERE CAST(sdk_ts AS date) between '2021-11-01' and '2021-11-30';
CREATE TABLE tamoco_2021_dec AS
SELECT * FROM public.tamoco_2021_residential_datazones_evening WHERE CAST(sdk_ts AS date) between '2021-12-01' and '2021-12-31';

--2.2.5 Create index on user id 
CREATE INDEX ID_index_2019_jan ON tamoco_2019_jan (hashed_user_id);
CREATE INDEX ID_index_2019_feb ON tamoco_2019_feb (hashed_user_id);
CREATE INDEX ID_index_2019_mar ON tamoco_2019_mar (hashed_user_id);
CREATE INDEX ID_index_2019_apr ON tamoco_2019_apr (hashed_user_id);
CREATE INDEX ID_index_2019_may ON tamoco_2019_may (hashed_user_id);
CREATE INDEX ID_index_2019_jun ON tamoco_2019_jun (hashed_user_id);
CREATE INDEX ID_index_2019_jul ON tamoco_2019_jul (hashed_user_id);
CREATE INDEX ID_index_2019_aug ON tamoco_2019_aug (hashed_user_id);
CREATE INDEX ID_index_2019_sep ON tamoco_2019_sep (hashed_user_id);
CREATE INDEX ID_index_2019_oct ON tamoco_2019_oct (hashed_user_id);
CREATE INDEX ID_index_2019_nov ON tamoco_2019_nov (hashed_user_id);
CREATE INDEX ID_index_2019_dec ON tamoco_2019_dec (hashed_user_id);
 
CREATE INDEX ID_index_2020_jan ON tamoco_2020_jan (hashed_user_id);
CREATE INDEX ID_index_2020_feb ON tamoco_2020_feb (hashed_user_id);
CREATE INDEX ID_index_2020_mar ON tamoco_2020_mar (hashed_user_id);
CREATE INDEX ID_index_2020_apr ON tamoco_2020_apr (hashed_user_id);
CREATE INDEX ID_index_2020_may ON tamoco_2020_may (hashed_user_id);
CREATE INDEX ID_index_2020_jun ON tamoco_2020_jun (hashed_user_id);
CREATE INDEX ID_index_2020_jul ON tamoco_2020_jul (hashed_user_id);
CREATE INDEX ID_index_2020_aug ON tamoco_2020_aug (hashed_user_id);
CREATE INDEX ID_index_2020_sep ON tamoco_2020_sep (hashed_user_id);
CREATE INDEX ID_index_2020_oct ON tamoco_2020_oct (hashed_user_id);
CREATE INDEX ID_index_2020_nov ON tamoco_2020_nov (hashed_user_id);
CREATE INDEX ID_index_2020_dec ON tamoco_2020_dec (hashed_user_id);

CREATE INDEX ID_index_2021_jan ON tamoco_2021_jan (hashed_user_id);
CREATE INDEX ID_index_2021_feb ON tamoco_2021_feb_ (hashed_user_id);
CREATE INDEX ID_index_2021_mar ON tamoco_2021_mar (hashed_user_id);
CREATE INDEX ID_index_2021_apr ON tamoco_2021_apr (hashed_user_id);
CREATE INDEX ID_index_2021_may ON tamoco_2021_may (hashed_user_id);
CREATE INDEX ID_index_2021_jun ON tamoco_2021_jun (hashed_user_id);
CREATE INDEX ID_index_2021_jul ON tamoco_2021_jul (hashed_user_id);
CREATE INDEX ID_index_2021_aug ON tamoco_2021_aug (hashed_user_id);
CREATE INDEX ID_index_2021_sep ON tamoco_2021_sep (hashed_user_id);
CREATE INDEX ID_index_2021_oct ON tamoco_2021_oct (hashed_user_id);
CREATE INDEX ID_index_2021_nov ON tamoco_2021_nov (hashed_user_id);
CREATE INDEX ID_index_2021_dec ON tamoco_2021_dec (hashed_user_id);

--2.3 In R perform home location analysis at a user level (2019-2021)
--[file name Home location estimates_landuse.Rmd]

--2.4 In R save home location results into table in database (2019-2021)
--[file name Home location estimates_landuse.Rmd]


--2.5 Post processing

--2.5.1 convert home locations into spatial point object using lat/lon 
--[table: homelocations_huq/tamoco_YEAR contains all home locations which could be determined by method]
--[table: homelocations_huq/tamoco_YEAR_subset subsets the prior table to only users with 2 or more active evenings]

--2.5.1.1 Huq 

ALTER TABLE homelocations_huq_2019 ADD COLUMN geom geometry(Point, 4326);
UPDATE homelocations_huq_2019 SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);
ALTER TABLE homelocations_huq_2019_subset ADD COLUMN geom geometry(Point, 4326);
UPDATE homelocations_huq_2019_subset SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE homelocations_huq_2020 ADD COLUMN geom geometry(Point, 4326);
UPDATE homelocations_huq_2020 SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);
ALTER TABLE homelocations_huq_2020_subset ADD COLUMN geom geometry(Point, 4326);
UPDATE homelocations_huq_2020_subset SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE homelocations_huq_2021 ADD COLUMN geom geometry(Point, 4326);
UPDATE homelocations_huq_2021 SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);
ALTER TABLE homelocations_huq_2021_subset ADD COLUMN geom geometry(Point, 4326);
UPDATE homelocations_huq_2021_subset SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

--2.5.1.2 Tamoco 
ALTER TABLE "homelocations_tamoco_2019_Jan" ADD COLUMN geom geometry(Point, 4326);
UPDATE "homelocations_tamoco_2019_Jan" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "homelocations_tamoco_2019_Feb" ADD COLUMN geom geometry(Point, 4326);
UPDATE "homelocations_tamoco_2019_Feb" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "homelocations_tamoco_2019_Mar" ADD COLUMN geom geometry(Point, 4326);
UPDATE "homelocations_tamoco_2019_Mar" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "homelocations_tamoco_2019_Apr" ADD COLUMN geom geometry(Point, 4326);
UPDATE "homelocations_tamoco_2019_Apr" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "homelocations_tamoco_2019_May" ADD COLUMN geom geometry(Point, 4326);
UPDATE "homelocations_tamoco_2019_May" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "homelocations_tamoco_2019_Jun" ADD COLUMN geom geometry(Point, 4326);
UPDATE "homelocations_tamoco_2019_Jun" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "homelocations_tamoco_2019_Jul" ADD COLUMN geom geometry(Point, 4326);
UPDATE "homelocations_tamoco_2019_Jul" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "homelocations_tamoco_2019_Aug" ADD COLUMN geom geometry(Point, 4326);
UPDATE "homelocations_tamoco_2019_Aug" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "homelocations_tamoco_2019_Sep" ADD COLUMN geom geometry(Point, 4326);
UPDATE "homelocations_tamoco_2019_Sep" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "homelocations_tamoco_2019_Oct" ADD COLUMN geom geometry(Point, 4326);
UPDATE "homelocations_tamoco_2019_Oct" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "homelocations_tamoco_2019_Nov" ADD COLUMN geom geometry(Point, 4326);
UPDATE "homelocations_tamoco_2019_Nov" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "homelocations_tamoco_2019_Dec" ADD COLUMN geom geometry(Point, 4326);
UPDATE "homelocations_tamoco_2019_Dec" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);


ALTER TABLE "homelocations_tamoco_2020_Jan" ADD COLUMN geom geometry(Point, 4326);
UPDATE "homelocations_tamoco_2020_Jan" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "homelocations_tamoco_2020_Feb" ADD COLUMN geom geometry(Point, 4326);
UPDATE "homelocations_tamoco_2020_Feb" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "homelocations_tamoco_2020_Mar" ADD COLUMN geom geometry(Point, 4326);
UPDATE "homelocations_tamoco_2020_Mar" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "homelocations_tamoco_2020_Apr" ADD COLUMN geom geometry(Point, 4326);
UPDATE "homelocations_tamoco_2020_Apr" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "homelocations_tamoco_2020_May" ADD COLUMN geom geometry(Point, 4326);
UPDATE "homelocations_tamoco_2020_May" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "homelocations_tamoco_2020_Jun" ADD COLUMN geom geometry(Point, 4326);
UPDATE "homelocations_tamoco_2020_Jun" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "homelocations_tamoco_2020_Jul" ADD COLUMN geom geometry(Point, 4326);
UPDATE "homelocations_tamoco_2020_Jul" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "homelocations_tamoco_2020_Aug" ADD COLUMN geom geometry(Point, 4326);
UPDATE "homelocations_tamoco_2020_Aug" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "homelocations_tamoco_2020_Sep" ADD COLUMN geom geometry(Point, 4326);
UPDATE "homelocations_tamoco_2020_Sep" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "homelocations_tamoco_2020_Oct" ADD COLUMN geom geometry(Point, 4326);
UPDATE "homelocations_tamoco_2020_Oct" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "homelocations_tamoco_2020_Nov" ADD COLUMN geom geometry(Point, 4326);
UPDATE "homelocations_tamoco_2020_Nov" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "homelocations_tamoco_2020_Dec" ADD COLUMN geom geometry(Point, 4326);
UPDATE "homelocations_tamoco_2020_Dec" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);


ALTER TABLE "homelocations_tamoco_2021_Jan" ADD COLUMN geom geometry(Point, 4326);
UPDATE "homelocations_tamoco_2021_Jan" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "homelocations_tamoco_2021_Feb" ADD COLUMN geom geometry(Point, 4326);
UPDATE "homelocations_tamoco_2021_Feb" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "homelocations_tamoco_2021_Mar" ADD COLUMN geom geometry(Point, 4326);
UPDATE "homelocations_tamoco_2021_Mar" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "homelocations_tamoco_2021_Apr" ADD COLUMN geom geometry(Point, 4326);
UPDATE "homelocations_tamoco_2021_Apr" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "homelocations_tamoco_2021_May" ADD COLUMN geom geometry(Point, 4326);
UPDATE "homelocations_tamoco_2021_May" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "homelocations_tamoco_2021_Jun" ADD COLUMN geom geometry(Point, 4326);
UPDATE "homelocations_tamoco_2021_Jun" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "homelocations_tamoco_2021_Jul" ADD COLUMN geom geometry(Point, 4326);
UPDATE "homelocations_tamoco_2021_Jul" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "homelocations_tamoco_2021_Aug" ADD COLUMN geom geometry(Point, 4326);
UPDATE "homelocations_tamoco_2021_Aug" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "homelocations_tamoco_2021_Sep" ADD COLUMN geom geometry(Point, 4326);
UPDATE "homelocations_tamoco_2021_Sep" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "homelocations_tamoco_2021_Oct" ADD COLUMN geom geometry(Point, 4326);
UPDATE "homelocations_tamoco_2021_Oct" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "homelocations_tamoco_2021_Nov" ADD COLUMN geom geometry(Point, 4326);
UPDATE "homelocations_tamoco_2021_Nov" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

ALTER TABLE "homelocations_tamoco_2021_Dec" ADD COLUMN geom geometry(Point, 4326);
UPDATE "homelocations_tamoco_2021_Dec" SET geom = ST_SetSRID(ST_MakePoint("Longitude", "Latitude"), 4326);

--2.6 Count the volume of data generated by users 
--2.6.1 With home locations 
--2.6.1.1 Huq
SELECT COUNT(*) 
FROM huq_2019  h
WHERE EXISTS (SELECT 1 FROM homelocations_huq_2019 p WHERE p."Device_iid_hash" = h.device_iid_hash);

SELECT COUNT(*) 
FROM huq_2020 h
WHERE EXISTS (SELECT 1 FROM homelocations_huq_2020 p WHERE p."Device_iid_hash" = h.device_iid_hash);

SELECT COUNT(*) 
FROM huq_2021  h
WHERE EXISTS (SELECT 1 FROM homelocations_huq_2021 p WHERE p."Device_iid_hash" = h.device_iid_hash);

--1.6.1.2 Tamoco
SELECT COUNT(*) 
FROM tamoco_2019  h
WHERE EXISTS (SELECT 1 FROM std_homelocations_tamoco_2019 p WHERE p."Device_iid_hash" = h.hashed_user_id);

SELECT COUNT(*) 
FROM tamoco_2021  h
WHERE EXISTS (SELECT 1 FROM homelocations_tamoco_2020 p WHERE p."Device_iid_hash" = h.hashed_user_id);

SELECT COUNT(*) 
FROM tamoco_2019  h
WHERE EXISTS (SELECT 1 FROM homelocations_tamoco_2021 p WHERE p."Device_iid_hash" = h.hashed_user_id);

--2.6.2 With home locations subset to 2+ active evenings
--2.6.2.1 Huq
SELECT COUNT(*) 
FROM huq_2019  h
WHERE EXISTS (SELECT 1 FROM homelocations_huq_2019_subset p WHERE p."Device_iid_hash" = h.device_iid_hash);

SELECT COUNT(*) 
FROM huq_2020 h
WHERE EXISTS (SELECT 1 FROM homelocations_huq_2020_subset p WHERE p."Device_iid_hash" = h.device_iid_hash);

SELECT COUNT(*) 
FROM huq_2021  h
WHERE EXISTS (SELECT 1 FROM homelocations_huq_2021_subset p WHERE p."Device_iid_hash" = h.device_iid_hash);

--2.6.2.2 Tamoco
SELECT COUNT(*) 
FROM tamoco_2019  h
WHERE EXISTS (SELECT 1 FROM homelocations_tamoco_2019_subset p WHERE p."Device_iid_hash" = h.hashed_user_id);

SELECT COUNT(*) 
FROM tamoco_2020  h
WHERE EXISTS (SELECT 1 FROM homelocations_tamoco_2020_subset p WHERE p."Device_iid_hash" = h.hashed_user_id);

SELECT COUNT(*) 
FROM tamoco_2020  h
WHERE EXISTS (SELECT 1 FROM homelocations_tamoco_2020_subset p WHERE p."Device_iid_hash" = h.hashed_user_id);


--2.7 Counting the number of huq/tamoco users in each Datazone and each Postcode
--2.7.1 Datazone (SIMD)
--2.7.1.1 Huq
create table datazonesgcr_homelocation_count as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public.homelocations_huq_2019_subset h 
   WHERE ST_Contains(d.geom, h.geom)) AS huq2019
FROM public.datazonesgcr_homelocation_count d;

create table datazonesgcr_homelocation_count_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public.homelocations_huq_2020_subset h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS huq2020
FROM public.datazonesgcr_homelocation_count d;

DROP TABLE datazonesgcr_homelocation_count; 
ALTER TABLE datazonesgcr_homelocation_count_1
RENAME TO datazonesgcr_homelocation_count;

create table datazonesgcr_homelocation_count_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public.homelocations_huq_2021_subset h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS huq2021
FROM public.datazonesgcr_homelocation_count d;

DROP TABLE datazonesgcr_homelocation_count; 
ALTER TABLE datazonesgcr_homelocation_count_1
RENAME TO datazonesgcr_homelocation_count;

--2.7.2.2 Tamoco (monthly because re-hashed monthly)
create table datazonesgcr_homelocation_count_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2019_Jan" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2019_jan
FROM public.datazonesgcr_homelocation_count d;

DROP TABLE datazonesgcr_homelocation_count; 
ALTER TABLE datazonesgcr_homelocation_count_1
RENAME TO datazonesgcr_homelocation_count;

create table datazonesgcr_homelocation_count_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2019_Feb" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2019_feb
FROM public.datazonesgcr_homelocation_count d;

DROP TABLE datazonesgcr_homelocation_count; 
ALTER TABLE datazonesgcr_homelocation_count_1
RENAME TO datazonesgcr_homelocation_count;

create table datazonesgcr_homelocation_count_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2019_Mar" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2019_mar
FROM public.datazonesgcr_homelocation_count d;

DROP TABLE datazonesgcr_homelocation_count; 
ALTER TABLE datazonesgcr_homelocation_count_1
RENAME TO datazonesgcr_homelocation_count;

create table datazonesgcr_homelocation_count_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2019_Apr" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2019_apr
FROM public.datazonesgcr_homelocation_count d;

DROP TABLE datazonesgcr_homelocation_count; 
ALTER TABLE datazonesgcr_homelocation_count_1
RENAME TO datazonesgcr_homelocation_count;

create table datazonesgcr_homelocation_count_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2019_May" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2019_may
FROM public.datazonesgcr_homelocation_count d;

DROP TABLE datazonesgcr_homelocation_count; 
ALTER TABLE datazonesgcr_homelocation_count_1
RENAME TO datazonesgcr_homelocation_count;

create table datazonesgcr_homelocation_count_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2019_Jun" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2019_jun
FROM public.datazonesgcr_homelocation_count d;

DROP TABLE datazonesgcr_homelocation_count; 
ALTER TABLE datazonesgcr_homelocation_count_1
RENAME TO datazonesgcr_homelocation_count;

create table datazonesgcr_homelocation_count_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2019_Jul" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2019jul
FROM public.datazonesgcr_homelocation_count d;

DROP TABLE datazonesgcr_homelocation_countd; 
ALTER TABLE datazonesgcr_homelocation_count_1
RENAME TO datazonesgcr_homelocation_count;

create table datazonesgcr_homelocation_count_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2019_Aug" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2019_aug
FROM public.datazonesgcr_homelocation_count d;

DROP TABLE datazonesgcr_homelocation_count; 
ALTER TABLE datazonesgcr_homelocation_count_1
RENAME TO datazonesgcr_homelocation_count;

create table datazonesgcr_homelocation_count_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2019_Sep" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2019_sep
FROM public.datazonesgcr_homelocation_count d;

DROP TABLE datazonesgcr_homelocation_count; 
ALTER TABLE datazonesgcr_homelocation_count_1
RENAME TO datazonesgcr_homelocation_count;

create table datazonesgcr_homelocation_count_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2019_Oct" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2019_oct
FROM public.datazonesgcr_homelocation_count d;

DROP TABLE datazonesgcr_homelocation_count; 
ALTER TABLE datazonesgcr_homelocation_count_1
RENAME TO datazonesgcr_homelocation_count;

create table datazonesgcr_homelocation_count_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2019_Nov" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2019_nov
FROM public.datazonesgcr_homelocation_count d;

DROP TABLE datazonesgcr_homelocation_count; 
ALTER TABLE datazonesgcr_homelocation_count_1
RENAME TO datazonesgcr_homelocation_count;

create table datazonesgcr_homelocation_count_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2019_Dec" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2019_dec
FROM public.datazonesgcr_homelocation_count d;

DROP TABLE datazonesgcr_homelocation_count; 
ALTER TABLE datazonesgcr_homelocation_count_1
RENAME TO datazonesgcr_homelocation_count;

create table datazonesgcr_homelocation_count_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2020_Jan" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2020_jan
FROM public.datazonesgcr_homelocation_count d;

DROP TABLE datazonesgcr_homelocation_count; 
ALTER TABLE datazonesgcr_homelocation_count_1
RENAME TO datazonesgcr_homelocation_count;

create table datazonesgcr_homelocation_count_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2020_Feb" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2020_feb
FROM public.datazonesgcr_homelocation_count d;

DROP TABLE datazonesgcr_homelocation_count; 
ALTER TABLE datazonesgcr_homelocation_count_1
RENAME TO datazonesgcr_homelocation_count;

create table datazonesgcr_homelocation_count_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2020_Mar" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2020_mar
FROM public.datazonesgcr_homelocation_count d;

DROP TABLE datazonesgcr_homelocation_count; 
ALTER TABLE datazonesgcr_homelocation_count_1
RENAME TO datazonesgcr_homelocation_count;

create table datazonesgcr_homelocation_count_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2020_Apr" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2020_apr
FROM public.datazonesgcr_homelocation_count d;

DROP TABLE datazonesgcr_homelocation_count; 
ALTER TABLE datazonesgcr_homelocation_count_1
RENAME TO datazonesgcr_homelocation_count;

create table datazonesgcr_homelocation_count_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2020_May" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2020_may
FROM public.datazonesgcr_homelocation_count d;

DROP TABLE datazonesgcr_homelocation_count; 
ALTER TABLE datazonesgcr_homelocation_count_1
RENAME TO datazonesgcr_homelocation_count;

create table datazonesgcr_homelocation_count_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2020_Jun" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2020_jun
FROM public.datazonesgcr_homelocation_count d;

DROP TABLE datazonesgcr_homelocation_count; 
ALTER TABLE datazonesgcr_homelocation_count_1
RENAME TO datazonesgcr_homelocation_count;

create table datazonesgcr_homelocation_count_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2020_Jul" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2020_jul
FROM public.datazonesgcr_homelocation_count d;

DROP TABLE datazonesgcr_homelocation_count; 
ALTER TABLE datazonesgcr_homelocation_count_1
RENAME TO datazonesgcr_homelocation_count;

create table datazonesgcr_homelocation_count_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2020_Aug" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2020_aug
FROM public.datazonesgcr_homelocation_count d;

DROP TABLE datazonesgcr_homelocation_count; 
ALTER TABLE datazonesgcr_homelocation_count_1
RENAME TO datazonesgcr_homelocation_count;

create table datazonesgcr_homelocation_count_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2020_Sep" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2020_sep
FROM public.datazonesgcr_homelocation_count d;

DROP TABLE datazonesgcr_homelocation_count; 
ALTER TABLE datazonesgcr_homelocation_count_1
RENAME TO datazonesgcr_homelocation_count;

create table datazonesgcr_homelocation_count_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2020_Oct" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2020_oct
FROM public.datazonesgcr_homelocation_count d;

DROP TABLE datazonesgcr_homelocation_count; 
ALTER TABLE datazonesgcr_homelocation_count_1
RENAME TO datazonesgcr_homelocation_count;

create table datazonesgcr_homelocation_count_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2020_Nov" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2020_nov
FROM public.datazonesgcr_homelocation_count d;

DROP TABLE datazonesgcr_homelocation_count; 
ALTER TABLE datazonesgcr_homelocation_count_1
RENAME TO datazonesgcr_homelocation_count;

create table datazonesgcr_homelocation_count_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2020_Dec" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2020_dec
FROM public.datazonesgcr_homelocation_count d;

DROP TABLE datazonesgcr_homelocation_count; 
ALTER TABLE datazonesgcr_homelocation_count_1
RENAME TO datazonesgcr_homelocation_count;

create table datazonesgcr_homelocation_count_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2021_Jan" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2021_jan
FROM public.datazonesgcr_homelocation_count d;

DROP TABLE datazonesgcr_homelocation_count; 
ALTER TABLE datazonesgcr_homelocation_count_1
RENAME TO datazonesgcr_homelocation_count;

create table datazonesgcr_homelocation_count_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2021_Feb" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2021_feb
FROM public.datazonesgcr_homelocation_count d;

DROP TABLE datazonesgcr_homelocation_count; 
ALTER TABLE datazonesgcr_homelocation_count_1
RENAME TO datazonesgcr_homelocation_count;

create table datazonesgcr_homelocation_count_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2021_Mar" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2021_mar
FROM public.datazonesgcr_homelocation_count d;

DROP TABLE datazonesgcr_homelocation_count; 
ALTER TABLE datazonesgcr_homelocation_count_1
RENAME TO datazonesgcr_homelocation_count;

create table datazonesgcr_homelocation_count_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2021_Apr" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2021_apr
FROM public.datazonesgcr_homelocation_count d;

DROP TABLE datazonesgcr_homelocation_count; 
ALTER TABLE datazonesgcr_homelocation_count_1
RENAME TO datazonesgcr_homelocation_count;

create table datazonesgcr_homelocation_count_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2021_May" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2021_may
FROM public.datazonesgcr_homelocation_count d;

DROP TABLE datazonesgcr_homelocation_count; 
ALTER TABLE datazonesgcr_homelocation_count_1
RENAME TO datazonesgcr_homelocation_count;

create table datazonesgcr_homelocation_count_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2021_Jun" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2021_jun
FROM public.datazonesgcr_homelocation_count d;

DROP TABLE datazonesgcr_homelocation_count; 
ALTER TABLE datazonesgcr_homelocation_count_1
RENAME TO datazonesgcr_homelocation_count;

create table datazonesgcr_homelocation_count_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2021_Jul" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2021_jul
FROM public.datazonesgcr_homelocation_count d;

DROP TABLE datazonesgcr_homelocation_count; 
ALTER TABLE datazonesgcr_homelocation_count_1
RENAME TO datazonesgcr_homelocation_count;

create table datazonesgcr_homelocation_count_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2021_Aug" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2021_aug
FROM public.datazonesgcr_homelocation_count d;

DROP TABLE datazonesgcr_homelocation_count; 
ALTER TABLE datazonesgcr_homelocation_count_1
RENAME TO datazonesgcr_homelocation_count;

create table datazonesgcr_homelocation_count_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2021_Sep" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2021_sep
FROM public.datazonesgcr_homelocation_count d;

DROP TABLE datazonesgcr_homelocation_count; 
ALTER TABLE datazonesgcr_homelocation_count_1
RENAME TO datazonesgcr_homelocation_count;

create table datazonesgcr_homelocation_count_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2021_Oct" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2021_oct
FROM public.datazonesgcr_homelocation_count d;

DROP TABLE datazonesgcr_homelocation_count; 
ALTER TABLE datazonesgcr_homelocation_count_1
RENAME TO datazonesgcr_homelocation_count;

create table datazonesgcr_homelocation_count_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2021_Nov" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2021_nov
FROM public.datazonesgcr_homelocation_count d;

DROP TABLE datazonesgcr_homelocation_count; 
ALTER TABLE datazonesgcr_homelocation_count_1
RENAME TO datazonesgcr_homelocation_count;

create table datazonesgcr_homelocation_count_1 as
SELECT 
   d.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2021_Dec" h 
   WHERE ST_Contains(d.geom4326, h.geom)) AS tamoco2021_dec
FROM public.datazonesgcr_homelocation_count d;

DROP TABLE datazonesgcr_homelocation_count; 
ALTER TABLE datazonesgcr_homelocation_count_1
RENAME TO datazonesgcr_homelocation_count;


--2.7.2 Postcode (CACI)
--2.7.2.1 Huq
create table caci_homelocation_count as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."homelocations_huq_2019_subset" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS huq2019
FROM public.caci_homelocation_count c

create table caci_homelocation_count_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."homelocations_huq_2020_subset" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS huq2020
FROM public.caci_homelocation_count c

DROP TABLE caci_homelocation_count; 
ALTER TABLE caci_homelocation_count_1
RENAME TO caci_homelocation_count;

create table caci_homelocation_count_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."homelocations_huq_2021_subset" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS huq2021
FROM public.caci_homelocation_count c

DROP TABLE caci_homelocation_count; 
ALTER TABLE caci_homelocation_count_1
RENAME TO caci_homelocation_count;

--2.7.2.2 Tamoco (monthly because re-hashed monthly)
create table caci_homelocation_count_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2019_Jan" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2019_jan
FROM public.caci_homelocation_count c;

DROP TABLE caci_homelocation_count; 
ALTER TABLE caci_homelocation_count_1
RENAME TO caci_homelocation_count;

create table caci_homelocation_count_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2019_Feb" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2019_feb
FROM public.caci_homelocation_count c;

DROP TABLE caci_homelocation_count; 
ALTER TABLE caci_homelocation_count_1
RENAME TO caci_homelocation_count;

create table caci_homelocation_count_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2019_Mar" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2019_mar
FROM public.caci_homelocation_count c;

DROP TABLE caci_homelocation_count; 
ALTER TABLE caci_homelocation_count_1
RENAME TO caci_homelocation_count;

create table caci_homelocation_count_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2019_Apr" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2019_apr
FROM public.caci_homelocation_count c;

DROP TABLE caci_homelocation_count; 
ALTER TABLE caci_homelocation_count_1
RENAME TO caci_homelocation_count;

create table caci_homelocation_count_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2019_May" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2019_may
FROM public.caci_homelocation_count c;

DROP TABLE caci_homelocation_count; 
ALTER TABLE caci_homelocation_count_1
RENAME TO caci_homelocation_count;

create table caci_homelocation_count_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2019_Jun" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2019_jun
FROM public.caci_homelocation_count c;

DROP TABLE caci_homelocation_count; 
ALTER TABLE caci_homelocation_count_1
RENAME TO caci_homelocation_count;

create table caci_homelocation_count_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2019_Jul" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2019_jul
FROM public.caci_homelocation_coun c;

DROP TABLE caci_homelocation_count; 
ALTER TABLE caci_homelocation_count_1
RENAME TO caci_homelocation_count;

create table caci_homelocation_count_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2019_Aug" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2019_aug
FROM public.caci_homelocation_count c;

DROP TABLE caci_homelocation_count; 
ALTER TABLE caci_homelocation_count_1
RENAME TO caci_homelocation_count;

create table caci_homelocation_count_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2019_Sep" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2019_sep
FROM public.caci_homelocation_count c;

DROP TABLE caci_homelocation_count; 
ALTER TABLE caci_homelocation_count_1
RENAME TO caci_homelocation_count;

create table caci_homelocation_count_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2019_Oct" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2019_oct
FROM public.caci_homelocation_count c;

DROP TABLE caci_homelocation_count; 
ALTER TABLE caci_homelocation_count_1
RENAME TO caci_homelocation_count;

create table caci_homelocation_count_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2019_Nov" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2019_nov
FROM public.caci_homelocation_count c;

create table caci_homelocation_count_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2019_Dec" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2019_dec
FROM public.caci_homelocation_count c;

DROP TABLE caci_homelocation_count; 
ALTER TABLE caci_homelocation_count_1
RENAME TO caci_homelocation_count;

create table caci_homelocation_count_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2020_Jan" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2020_jan
FROM public.caci_homelocation_count c;

DROP TABLE caci_homelocation_count; 
ALTER TABLE caci_homelocation_count_1
RENAME TO caci_homelocation_count;

create table caci_homelocation_count_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2020_Feb" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2020_feb
FROM public.caci_homelocation_count c;

DROP TABLE caci_homelocation_count; 
ALTER TABLE caci_homelocation_count_1
RENAME TO caci_homelocation_count;

create table caci_homelocation_count_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2020_Mar" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2020_mar
FROM public.caci_homelocation_count c;

DROP TABLE caci_homelocation_count; 
ALTER TABLE caci_homelocation_count_1
RENAME TO caci_homelocation_count;

create table caci_homelocation_count_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2020_Apr" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2020_apr
FROM public.caci_homelocation_count c;

DROP TABLE caci_homelocation_count; 
ALTER TABLE caci_homelocation_count_1
RENAME TO caci_homelocation_count;

create table caci_homelocation_count_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2020_May" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2020_may
FROM public.caci_homelocation_count c;

DROP TABLE caci_homelocation_count; 
ALTER TABLE caci_homelocation_count_1
RENAME TO caci_homelocation_count;

create table caci_homelocation_count_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2020_Jun" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2020_jun
FROM public.caci_homelocation_count c;

DROP TABLE caci_homelocation_count; 
ALTER TABLE caci_homelocation_count_1
RENAME TO caci_homelocation_count;

create table caci_homelocation_count_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2020_Jul" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2020_jul
FROM public.caci_homelocation_count c;

DROP TABLE caci_homelocation_count; 
ALTER TABLE caci_homelocation_countd_1
RENAME TO caci_homelocation_count;

create table caci_homelocation_count_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2020_Aug" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2020_aug
FROM public.caci_homelocation_count c;

DROP TABLE caci_homelocation_count; 
ALTER TABLE caci_homelocation_count_1
RENAME TO caci_homelocation_count;

create table caci_homelocation_count_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2020_Sep" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2020_sep
FROM public.caci_homelocation_count c;

DROP TABLE caci_homelocation_count; 
ALTER TABLE caci_homelocation_count_1
RENAME TO caci_homelocation_count;

create table caci_homelocation_count_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2020_Oct" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2020_oct
FROM public.caci_homelocation_count c;

DROP TABLE caci_homelocation_count; 
ALTER TABLE caci_homelocation_count_1
RENAME TO caci_homelocation_count;

create table caci_homelocation_count_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2020_Nov" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2020_nov
FROM public.caci_homelocation_count c;

DROP TABLE caci_homelocation_count; 
ALTER TABLE caci_homelocation_count_1
RENAME TO caci_homelocation_count;

create table caci_homelocation_count_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2020_Dec" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2020_dec
FROM public.caci_homelocation_count c;

DROP TABLE caci_homelocation_count; 
ALTER TABLE caci_homelocation_count_1
RENAME TO caci_homelocation_count;

create table caci_homelocation_count_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2021_Jan" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2021_jan
FROM public.caci_homelocation_count c;

DROP TABLE caci_homelocation_count; 
ALTER TABLE caci_homelocation_count_1
RENAME TO caci_homelocation_count;

create table caci_homelocation_count_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2021_Feb" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2021_feb
FROM public.caci_homelocation_count c;

DROP TABLE caci_homelocation_count; 
ALTER TABLE caci_homelocation_count_1
RENAME TO caci_homelocation_count;

create table caci_homelocation_count_std_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2021_Mar" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2021_mar
FROM public.caci_homelocation_count c;

DROP TABLE caci_homelocation_count; 
ALTER TABLE caci_homelocation_count_1
RENAME TO caci_homelocation_count;

create table caci_homelocation_count_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2020_Apr" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2020_apr
FROM public.caci_homelocation_count c;

DROP TABLE caci_homelocation_count; 
ALTER TABLE caci_homelocation_count_1
RENAME TO caci_homelocation_count;

create table caci_homelocation_count_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2021_May" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2021_may
FROM public.caci_homelocation_count c;

DROP TABLE caci_homelocation_count; 
ALTER TABLE caci_homelocation_count_1
RENAME TO caci_homelocation_count;

create table caci_homelocation_count_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2021_Jun" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2021_jun
FROM public.caci_homelocation_count c;

DROP TABLE caci_homelocation_count; 
ALTER TABLE caci_homelocation_count_1
RENAME TO caci_homelocation_count;

create table caci_homelocation_count_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2021_Jul" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2021_jul
FROM public.caci_homelocation_count c;

DROP TABLE caci_homelocation_count; 
ALTER TABLE caci_homelocation_count_1
RENAME TO caci_homelocation_count;

create table caci_homelocation_count_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2021_Aug" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2021_aug
FROM public.caci_homelocation_count c;

DROP TABLE caci_homelocation_count; 
ALTER TABLE caci_homelocation_count_1
RENAME TO caci_homelocation_count;

create table caci_homelocation_count_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2021_Sep" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2021_sep
FROM public.caci_homelocation_count c;

DROP TABLE caci_homelocation_count; 
ALTER TABLE caci_homelocation_count_1
RENAME TO caci_homelocation_count;

create table caci_homelocation_count_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2021_Oct" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2021_oct
FROM public.caci_homelocation_count c;

DROP TABLE caci_homelocation_count; 
ALTER TABLE caci_homelocation_count_1
RENAME TO caci_homelocation_count;

create table caci_homelocation_count_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2021_Nov" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2021_nov
FROM public.caci_homelocation_count c;

DROP TABLE caci_homelocation_count; 
ALTER TABLE caci_homelocation_countd_1
RENAME TO caci_homelocation_count;

create table caci_homelocation_count_1 as
SELECT 
   c.*,
  (SELECT count(h.geom) FROM public."homelocations_tamoco_2021_Dec" h 
   WHERE ST_Contains(c.geom4326, h.geom)) AS tamoco2021_dec
FROM public.caci_homelocation_count c;

DROP TABLE caci_homelocation_count; 
ALTER TABLE caci_homelocation_count_1
RENAME TO caci_homelocation_count;

--2.7 Geographic analysis
--2.7.1 Sum populations in each council area
SELECT "council_area" as council_area, sum(adultpop_2020) as adult_pop_2020, 
sum(huq2019) as huq2019, sum(huq2020) as huq2020, sum(huq2021) as huq2021, 
sum(tamoco2019_jan) as tamoco2019_jan, sum(tamoco2019_feb) as tamoco2019_feb, sum(tamoco2019_mar) as tamoco2019_mar, sum(tamoco2019_apr) as tamoco2019_apr,
sum(tamoco2019_may) as tamoco2019_may, sum(tamoco2019_jun) as tamoco2019_jun, sum(tamoco2019_jul) as tamoco2019_jul, sum(tamoco2019_aug) as tamoco2019_aug, sum(tamoco2019_sep) as tamoco2019_sep, 
sum(tamoco2019_oct) as tamoco2019_oct, sum(tamoco2019_nov) as tamoco2019_nov, sum(tamoco2019_dec) as tamoco2019_dec,
sum(tamoco2020_jan) as tamoco2020_jan, sum(tamoco2020_feb) as tamoco2020_feb, sum(tamoco2020_mar) as tamoco2020_mar, sum(tamoco2020_apr) as tamoco2020_apr,
sum(tamoco2020_may) as tamoco2020_may, sum(tamoco2020_jun) as tamoco2020_jun, sum(tamoco2020_jul) as tamoco2020_jul, sum(tamoco2020_aug) as tamoco2020_aug, sum(tamoco2020_sep) as tamoco2020_sep, 
sum(tamoco2020_oct) as tamoco2020_oct, sum(tamoco2020_nov) as tamoco2020_nov, sum(tamoco2020_dec) as tamoco2020_dec,
sum(tamoco2021_jan) as tamoco2021_jan, sum(tamoco2021_feb) as tamoco2021_feb, sum(tamoco2021_mar) as tamoco2021_mar, sum(tamoco2021_apr) as tamoco2021_apr,
sum(tamoco2021_may) as tamoco2021_may, sum(tamoco2021_jun) as tamoco2021_jun, sum(tamoco2021_jul) as tamoco2021_jul, sum(tamoco2021_aug) as tamoco2021_aug, sum(tamoco2021_sep) as tamoco2021_sep, 
sum(tamoco2021_oct) as tamoco2021_oct, sum(tamoco2021_nov) as tamoco2021_nov, sum(tamoco2021_dec) as tamoco2021_dec
FROM datazonesgcr_homelocation_count
GROUP BY "council_area";


--1.7.2 Sum populations in each Intermediate zone
SELECT "intermediate_zone" as intermediate_area, sum(adultpop_2020) as adult_pop_2020, 
sum(huq2019) as huq2019, sum(huq2020) as huq2020, sum(huq2021) as huq2021, 
sum(tamoco2019_jan) as tamoco2019_jan, sum(tamoco2019_feb) as tamoco2019_feb, sum(tamoco2019_mar) as tamoco2019_mar, sum(tamoco2019_apr) as tamoco2019_apr,
sum(tamoco2019_may) as tamoco2019_may, sum(tamoco2019_jun) as tamoco2019_jun, sum(tamoco2019_jul) as tamoco2019_jul, sum(tamoco2019_aug) as tamoco2019_aug, sum(tamoco2019_sep) as tamoco2019_sep, 
sum(tamoco2019_oct) as tamoco2019_oct, sum(tamoco2019_nov) as tamoco2019_nov, sum(tamoco2019_dec) as tamoco2019_dec,
sum(tamoco2020_jan) as tamoco2020_jan, sum(tamoco2020_feb) as tamoco2020_feb, sum(tamoco2020_mar) as tamoco2020_mar, sum(tamoco2020_apr) as tamoco2020_apr,
sum(tamoco2020_may) as tamoco2020_may, sum(tamoco2020_jun) as tamoco2020_jun, sum(tamoco2020_jul) as tamoco2020_jul, sum(tamoco2020_aug) as tamoco2020_aug, sum(tamoco2020_sep) as tamoco2020_sep, 
sum(tamoco2020_oct) as tamoco2020_oct, sum(tamoco2020_nov) as tamoco2020_nov, sum(tamoco2020_dec) as tamoco2020_dec,
sum(tamoco2021_jan) as tamoco2021_jan, sum(tamoco2021_feb) as tamoco2021_feb, sum(tamoco2021_mar) as tamoco2021_mar, sum(tamoco2021_apr) as tamoco2021_apr,
sum(tamoco2021_may) as tamoco2021_may, sum(tamoco2021_jun) as tamoco2021_jun, sum(tamoco2021_jul) as tamoco2021_jul, sum(tamoco2021_aug) as tamoco2021_aug, sum(tamoco2021_sep) as tamoco2021_sep, 
sum(tamoco2021_oct) as tamoco2021_oct, sum(tamoco2021_nov) as tamoco2021_nov, sum(tamoco2021_dec) as tamoco2021_dec
FROM datazonesgcr_homelocation_count
GROUP BY "intermediate_zone";

--2.8 Sociodemographic analysis accross study area
--2.8.1 SIMD
--2.8.1.1 Mobile phone population by SIMD percentile
SELECT "simd2020_withinds_SIMD_2020v2_Percentile" as simd_percentile, sum(adultpop_2020) as adult_pop_2020, 
sum(huq2019) as huq2019, sum(huq2020) as huq2020, sum(huq2021) as huq2021, 
sum(tamoco2019_jan) as tamoco2019_jan, sum(tamoco2019_feb) as tamoco2019_feb, sum(tamoco2019_mar) as tamoco2019_mar, sum(tamoco2019_apr) as tamoco2019_apr,
sum(tamoco2019_may) as tamoco2019_may, sum(tamoco2019_jun) as tamoco2019_jun, sum(tamoco2019_jul) as tamoco2019_jul, sum(tamoco2019_aug) as tamoco2019_aug, sum(tamoco2019_sep) as tamoco2019_sep, 
sum(tamoco2019_oct) as tamoco2019_oct, sum(tamoco2019_nov) as tamoco2019_nov, sum(tamoco2019_dec) as tamoco2019_dec,
sum(tamoco2020_jan) as tamoco2020_jan, sum(tamoco2020_feb) as tamoco2020_feb, sum(tamoco2020_mar) as tamoco2020_mar, sum(tamoco2020_apr) as tamoco2020_apr,
sum(tamoco2020_may) as tamoco2020_may, sum(tamoco2020_jun) as tamoco2020_jun, sum(tamoco2020_jul) as tamoco2020_jul, sum(tamoco2020_aug) as tamoco2020_aug, sum(tamoco2020_sep) as tamoco2020_sep, 
sum(tamoco2020_oct) as tamoco2020_oct, sum(tamoco2020_nov) as tamoco2020_nov, sum(tamoco2020_dec) as tamoco2020_dec,
sum(tamoco2021_jan) as tamoco2021_jan, sum(tamoco2021_feb) as tamoco2021_feb, sum(tamoco2021_mar) as tamoco2021_mar, sum(tamoco2021_apr) as tamoco2021_apr,
sum(tamoco2021_may) as tamoco2021_may, sum(tamoco2021_jun) as tamoco2021_jun, sum(tamoco2021_jul) as tamoco2021_jul, sum(tamoco2021_aug) as tamoco2021_aug, sum(tamoco2021_sep) as tamoco2021_sep, 
sum(tamoco2021_oct) as tamoco2021_oct, sum(tamoco2021_nov) as tamoco2021_nov, sum(tamoco2021_dec) as tamoco2021_dec
FROM datazonesgcr_homelocation_count
GROUP BY "simd2020_withinds_SIMD_2020v2_Percentile";



--2.8.1.2 Mobile phone population by SIMD decile
SELECT "simd2020_withinds_SIMD2020v2_Decile" as simd_decile, sum(adultpop_2020) as adult_pop_2020, 
sum(huq2019) as huq2019, sum(huq2020) as huq2020, sum(huq2021) as huq2021, 
sum(tamoco2019_jan) as tamoco2019_jan, sum(tamoco2019_feb) as tamoco2019_feb, sum(tamoco2019_mar) as tamoco2019_mar, sum(tamoco2019_apr) as tamoco2019_apr,
sum(tamoco2019_may) as tamoco2019_may, sum(tamoco2019_jun) as tamoco2019_jun, sum(tamoco2019_jul) as tamoco2019_jul, sum(tamoco2019_aug) as tamoco2019_aug, sum(tamoco2019_sep) as tamoco2019_sep, 
sum(tamoco2019_oct) as tamoco2019_oct, sum(tamoco2019_nov) as tamoco2019_nov, sum(tamoco2019_dec) as tamoco2019_dec,
sum(tamoco2020_jan) as tamoco2020_jan, sum(tamoco2020_feb) as tamoco2020_feb, sum(tamoco2020_mar) as tamoco2020_mar, sum(tamoco2020_apr) as tamoco2020_apr,
sum(tamoco2020_may) as tamoco2020_may, sum(tamoco2020_jun) as tamoco2020_jun, sum(tamoco2020_jul) as tamoco2020_jul, sum(tamoco2020_aug) as tamoco2020_aug, sum(tamoco2020_sep) as tamoco2020_sep, 
sum(tamoco2020_oct) as tamoco2020_oct, sum(tamoco2020_nov) as tamoco2020_nov, sum(tamoco2020_dec) as tamoco2020_dec,
sum(tamoco2021_jan) as tamoco2021_jan, sum(tamoco2021_feb) as tamoco2021_feb, sum(tamoco2021_mar) as tamoco2021_mar, sum(tamoco2021_apr) as tamoco2021_apr,
sum(tamoco2021_may) as tamoco2021_may, sum(tamoco2021_jun) as tamoco2021_jun, sum(tamoco2021_jul) as tamoco2021_jul, sum(tamoco2021_aug) as tamoco2021_aug, sum(tamoco2021_sep) as tamoco2021_sep, 
sum(tamoco2021_oct) as tamoco2021_oct, sum(tamoco2021_nov) as tamoco2021_nov, sum(tamoco2021_dec) as tamoco2021_dec
FROM datazonesgcr_homelocation_count
GROUP BY "simd2020_withinds_SIMD2020v2_Decile";

--1.8.1.3 Mobile phone population by SIMD quintile
SELECT "simd2020_withinds_SIMD2020v2_Quintile" as simd_quintile, sum(adultpop_2020) as adult_pop_2020, 
sum(huq2019) as huq2019, sum(huq2020) as huq2020, sum(huq2021) as huq2021, 
sum(tamoco2019_jan) as tamoco2019_jan, sum(tamoco2019_feb) as tamoco2019_feb, sum(tamoco2019_mar) as tamoco2019_mar, sum(tamoco2019_apr) as tamoco2019_apr,
sum(tamoco2019_may) as tamoco2019_may, sum(tamoco2019_jun) as tamoco2019_jun, sum(tamoco2019_jul) as tamoco2019_jul, sum(tamoco2019_aug) as tamoco2019_aug, sum(tamoco2019_sep) as tamoco2019_sep, 
sum(tamoco2019_oct) as tamoco2019_oct, sum(tamoco2019_nov) as tamoco2019_nov, sum(tamoco2019_dec) as tamoco2019_dec,
sum(tamoco2020_jan) as tamoco2020_jan, sum(tamoco2020_feb) as tamoco2020_feb, sum(tamoco2020_mar) as tamoco2020_mar, sum(tamoco2020_apr) as tamoco2020_apr,
sum(tamoco2020_may) as tamoco2020_may, sum(tamoco2020_jun) as tamoco2020_jun, sum(tamoco2020_jul) as tamoco2020_jul, sum(tamoco2020_aug) as tamoco2020_aug, sum(tamoco2020_sep) as tamoco2020_sep, 
sum(tamoco2020_oct) as tamoco2020_oct, sum(tamoco2020_nov) as tamoco2020_nov, sum(tamoco2020_dec) as tamoco2020_dec,
sum(tamoco2021_jan) as tamoco2021_jan, sum(tamoco2021_feb) as tamoco2021_feb, sum(tamoco2021_mar) as tamoco2021_mar, sum(tamoco2021_apr) as tamoco2021_apr,
sum(tamoco2021_may) as tamoco2021_may, sum(tamoco2021_jun) as tamoco2021_jun, sum(tamoco2021_jul) as tamoco2021_jul, sum(tamoco2021_aug) as tamoco2021_aug, sum(tamoco2021_sep) as tamoco2021_sep, 
sum(tamoco2021_oct) as tamoco2021_oct, sum(tamoco2021_nov) as tamoco2021_nov, sum(tamoco2021_dec) as tamoco2021_dec
FROM datazonesgcr_homelocation_count
GROUP BY "simd2020_withinds_SIMD2020v2_Quintile";

--2.8.2 CACI
--2.8.2.1 Mobile phone population by CACI category
SELECT category as caci_category, sum(huq2019) as huq2019, sum(huq2020) as huq2020, sum(huq2021) as huq2021, sum(tamoco2019_jan) as tamoco2019_jan, sum(tamoco2019_feb) as tamoco2019_feb, sum(tamoco2019_mar) as tamoco2019_mar, sum(tamoco2019_apr) as tamoco2019_apr,sum(tamoco2019_may) as tamoco2019_may, sum(tamoco2019_jun) as tamoco2019_jun, sum(tamoco2019_jul) as tamoco2019_jul, sum(tamoco2019_aug) as tamoco2019_aug, sum(tamoco2019_sep) as tamoco2019_sep, 
sum(tamoco2019_oct) as tamoco2019_oct, sum(tamoco2019_nov) as tamoco2019_nov, sum(tamoco2019_dec) as tamoco2019_dec,
sum(tamoco2020_jan) as tamoco2020_jan, sum(tamoco2020_feb) as tamoco2020_feb, sum(tamoco2020_mar) as tamoco2020_mar, sum(tamoco2020_apr) as tamoco2020_apr,
sum(tamoco2020_may) as tamoco2020_may, sum(tamoco2020_jun) as tamoco2020_jun, sum(tamoco2020_jul) as tamoco2020_jul, sum(tamoco2020_aug) as tamoco2020_aug, sum(tamoco2020_sep) as tamoco2020_sep, 
sum(tamoco2020_oct) as tamoco2020_oct, sum(tamoco2020_nov) as tamoco2020_nov, sum(tamoco2020_dec) as tamoco2020_dec,
sum(tamoco2021_jan) as tamoco2021_jan, sum(tamoco2021_feb) as tamoco2021_feb, sum(tamoco2021_mar) as tamoco2021_mar, sum(tamoco2021_apr) as tamoco2021_apr,
sum(tamoco2021_may) as tamoco2021_may, sum(tamoco2021_jun) as tamoco2021_jun, sum(tamoco2021_jul) as tamoco2021_jul, sum(tamoco2021_aug) as tamoco2021_aug, sum(tamoco2021_sep) as tamoco2021_sep, 
sum(tamoco2021_oct) as tamoco2021_oct, sum(tamoco2021_nov) as tamoco2021_nov, sum(tamoco2021_dec) as tamoco2021_dec, sum(population) as population
FROM caci_homelocation_count
GROUP BY category;


--1.8.2.2 Mobile phone population by CACI group
SELECT "group" as caci_group,sum(huq2019) as huq2019, sum(huq2020) as huq2020, sum(huq2021) as huq2021, sum(tamoco2019_jan) as tamoco2019_jan, sum(tamoco2019_feb) as tamoco2019_feb, sum(tamoco2019_mar) as tamoco2019_mar, sum(tamoco2019_apr) as tamoco2019_apr, sum(tamoco2019_may) as tamoco2019_may, sum(tamoco2019_jun) as tamoco2019_jun, sum(tamoco2019_jul) as tamoco2019_jul, sum(tamoco2019_aug) as tamoco2019_aug, sum(tamoco2019_sep) as tamoco2019_sep, 
sum(tamoco2019_oct) as tamoco2019_oct, sum(tamoco2019_nov) as tamoco2019_nov, sum(tamoco2019_dec) as tamoco2019_dec,
sum(tamoco2020_jan) as tamoco2020_jan, sum(tamoco2020_feb) as tamoco2020_feb, sum(tamoco2020_mar) as tamoco2020_mar, sum(tamoco2020_apr) as tamoco2020_apr,
sum(tamoco2020_may) as tamoco2020_may, sum(tamoco2020_jun) as tamoco2020_jun, sum(tamoco2020_jul) as tamoco2020_jul, sum(tamoco2020_aug) as tamoco2020_aug, sum(tamoco2020_sep) as tamoco2020_sep, 
sum(tamoco2020_oct) as tamoco2020_oct, sum(tamoco2020_nov) as tamoco2020_nov, sum(tamoco2020_dec) as tamoco2020_dec,
sum(tamoco2021_jan) as tamoco2021_jan, sum(tamoco2021_feb) as tamoco2021_feb, sum(tamoco2021_mar) as tamoco2021_mar, sum(tamoco2021_apr) as tamoco2021_apr,
sum(tamoco2021_may) as tamoco2021_may, sum(tamoco2021_jun) as tamoco2021_jun, sum(tamoco2021_jul) as tamoco2021_jul, sum(tamoco2021_aug) as tamoco2021_aug, sum(tamoco2021_sep) as tamoco2021_sep, 
sum(tamoco2021_oct) as tamoco2021_oct, sum(tamoco2021_nov) as tamoco2021_nov, sum(tamoco2021_dec) as tamoco2021_dec, sum(population) as population
FROM caci_homelocation_count
GROUP BY "group";

--1.8.2.3 Mobile phone population by CACI type
SELECT "type" as caci_type,sum(huq2019) as huq2019, sum(huq2020) as huq2020, sum(huq2021) as huq2021, sum(tamoco2019_jan) as tamoco2019_jan, sum(tamoco2019_feb) as tamoco2019_feb, sum(tamoco2019_mar) as tamoco2019_mar, sum(tamoco2019_apr) as tamoco2019_apr, sum(tamoco2019_may) as tamoco2019_may, sum(tamoco2019_jun) as tamoco2019_jun, sum(tamoco2019_jul) as tamoco2019_jul, sum(tamoco2019_aug) as tamoco2019_aug, sum(tamoco2019_sep) as tamoco2019_sep, 
sum(tamoco2019_oct) as tamoco2019_oct, sum(tamoco2019_nov) as tamoco2019_nov, sum(tamoco2019_dec) as tamoco2019_dec,
sum(tamoco2020_jan) as tamoco2020_jan, sum(tamoco2020_feb) as tamoco2020_feb, sum(tamoco2020_mar) as tamoco2020_mar, sum(tamoco2020_apr) as tamoco2020_apr,
sum(tamoco2020_may) as tamoco2020_may, sum(tamoco2020_jun) as tamoco2020_jun, sum(tamoco2020_jul) as tamoco2020_jul, sum(tamoco2020_aug) as tamoco2020_aug, sum(tamoco2020_sep) as tamoco2020_sep, 
sum(tamoco2020_oct) as tamoco2020_oct, sum(tamoco2020_nov) as tamoco2020_nov, sum(tamoco2020_dec) as tamoco2020_dec,
sum(tamoco2021_jan) as tamoco2021_jan, sum(tamoco2021_feb) as tamoco2021_feb, sum(tamoco2021_mar) as tamoco2021_mar, sum(tamoco2021_apr) as tamoco2021_apr,
sum(tamoco2021_may) as tamoco2021_may, sum(tamoco2021_jun) as tamoco2021_jun, sum(tamoco2021_jul) as tamoco2021_jul, sum(tamoco2021_aug) as tamoco2021_aug, sum(tamoco2021_sep) as tamoco2021_sep, 
sum(tamoco2021_oct) as tamoco2021_oct, sum(tamoco2021_nov) as tamoco2021_nov, sum(tamoco2021_dec) as tamoco2021_dec, sum(population) as population
FROM caci_homelocation_count
GROUP BY "type";

--2.9 Sociodemographic analysis within councils
--2.9.0 Codes:
'S12000029' = 'South Lanarkshire'
'S12000049' = 'Glasgow City'
'S12000045' = 'East Dunbartonshire'	
'S12000018' = 'Inverclyde'
'S12000038' = 'Renfrewshire'	
'S12000050' = 'North Lanarkshire'	
'S12000039' = 'West Dunbartonshire'
'S12000011' = 'East Renfrewshire'	

--2.9.1 SIMD
--2.9.1.1 Mobile phone populations by SIMD percentile for each council
SELECT "simd2020_withinds_SIMD_2020v2_Percentile" as simd_percentile, sum(adultpop_2020) as adult_pop_2020, sum(huq2019) as huq2019, sum(huq2020) as huq2020, sum(huq2021) as huq2021, sum(tamoco2019_jan) as tamoco2019_jan, sum(tamoco2019_feb) as tamoco2019_feb, sum(tamoco2019_mar) as tamoco2019_mar, sum(tamoco2019_apr) as tamoco2019_apr,
sum(tamoco2019_may) as tamoco2019_may, sum(tamoco2019_jun) as tamoco2019_jun, sum(tamoco2019_jul) as tamoco2019_jul, sum(tamoco2019_aug) as tamoco2019_aug, sum(tamoco2019_sep) as tamoco2019_sep, 
sum(tamoco2019_oct) as tamoco2019_oct, sum(tamoco2019_nov) as tamoco2019_nov, sum(tamoco2019_dec) as tamoco2019_dec,
sum(tamoco2020_jan) as tamoco2020_jan, sum(tamoco2020_feb) as tamoco2020_feb, sum(tamoco2020_mar) as tamoco2020_mar, sum(tamoco2020_apr) as tamoco2020_apr,
sum(tamoco2020_may) as tamoco2020_may, sum(tamoco2020_jun) as tamoco2020_jun, sum(tamoco2020_jul) as tamoco2020_jul, sum(tamoco2020_aug) as tamoco2020_aug, sum(tamoco2020_sep) as tamoco2020_sep, 
sum(tamoco2020_oct) as tamoco2020_oct, sum(tamoco2020_nov) as tamoco2020_nov, sum(tamoco2020_dec) as tamoco2020_dec,
sum(tamoco2021_jan) as tamoco2021_jan, sum(tamoco2021_feb) as tamoco2021_feb, sum(tamoco2021_mar) as tamoco2021_mar, sum(tamoco2021_apr) as tamoco2021_apr,
sum(tamoco2021_may) as tamoco2021_may, sum(tamoco2021_june) as tamoco2021_jun, sum(tamoco2021_jul) as tamoco2021_jul, sum(tamoco2021_aug) as tamoco2021_aug, sum(tamoco2021_sep) as tamoco2021_sep, 
sum(tamoco2021_oct) as tamoco2021_oct, sum(tamoco2021_nov) as tamoco2021_nov, sum(tamoco2021_dec) as tamoco2021_dec
FROM datazonesgcr_homelocation_count
WHERE council_area = 'South Lanarkshire'
GROUP BY "simd2020_withinds_SIMD_2020v2_Percentile";

SELECT "simd2020_withinds_SIMD_2020v2_Percentile" as simd_percentile, sum(adultpop_2020) as adult_pop_2020, sum(huq2019) as huq2019, sum(huq2020) as huq2020, sum(huq2021) as huq2021, sum(tamoco2019_jan) as tamoco2019_jan, sum(tamoco2019_feb) as tamoco2019_feb, sum(tamoco2019_mar) as tamoco2019_mar, sum(tamoco2019_apr) as tamoco2019_apr,
sum(tamoco2019_may) as tamoco2019_may, sum(tamoco2019_jun) as tamoco2019_jun, sum(tamoco2019_jul) as tamoco2019_jul, sum(tamoco2019_aug) as tamoco2019_aug, sum(tamoco2019_sep) as tamoco2019_sep, 
sum(tamoco2019_oct) as tamoco2019_oct, sum(tamoco2019_nov) as tamoco2019_nov, sum(tamoco2019_dec) as tamoco2019_dec,
sum(tamoco2020_jan) as tamoco2020_jan, sum(tamoco2020_feb) as tamoco2020_feb, sum(tamoco2020_mar) as tamoco2020_mar, sum(tamoco2020_apr) as tamoco2020_apr,
sum(tamoco2020_may) as tamoco2020_may, sum(tamoco2020_jun) as tamoco2020_jun, sum(tamoco2020_jul) as tamoco2020_jul, sum(tamoco2020_aug) as tamoco2020_aug, sum(tamoco2020_sep) as tamoco2020_sep, 
sum(tamoco2020_oct) as tamoco2020_oct, sum(tamoco2020_nov) as tamoco2020_nov, sum(tamoco2020_dec) as tamoco2020_dec,
sum(tamoco2021_jan) as tamoco2021_jan, sum(tamoco2021_feb) as tamoco2021_feb, sum(tamoco2021_mar) as tamoco2021_mar, sum(tamoco2021_apr) as tamoco2021_apr,
sum(tamoco2021_may) as tamoco2021_may, sum(tamoco2021_june) as tamoco2021_jun, sum(tamoco2021_jul) as tamoco2021_jul, sum(tamoco2021_aug) as tamoco2021_aug, sum(tamoco2021_sep) as tamoco2021_sep, 
sum(tamoco2021_oct) as tamoco2021_oct, sum(tamoco2021_nov) as tamoco2021_nov, sum(tamoco2021_dec) as tamoco2021_dec
FROM datazonesgcr_homelocation_count
WHERE council_area = 'Glasgow City'
GROUP BY "simd2020_withinds_SIMD_2020v2_Percentile";

SELECT "simd2020_withinds_SIMD_2020v2_Percentile" as simd_percentile, sum(adultpop_2020) as adult_pop_2020, sum(huq2019) as huq2019, sum(huq2020) as huq2020, sum(huq2021) as huq2021, sum(tamoco2019_jan) as tamoco2019_jan, sum(tamoco2019_feb) as tamoco2019_feb, sum(tamoco2019_mar) as tamoco2019_mar, sum(tamoco2019_apr) as tamoco2019_apr,
sum(tamoco2019_may) as tamoco2019_may, sum(tamoco2019_jun) as tamoco2019_jun, sum(tamoco2019_jul) as tamoco2019_jul, sum(tamoco2019_aug) as tamoco2019_aug, sum(tamoco2019_sep) as tamoco2019_sep, 
sum(tamoco2019_oct) as tamoco2019_oct, sum(tamoco2019_nov) as tamoco2019_nov, sum(tamoco2019_dec) as tamoco2019_dec,
sum(tamoco2020_jan) as tamoco2020_jan, sum(tamoco2020_feb) as tamoco2020_feb, sum(tamoco2020_mar) as tamoco2020_mar, sum(tamoco2020_apr) as tamoco2020_apr,
sum(tamoco2020_may) as tamoco2020_may, sum(tamoco2020_jun) as tamoco2020_jun, sum(tamoco2020_jul) as tamoco2020_jul, sum(tamoco2020_aug) as tamoco2020_aug, sum(tamoco2020_sep) as tamoco2020_sep, 
sum(tamoco2020_oct) as tamoco2020_oct, sum(tamoco2020_nov) as tamoco2020_nov, sum(tamoco2020_dec) as tamoco2020_dec,
sum(tamoco2021_jan) as tamoco2021_jan, sum(tamoco2021_feb) as tamoco2021_feb, sum(tamoco2021_mar) as tamoco2021_mar, sum(tamoco2021_apr) as tamoco2021_apr,
sum(tamoco2021_may) as tamoco2021_may, sum(tamoco2021_june) as tamoco2021_jun, sum(tamoco2021_jul) as tamoco2021_jul, sum(tamoco2021_aug) as tamoco2021_aug, sum(tamoco2021_sep) as tamoco2021_sep, 
sum(tamoco2021_oct) as tamoco2021_oct, sum(tamoco2021_nov) as tamoco2021_nov, sum(tamoco2021_dec) as tamoco2021_dec
FROM datazonesgcr_homelocation_count
WHERE council_area = 'East Dunbartonshire'
GROUP BY "simd2020_withinds_SIMD_2020v2_Percentile";

SELECT "simd2020_withinds_SIMD_2020v2_Percentile" as simd_percentile, sum(adultpop_2020) as adult_pop_2020, sum(huq2019) as huq2019, sum(huq2020) as huq2020, sum(huq2021) as huq2021, sum(tamoco2019_jan) as tamoco2019_jan, sum(tamoco2019_feb) as tamoco2019_feb, sum(tamoco2019_mar) as tamoco2019_mar, sum(tamoco2019_apr) as tamoco2019_apr,
sum(tamoco2019_may) as tamoco2019_may, sum(tamoco2019_jun) as tamoco2019_jun, sum(tamoco2019_jul) as tamoco2019_jul, sum(tamoco2019_aug) as tamoco2019_aug, sum(tamoco2019_sep) as tamoco2019_sep, 
sum(tamoco2019_oct) as tamoco2019_oct, sum(tamoco2019_nov) as tamoco2019_nov, sum(tamoco2019_dec) as tamoco2019_dec,
sum(tamoco2020_jan) as tamoco2020_jan, sum(tamoco2020_feb) as tamoco2020_feb, sum(tamoco2020_mar) as tamoco2020_mar, sum(tamoco2020_apr) as tamoco2020_apr,
sum(tamoco2020_may) as tamoco2020_may, sum(tamoco2020_jun) as tamoco2020_jun, sum(tamoco2020_jul) as tamoco2020_jul, sum(tamoco2020_aug) as tamoco2020_aug, sum(tamoco2020_sep) as tamoco2020_sep, 
sum(tamoco2020_oct) as tamoco2020_oct, sum(tamoco2020_nov) as tamoco2020_nov, sum(tamoco2020_dec) as tamoco2020_dec,
sum(tamoco2021_jan) as tamoco2021_jan, sum(tamoco2021_feb) as tamoco2021_feb, sum(tamoco2021_mar) as tamoco2021_mar, sum(tamoco2021_apr) as tamoco2021_apr,
sum(tamoco2021_may) as tamoco2021_may, sum(tamoco2021_june) as tamoco2021_jun, sum(tamoco2021_jul) as tamoco2021_jul, sum(tamoco2021_aug) as tamoco2021_aug, sum(tamoco2021_sep) as tamoco2021_sep, 
sum(tamoco2021_oct) as tamoco2021_oct, sum(tamoco2021_nov) as tamoco2021_nov, sum(tamoco2021_dec) as tamoco2021_dec
FROM datazonesgcr_homelocation_count
WHERE council_area = 'Inverclyde'
GROUP BY "simd2020_withinds_SIMD_2020v2_Percentile";

SELECT "simd2020_withinds_SIMD_2020v2_Percentile" as simd_percentile, sum(adultpop_2020) as adult_pop_2020, sum(huq2019) as huq2019, sum(huq2020) as huq2020, sum(huq2021) as huq2021, sum(tamoco2019_jan) as tamoco2019_jan, sum(tamoco2019_feb) as tamoco2019_feb, sum(tamoco2019_mar) as tamoco2019_mar, sum(tamoco2019_apr) as tamoco2019_apr,
sum(tamoco2019_may) as tamoco2019_may, sum(tamoco2019_jun) as tamoco2019_jun, sum(tamoco2019_jul) as tamoco2019_jul, sum(tamoco2019_aug) as tamoco2019_aug, sum(tamoco2019_sep) as tamoco2019_sep, 
sum(tamoco2019_oct) as tamoco2019_oct, sum(tamoco2019_nov) as tamoco2019_nov, sum(tamoco2019_dec) as tamoco2019_dec,
sum(tamoco2020_jan) as tamoco2020_jan, sum(tamoco2020_feb) as tamoco2020_feb, sum(tamoco2020_mar) as tamoco2020_mar, sum(tamoco2020_apr) as tamoco2020_apr,
sum(tamoco2020_may) as tamoco2020_may, sum(tamoco2020_jun) as tamoco2020_jun, sum(tamoco2020_jul) as tamoco2020_jul, sum(tamoco2020_aug) as tamoco2020_aug, sum(tamoco2020_sep) as tamoco2020_sep, 
sum(tamoco2020_oct) as tamoco2020_oct, sum(tamoco2020_nov) as tamoco2020_nov, sum(tamoco2020_dec) as tamoco2020_dec,
sum(tamoco2021_jan) as tamoco2021_jan, sum(tamoco2021_feb) as tamoco2021_feb, sum(tamoco2021_mar) as tamoco2021_mar, sum(tamoco2021_apr) as tamoco2021_apr,
sum(tamoco2021_may) as tamoco2021_may, sum(tamoco2021_june) as tamoco2021_jun, sum(tamoco2021_jul) as tamoco2021_jul, sum(tamoco2021_aug) as tamoco2021_aug, sum(tamoco2021_sep) as tamoco2021_sep, 
sum(tamoco2021_oct) as tamoco2021_oct, sum(tamoco2021_nov) as tamoco2021_nov, sum(tamoco2021_dec) as tamoco2021_dec
FROM datazonesgcr_homelocation_count
WHERE council_area = 'Renfrewshire'
GROUP BY "simd2020_withinds_SIMD_2020v2_Percentile";

SELECT "simd2020_withinds_SIMD_2020v2_Percentile" as simd_percentile, sum(adultpop_2020) as adult_pop_2020, sum(huq2019) as huq2019, sum(huq2020) as huq2020, sum(huq2021) as huq2021, sum(tamoco2019_jan) as tamoco2019_jan, sum(tamoco2019_feb) as tamoco2019_feb, sum(tamoco2019_mar) as tamoco2019_mar, sum(tamoco2019_apr) as tamoco2019_apr,
sum(tamoco2019_may) as tamoco2019_may, sum(tamoco2019_jun) as tamoco2019_jun, sum(tamoco2019_jul) as tamoco2019_jul, sum(tamoco2019_aug) as tamoco2019_aug, sum(tamoco2019_sep) as tamoco2019_sep, 
sum(tamoco2019_oct) as tamoco2019_oct, sum(tamoco2019_nov) as tamoco2019_nov, sum(tamoco2019_dec) as tamoco2019_dec,
sum(tamoco2020_jan) as tamoco2020_jan, sum(tamoco2020_feb) as tamoco2020_feb, sum(tamoco2020_mar) as tamoco2020_mar, sum(tamoco2020_apr) as tamoco2020_apr,
sum(tamoco2020_may) as tamoco2020_may, sum(tamoco2020_jun) as tamoco2020_jun, sum(tamoco2020_jul) as tamoco2020_jul, sum(tamoco2020_aug) as tamoco2020_aug, sum(tamoco2020_sep) as tamoco2020_sep, 
sum(tamoco2020_oct) as tamoco2020_oct, sum(tamoco2020_nov) as tamoco2020_nov, sum(tamoco2020_dec) as tamoco2020_dec,
sum(tamoco2021_jan) as tamoco2021_jan, sum(tamoco2021_feb) as tamoco2021_feb, sum(tamoco2021_mar) as tamoco2021_mar, sum(tamoco2021_apr) as tamoco2021_apr,
sum(tamoco2021_may) as tamoco2021_may, sum(tamoco2021_june) as tamoco2021_jun, sum(tamoco2021_jul) as tamoco2021_jul, sum(tamoco2021_aug) as tamoco2021_aug, sum(tamoco2021_sep) as tamoco2021_sep, 
sum(tamoco2021_oct) as tamoco2021_oct, sum(tamoco2021_nov) as tamoco2021_nov, sum(tamoco2021_dec) as tamoco2021_dec
FROM datazonesgcr_homelocation_count
WHERE council_area = 'North Lanarkshire'
GROUP BY "simd2020_withinds_SIMD_2020v2_Percentile";

SELECT "simd2020_withinds_SIMD_2020v2_Percentile" as simd_percentile, sum(adultpop_2020) as adult_pop_2020, sum(huq2019) as huq2019, sum(huq2020) as huq2020, sum(huq2021) as huq2021, sum(tamoco2019_jan) as tamoco2019_jan, sum(tamoco2019_feb) as tamoco2019_feb, sum(tamoco2019_mar) as tamoco2019_mar, sum(tamoco2019_apr) as tamoco2019_apr,
sum(tamoco2019_may) as tamoco2019_may, sum(tamoco2019_jun) as tamoco2019_jun, sum(tamoco2019_jul) as tamoco2019_jul, sum(tamoco2019_aug) as tamoco2019_aug, sum(tamoco2019_sep) as tamoco2019_sep, 
sum(tamoco2019_oct) as tamoco2019_oct, sum(tamoco2019_nov) as tamoco2019_nov, sum(tamoco2019_dec) as tamoco2019_dec,
sum(tamoco2020_jan) as tamoco2020_jan, sum(tamoco2020_feb) as tamoco2020_feb, sum(tamoco2020_mar) as tamoco2020_mar, sum(tamoco2020_apr) as tamoco2020_apr,
sum(tamoco2020_may) as tamoco2020_may, sum(tamoco2020_jun) as tamoco2020_jun, sum(tamoco2020_jul) as tamoco2020_jul, sum(tamoco2020_aug) as tamoco2020_aug, sum(tamoco2020_sep) as tamoco2020_sep, 
sum(tamoco2020_oct) as tamoco2020_oct, sum(tamoco2020_nov) as tamoco2020_nov, sum(tamoco2020_dec) as tamoco2020_dec,
sum(tamoco2021_jan) as tamoco2021_jan, sum(tamoco2021_feb) as tamoco2021_feb, sum(tamoco2021_mar) as tamoco2021_mar, sum(tamoco2021_apr) as tamoco2021_apr,
sum(tamoco2021_may) as tamoco2021_may, sum(tamoco2021_june) as tamoco2021_jun, sum(tamoco2021_jul) as tamoco2021_jul, sum(tamoco2021_aug) as tamoco2021_aug, sum(tamoco2021_sep) as tamoco2021_sep, 
sum(tamoco2021_oct) as tamoco2021_oct, sum(tamoco2021_nov) as tamoco2021_nov, sum(tamoco2021_dec) as tamoco2021_dec
FROM datazonesgcr_homelocation_count
WHERE council_area = 'West Dunbartonshire'
GROUP BY "simd2020_withinds_SIMD_2020v2_Percentile";

SELECT "simd2020_withinds_SIMD_2020v2_Percentile" as simd_percentile, sum(adultpop_2020) as adult_pop_2020, sum(huq2019) as huq2019, sum(huq2020) as huq2020, sum(huq2021) as huq2021, sum(tamoco2019_jan) as tamoco2019_jan, sum(tamoco2019_feb) as tamoco2019_feb, sum(tamoco2019_mar) as tamoco2019_mar, sum(tamoco2019_apr) as tamoco2019_apr,
sum(tamoco2019_may) as tamoco2019_may, sum(tamoco2019_jun) as tamoco2019_jun, sum(tamoco2019_jul) as tamoco2019_jul, sum(tamoco2019_aug) as tamoco2019_aug, sum(tamoco2019_sep) as tamoco2019_sep, 
sum(tamoco2019_oct) as tamoco2019_oct, sum(tamoco2019_nov) as tamoco2019_nov, sum(tamoco2019_dec) as tamoco2019_dec,
sum(tamoco2020_jan) as tamoco2020_jan, sum(tamoco2020_feb) as tamoco2020_feb, sum(tamoco2020_mar) as tamoco2020_mar, sum(tamoco2020_apr) as tamoco2020_apr,
sum(tamoco2020_may) as tamoco2020_may, sum(tamoco2020_jun) as tamoco2020_jun, sum(tamoco2020_jul) as tamoco2020_jul, sum(tamoco2020_aug) as tamoco2020_aug, sum(tamoco2020_sep) as tamoco2020_sep, 
sum(tamoco2020_oct) as tamoco2020_oct, sum(tamoco2020_nov) as tamoco2020_nov, sum(tamoco2020_dec) as tamoco2020_dec,
sum(tamoco2021_jan) as tamoco2021_jan, sum(tamoco2021_feb) as tamoco2021_feb, sum(tamoco2021_mar) as tamoco2021_mar, sum(tamoco2021_apr) as tamoco2021_apr,
sum(tamoco2021_may) as tamoco2021_may, sum(tamoco2021_june) as tamoco2021_jun, sum(tamoco2021_jul) as tamoco2021_jul, sum(tamoco2021_aug) as tamoco2021_aug, sum(tamoco2021_sep) as tamoco2021_sep, 
sum(tamoco2021_oct) as tamoco2021_oct, sum(tamoco2021_nov) as tamoco2021_nov, sum(tamoco2021_dec) as tamoco2021_dec
FROM datazonesgcr_homelocation_count
WHERE council_area = 'East Renfrewshire'
GROUP BY "simd2020_withinds_SIMD_2020v2_Percentile";


--2.9.2 CACI
--2.9.2.1 Mobile phone population by CACI type for each council
SELECT "type" as caci_type,sum(huq2019) as huq2019, sum(huq2020) as huq2020, sum(huq2021) as huq2021, sum(tamoco2019_jan) as tamoco2019_jan, sum(tamoco2019_feb) as tamoco2019_feb, sum(tamoco2019_mar) as tamoco2019_mar, sum(tamoco2019_apr) as tamoco2019_apr, sum(tamoco2019_may) as tamoco2019_may, sum(tamoco2019_jun) as tamoco2019_jun, sum(tamoco2019_jul) as tamoco2019_jul, sum(tamoco2019_aug) as tamoco2019_aug, sum(tamoco2019_sep) as tamoco2019_sep, 
sum(tamoco2019_oct) as tamoco2019_oct, sum(tamoco2019_nov) as tamoco2019_nov, sum(tamoco2019_dec) as tamoco2019_dec,
sum(tamoco2020_jan) as tamoco2020_jan, sum(tamoco2020_feb) as tamoco2020_feb, sum(tamoco2020_mar) as tamoco2020_mar, sum(tamoco2020_apr) as tamoco2020_apr,
sum(tamoco2020_may) as tamoco2020_may, sum(tamoco2020_jun) as tamoco2020_jun, sum(tamoco2020_jul) as tamoco2020_jul, sum(tamoco2020_aug) as tamoco2020_aug, sum(tamoco2020_sep) as tamoco2020_sep, 
sum(tamoco2020_oct) as tamoco2020_oct, sum(tamoco2020_nov) as tamoco2020_nov, sum(tamoco2020_dec) as tamoco2020_dec,
sum(tamoco2021_jan) as tamoco2021_jan, sum(tamoco2021_feb) as tamoco2021_feb, sum(tamoco2021_mar) as tamoco2021_mar, sum(tamoco2021_apr) as tamoco2021_apr,
sum(tamoco2021_may) as tamoco2021_may, sum(tamoco2021_jun) as tamoco2021_jun, sum(tamoco2021_jul) as tamoco2021_jul, sum(tamoco2021_aug) as tamoco2021_aug, sum(tamoco2021_sep) as tamoco2021_sep, 
sum(tamoco2021_oct) as tamoco2021_oct, sum(tamoco2021_nov) as tamoco2021_nov, sum(tamoco2021_dec) as tamoco2021_dec, sum(population) as population
FROM caci_homelocation_count
WHERE council = 'S12000029'
GROUP BY "type";

SELECT "type" as caci_type,sum(huq2019) as huq2019, sum(huq2020) as huq2020, sum(huq2021) as huq2021, sum(tamoco2019_jan) as tamoco2019_jan, sum(tamoco2019_feb) as tamoco2019_feb, sum(tamoco2019_mar) as tamoco2019_mar, sum(tamoco2019_apr) as tamoco2019_apr, sum(tamoco2019_may) as tamoco2019_may, sum(tamoco2019_jun) as tamoco2019_jun, sum(tamoco2019_jul) as tamoco2019_jul, sum(tamoco2019_aug) as tamoco2019_aug, sum(tamoco2019_sep) as tamoco2019_sep, 
sum(tamoco2019_oct) as tamoco2019_oct, sum(tamoco2019_nov) as tamoco2019_nov, sum(tamoco2019_dec) as tamoco2019_dec,
sum(tamoco2020_jan) as tamoco2020_jan, sum(tamoco2020_feb) as tamoco2020_feb, sum(tamoco2020_mar) as tamoco2020_mar, sum(tamoco2020_apr) as tamoco2020_apr,
sum(tamoco2020_may) as tamoco2020_may, sum(tamoco2020_jun) as tamoco2020_jun, sum(tamoco2020_jul) as tamoco2020_jul, sum(tamoco2020_aug) as tamoco2020_aug, sum(tamoco2020_sep) as tamoco2020_sep, 
sum(tamoco2020_oct) as tamoco2020_oct, sum(tamoco2020_nov) as tamoco2020_nov, sum(tamoco2020_dec) as tamoco2020_dec,
sum(tamoco2021_jan) as tamoco2021_jan, sum(tamoco2021_feb) as tamoco2021_feb, sum(tamoco2021_mar) as tamoco2021_mar, sum(tamoco2021_apr) as tamoco2021_apr,
sum(tamoco2021_may) as tamoco2021_may, sum(tamoco2021_jun) as tamoco2021_jun, sum(tamoco2021_jul) as tamoco2021_jul, sum(tamoco2021_aug) as tamoco2021_aug, sum(tamoco2021_sep) as tamoco2021_sep, 
sum(tamoco2021_oct) as tamoco2021_oct, sum(tamoco2021_nov) as tamoco2021_nov, sum(tamoco2021_dec) as tamoco2021_dec, sum(population) as population
FROM caci_homelocation_count
WHERE council = 'S12000049'
GROUP BY "type";

SELECT "type" as caci_type,sum(huq2019) as huq2019, sum(huq2020) as huq2020, sum(huq2021) as huq2021, sum(tamoco2019_jan) as tamoco2019_jan, sum(tamoco2019_feb) as tamoco2019_feb, sum(tamoco2019_mar) as tamoco2019_mar, sum(tamoco2019_apr) as tamoco2019_apr, sum(tamoco2019_may) as tamoco2019_may, sum(tamoco2019_jun) as tamoco2019_jun, sum(tamoco2019_jul) as tamoco2019_jul, sum(tamoco2019_aug) as tamoco2019_aug, sum(tamoco2019_sep) as tamoco2019_sep, 
sum(tamoco2019_oct) as tamoco2019_oct, sum(tamoco2019_nov) as tamoco2019_nov, sum(tamoco2019_dec) as tamoco2019_dec,
sum(tamoco2020_jan) as tamoco2020_jan, sum(tamoco2020_feb) as tamoco2020_feb, sum(tamoco2020_mar) as tamoco2020_mar, sum(tamoco2020_apr) as tamoco2020_apr,
sum(tamoco2020_may) as tamoco2020_may, sum(tamoco2020_jun) as tamoco2020_jun, sum(tamoco2020_jul) as tamoco2020_jul, sum(tamoco2020_aug) as tamoco2020_aug, sum(tamoco2020_sep) as tamoco2020_sep, 
sum(tamoco2020_oct) as tamoco2020_oct, sum(tamoco2020_nov) as tamoco2020_nov, sum(tamoco2020_dec) as tamoco2020_dec,
sum(tamoco2021_jan) as tamoco2021_jan, sum(tamoco2021_feb) as tamoco2021_feb, sum(tamoco2021_mar) as tamoco2021_mar, sum(tamoco2021_apr) as tamoco2021_apr,
sum(tamoco2021_may) as tamoco2021_may, sum(tamoco2021_jun) as tamoco2021_jun, sum(tamoco2021_jul) as tamoco2021_jul, sum(tamoco2021_aug) as tamoco2021_aug, sum(tamoco2021_sep) as tamoco2021_sep, 
sum(tamoco2021_oct) as tamoco2021_oct, sum(tamoco2021_nov) as tamoco2021_nov, sum(tamoco2021_dec) as tamoco2021_dec, sum(population) as population
FROM caci_homelocation_count
WHERE council = 'S12000045'
GROUP BY "type";

SELECT "type" as caci_type,sum(huq2019) as huq2019, sum(huq2020) as huq2020, sum(huq2021) as huq2021, sum(tamoco2019_jan) as tamoco2019_jan, sum(tamoco2019_feb) as tamoco2019_feb, sum(tamoco2019_mar) as tamoco2019_mar, sum(tamoco2019_apr) as tamoco2019_apr, sum(tamoco2019_may) as tamoco2019_may, sum(tamoco2019_jun) as tamoco2019_jun, sum(tamoco2019_jul) as tamoco2019_jul, sum(tamoco2019_aug) as tamoco2019_aug, sum(tamoco2019_sep) as tamoco2019_sep, 
sum(tamoco2019_oct) as tamoco2019_oct, sum(tamoco2019_nov) as tamoco2019_nov, sum(tamoco2019_dec) as tamoco2019_dec,
sum(tamoco2020_jan) as tamoco2020_jan, sum(tamoco2020_feb) as tamoco2020_feb, sum(tamoco2020_mar) as tamoco2020_mar, sum(tamoco2020_apr) as tamoco2020_apr,
sum(tamoco2020_may) as tamoco2020_may, sum(tamoco2020_jun) as tamoco2020_jun, sum(tamoco2020_jul) as tamoco2020_jul, sum(tamoco2020_aug) as tamoco2020_aug, sum(tamoco2020_sep) as tamoco2020_sep, 
sum(tamoco2020_oct) as tamoco2020_oct, sum(tamoco2020_nov) as tamoco2020_nov, sum(tamoco2020_dec) as tamoco2020_dec,
sum(tamoco2021_jan) as tamoco2021_jan, sum(tamoco2021_feb) as tamoco2021_feb, sum(tamoco2021_mar) as tamoco2021_mar, sum(tamoco2021_apr) as tamoco2021_apr,
sum(tamoco2021_may) as tamoco2021_may, sum(tamoco2021_jun) as tamoco2021_jun, sum(tamoco2021_jul) as tamoco2021_jul, sum(tamoco2021_aug) as tamoco2021_aug, sum(tamoco2021_sep) as tamoco2021_sep, 
sum(tamoco2021_oct) as tamoco2021_oct, sum(tamoco2021_nov) as tamoco2021_nov, sum(tamoco2021_dec) as tamoco2021_dec, sum(population) as population
FROM caci_homelocation_count
WHERE council = 'S12000018'
GROUP BY "type";

SELECT "type" as caci_type,sum(huq2019) as huq2019, sum(huq2020) as huq2020, sum(huq2021) as huq2021, sum(tamoco2019_jan) as tamoco2019_jan, sum(tamoco2019_feb) as tamoco2019_feb, sum(tamoco2019_mar) as tamoco2019_mar, sum(tamoco2019_apr) as tamoco2019_apr, sum(tamoco2019_may) as tamoco2019_may, sum(tamoco2019_jun) as tamoco2019_jun, sum(tamoco2019_jul) as tamoco2019_jul, sum(tamoco2019_aug) as tamoco2019_aug, sum(tamoco2019_sep) as tamoco2019_sep, 
sum(tamoco2019_oct) as tamoco2019_oct, sum(tamoco2019_nov) as tamoco2019_nov, sum(tamoco2019_dec) as tamoco2019_dec,
sum(tamoco2020_jan) as tamoco2020_jan, sum(tamoco2020_feb) as tamoco2020_feb, sum(tamoco2020_mar) as tamoco2020_mar, sum(tamoco2020_apr) as tamoco2020_apr,
sum(tamoco2020_may) as tamoco2020_may, sum(tamoco2020_jun) as tamoco2020_jun, sum(tamoco2020_jul) as tamoco2020_jul, sum(tamoco2020_aug) as tamoco2020_aug, sum(tamoco2020_sep) as tamoco2020_sep, 
sum(tamoco2020_oct) as tamoco2020_oct, sum(tamoco2020_nov) as tamoco2020_nov, sum(tamoco2020_dec) as tamoco2020_dec,
sum(tamoco2021_jan) as tamoco2021_jan, sum(tamoco2021_feb) as tamoco2021_feb, sum(tamoco2021_mar) as tamoco2021_mar, sum(tamoco2021_apr) as tamoco2021_apr,
sum(tamoco2021_may) as tamoco2021_may, sum(tamoco2021_jun) as tamoco2021_jun, sum(tamoco2021_jul) as tamoco2021_jul, sum(tamoco2021_aug) as tamoco2021_aug, sum(tamoco2021_sep) as tamoco2021_sep, 
sum(tamoco2021_oct) as tamoco2021_oct, sum(tamoco2021_nov) as tamoco2021_nov, sum(tamoco2021_dec) as tamoco2021_dec, sum(population) as population
FROM caci_homelocation_count
WHERE council = 'S12000038'
GROUP BY "type";

SELECT "type" as caci_type,sum(huq2019) as huq2019, sum(huq2020) as huq2020, sum(huq2021) as huq2021, sum(tamoco2019_jan) as tamoco2019_jan, sum(tamoco2019_feb) as tamoco2019_feb, sum(tamoco2019_mar) as tamoco2019_mar, sum(tamoco2019_apr) as tamoco2019_apr, sum(tamoco2019_may) as tamoco2019_may, sum(tamoco2019_jun) as tamoco2019_jun, sum(tamoco2019_jul) as tamoco2019_jul, sum(tamoco2019_aug) as tamoco2019_aug, sum(tamoco2019_sep) as tamoco2019_sep, 
sum(tamoco2019_oct) as tamoco2019_oct, sum(tamoco2019_nov) as tamoco2019_nov, sum(tamoco2019_dec) as tamoco2019_dec,
sum(tamoco2020_jan) as tamoco2020_jan, sum(tamoco2020_feb) as tamoco2020_feb, sum(tamoco2020_mar) as tamoco2020_mar, sum(tamoco2020_apr) as tamoco2020_apr,
sum(tamoco2020_may) as tamoco2020_may, sum(tamoco2020_jun) as tamoco2020_jun, sum(tamoco2020_jul) as tamoco2020_jul, sum(tamoco2020_aug) as tamoco2020_aug, sum(tamoco2020_sep) as tamoco2020_sep, 
sum(tamoco2020_oct) as tamoco2020_oct, sum(tamoco2020_nov) as tamoco2020_nov, sum(tamoco2020_dec) as tamoco2020_dec,
sum(tamoco2021_jan) as tamoco2021_jan, sum(tamoco2021_feb) as tamoco2021_feb, sum(tamoco2021_mar) as tamoco2021_mar, sum(tamoco2021_apr) as tamoco2021_apr,
sum(tamoco2021_may) as tamoco2021_may, sum(tamoco2021_jun) as tamoco2021_jun, sum(tamoco2021_jul) as tamoco2021_jul, sum(tamoco2021_aug) as tamoco2021_aug, sum(tamoco2021_sep) as tamoco2021_sep, 
sum(tamoco2021_oct) as tamoco2021_oct, sum(tamoco2021_nov) as tamoco2021_nov, sum(tamoco2021_dec) as tamoco2021_dec, sum(population) as population
FROM caci_homelocation_count
WHERE council = 'S12000050'
GROUP BY "type";

SELECT "type" as caci_type,sum(huq2019) as huq2019, sum(huq2020) as huq2020, sum(huq2021) as huq2021, sum(tamoco2019_jan) as tamoco2019_jan, sum(tamoco2019_feb) as tamoco2019_feb, sum(tamoco2019_mar) as tamoco2019_mar, sum(tamoco2019_apr) as tamoco2019_apr, sum(tamoco2019_may) as tamoco2019_may, sum(tamoco2019_jun) as tamoco2019_jun, sum(tamoco2019_jul) as tamoco2019_jul, sum(tamoco2019_aug) as tamoco2019_aug, sum(tamoco2019_sep) as tamoco2019_sep, 
sum(tamoco2019_oct) as tamoco2019_oct, sum(tamoco2019_nov) as tamoco2019_nov, sum(tamoco2019_dec) as tamoco2019_dec,
sum(tamoco2020_jan) as tamoco2020_jan, sum(tamoco2020_feb) as tamoco2020_feb, sum(tamoco2020_mar) as tamoco2020_mar, sum(tamoco2020_apr) as tamoco2020_apr,
sum(tamoco2020_may) as tamoco2020_may, sum(tamoco2020_jun) as tamoco2020_jun, sum(tamoco2020_jul) as tamoco2020_jul, sum(tamoco2020_aug) as tamoco2020_aug, sum(tamoco2020_sep) as tamoco2020_sep, 
sum(tamoco2020_oct) as tamoco2020_oct, sum(tamoco2020_nov) as tamoco2020_nov, sum(tamoco2020_dec) as tamoco2020_dec,
sum(tamoco2021_jan) as tamoco2021_jan, sum(tamoco2021_feb) as tamoco2021_feb, sum(tamoco2021_mar) as tamoco2021_mar, sum(tamoco2021_apr) as tamoco2021_apr,
sum(tamoco2021_may) as tamoco2021_may, sum(tamoco2021_jun) as tamoco2021_jun, sum(tamoco2021_jul) as tamoco2021_jul, sum(tamoco2021_aug) as tamoco2021_aug, sum(tamoco2021_sep) as tamoco2021_sep, 
sum(tamoco2021_oct) as tamoco2021_oct, sum(tamoco2021_nov) as tamoco2021_nov, sum(tamoco2021_dec) as tamoco2021_dec, sum(population) as population
FROM caci_homelocation_count
WHERE council = 'S12000039'
GROUP BY "type";

SELECT "type" as caci_type,sum(huq2019) as huq2019, sum(huq2020) as huq2020, sum(huq2021) as huq2021, sum(tamoco2019_jan) as tamoco2019_jan, sum(tamoco2019_feb) as tamoco2019_feb, sum(tamoco2019_mar) as tamoco2019_mar, sum(tamoco2019_apr) as tamoco2019_apr, sum(tamoco2019_may) as tamoco2019_may, sum(tamoco2019_jun) as tamoco2019_jun, sum(tamoco2019_jul) as tamoco2019_jul, sum(tamoco2019_aug) as tamoco2019_aug, sum(tamoco2019_sep) as tamoco2019_sep, 
sum(tamoco2019_oct) as tamoco2019_oct, sum(tamoco2019_nov) as tamoco2019_nov, sum(tamoco2019_dec) as tamoco2019_dec,
sum(tamoco2020_jan) as tamoco2020_jan, sum(tamoco2020_feb) as tamoco2020_feb, sum(tamoco2020_mar) as tamoco2020_mar, sum(tamoco2020_apr) as tamoco2020_apr,
sum(tamoco2020_may) as tamoco2020_may, sum(tamoco2020_jun) as tamoco2020_jun, sum(tamoco2020_jul) as tamoco2020_jul, sum(tamoco2020_aug) as tamoco2020_aug, sum(tamoco2020_sep) as tamoco2020_sep, 
sum(tamoco2020_oct) as tamoco2020_oct, sum(tamoco2020_nov) as tamoco2020_nov, sum(tamoco2020_dec) as tamoco2020_dec,
sum(tamoco2021_jan) as tamoco2021_jan, sum(tamoco2021_feb) as tamoco2021_feb, sum(tamoco2021_mar) as tamoco2021_mar, sum(tamoco2021_apr) as tamoco2021_apr,
sum(tamoco2021_may) as tamoco2021_may, sum(tamoco2021_jun) as tamoco2021_jun, sum(tamoco2021_jul) as tamoco2021_jul, sum(tamoco2021_aug) as tamoco2021_aug, sum(tamoco2021_sep) as tamoco2021_sep, 
sum(tamoco2021_oct) as tamoco2021_oct, sum(tamoco2021_nov) as tamoco2021_nov, sum(tamoco2021_dec) as tamoco2021_dec, sum(population) as population
FROM caci_homelocation_count
WHERE council = 'S12000011'
GROUP BY "type";
