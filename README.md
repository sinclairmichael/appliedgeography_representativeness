# Assessing the socio-demographic representativeness of mobile phone application data
This repo contains the code used for the results in paper: 'Assessing the socio-demographic representativeness of mobile phone application data' in Applied Geography

Given the volume of data being processed, the pre-processing and post-processing of the data for home location estimates are completed using SQL on the database tables. The home location estimates for each of the techniques in this paper are completed in R (between pre and post processing) and results saved back to tables in the database for further analysis

### The SQL file 'Complete SQL code.sql' contains the complete workflow which is as follows:
Section 1: relates to the method 'Using activity heuristics only to estimate home location'<br>
Section 2: relates to the method 'Using activity heuristics and land use to estimate home location'

### Pre-processing steps for both methods
Section 1.1 and 1.2 are the pre-processing steps required for huq/tamoco before standard home detection approach<br>
Section 2.1 and 2.1 are the pre-processing steps required for huq/tamoco before advanced home detection approach

### Applying both home detection methods (IN R)
Section 1.3 and 1.4 are reference to the R scripts to performs the standard home detection approach for huq/tamoco<br>
Section 2.3 and 2.4 are reference to the R scripts to performs the advanced home detection approach for huq/tamoco

### Extraction of mobile and population samples for both methods
Section 1.5 - 1.9 are the extraction of population comparisons for the standard home detection approach<br>
Section 2.5 - 2.9 are the extraction of population comparisons for the advanced home detection approach

#### Data sources used:
A. Huq data (2019-2021) [tables: huq_2019; huq_2020; huq_2021]<br>
Information: https://www.ubdc.ac.uk/data-services/data-catalogue/transport-and-mobility-data/huq-data/<br>
B. Tamoco data (2019-2021) [tables: tamoco_2019; tamoco_2020; tamoco_2021]<br>
Information: https://www.ubdc.ac.uk/data-services/data-catalogue/transport-and-mobility-data/tamoco-data/<br>
C. Scottish Datazone boundaries and adult populations for 2020 [table: datazonesgcr_homelocation_count]<br>
Information: http://spatialdata.gov.scot/<br>
D. Postcode boundaries for Glasgow City Region with population for 2020 [table: caci_homelocation_count]<br>
Information: https://www.nrscotland.gov.uk/statistics-and-data/geography/nrs-postcode-extract<br>
E. Geomni UKBuildings land use data [table: ukbuildingsgcr_4326]<br>
Information: https://www.verisk.com/en-gb/3d-visual-intelligence/products/ukbuildings/ (available through digimap)<br>
F. SIMD 2020 data (joined to datazones [C])<br>
Information: https://www.gov.scot/collections/scottish-index-of-multiple-deprivation-2020/<br>
G. CACI Acorn classification data (joined to postcodes [D])<br>
Information: https://acorn.caci.co.uk/downloads/Acorn-User-guide.pdf<br>
