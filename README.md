# PyGeoScrapper
This is a Framework to scrape company data from Yellowpagesusa.net and perform geospatial capabilities using opensource tools.
The following capabilities are supported:

1. Scrape data from Yellowpagesusa.net based on STATE code and number of links to scrape at a time
2. Restart of scrapping in case of internet link failure
3. Geocode the scrapped address
4. Create Risk Circle with center as a random location
5. Calculate the total insured value and total impacted locations under the risk circle
6. Populating the risk locations along with the risk circle center on google map

This framework uses Python 2.7, Google geocoder and MySQL.

The following python packages must be installed to be able to use the program:
1. untangle
2. lxml
3. mysql

Create the following table structures:
1. Table: company_info
Columns:
ID int(11) AI PK 
COMPANY_NAME varchar(80) 
COMPANY_PHONE varchar(15) 
COMPANY_FAX varchar(15) 
COMPANY_ADDR varchar(150) 
IS_SCRUBBED char(1) 
COMPANY_TIV decimal(6,0)

2. Table: geo_scrub_addr
Columns:
ID int(11) AI PK 
RAW_ADDR varchar(150) 
SCRUB_STS varchar(25) 
ADDR_TYPE varchar(100) 
SCRUB_TYPE varchar(25) 
SCRUB_ADDR varchar(150) 
REQUEST_LINK varchar(300) 
LATITUDE decimal(9,6) 
LONGITUDE decimal(9,6) 
SWLATITUDE decimal(9,6) 
SWLONGITUDE decimal(9,6) 
NELATITUDE decimal(9,6) 
NELONGITUDE decimal(9,6) 
G_PLACE_ID varchar(100)

3. Table: scrap_addr_link
Columns:
ADDR_BASE_URL varchar(100) PK 
STATE char(2) 
PROCESS_FLAG char(1)

4. Table: scrap_link
Columns:
LINK varchar(150) PK 
STATE char(2) 
SCRAP_TM datetime

Process Flow:
1. Insert base link url into  scrap_addr_link table  ---  This is the starting point and is manual
2. Kick off PyScrapper.py with the following arguments : 2 digited US state code, Number of links to scrap
2. Process scrapes the company name, company address, fax, tel info and stores into company_info table
3. Next use Geocode_Address_Google.py to perform geospatial capabilities

