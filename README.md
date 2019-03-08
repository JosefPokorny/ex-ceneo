# ex-ceneo
Keboola connection extraktor from administration portal of Ceno.pl

## Description
Extraktor provides daily statistics on individual product level available from customize "Raport przejść" available 
after the login on https://shops.ceneo.pl/Reports/MyReports. The original report provides detail on individual click 
but the extractor aggregates data on the product level in given day.

There are two way to input the period of interest 

a) number of past days starting from yesterday, 

b) close interval of date in format YYYY/MM/DD, while this is a superior option when both alternatives are filled in.

## Available Statistics
- *product_ID_Mall*,
- *date*,
- *product_name*,
- *category_main*,
- *category*, 
- *cost_of_clicks_sum*,
- *count_of_clicks*,
- *cost_of_click_min*,
- *cost_of_click_max*,
- *cost_of_biding_sum*,
- *cost_total_sum*



## Usage
You could access the Arukereso extractor in Keboola project after login by adding to URL path 

*.../extractors/revolt-bi.ex-ceneo*


## Extension


Prepared by  ![alt text](https://www.revolt.bi/wp-content/uploads/2018/08/mail-logo-zluta.png "revolt.bi")
