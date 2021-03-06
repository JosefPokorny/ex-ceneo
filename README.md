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
- *product_ID_Mall*, primary key
- *date*, primary key
- *product_name*,
- *category_main*,
- *category*,  the lowest granularity of category
- *cost_of_clicks_sum*,
- *count_of_clicks*,
- *cost_of_click_min*, minimal price of click for given product in given date
- *cost_of_click_max*, maximal price of click for given product in given date
- *cost_of_biding_sum*, cost of bidding
- *cost_total_sum*
- *position*



## Usage
You could access the Arukereso extractor in Keboola project after login by adding to URL path 

*.../extractors/revolt-bi.ex-ceneo*


## Extension
The extractor is also prepared to integrate report "Oferty na Ceneo wg. cen". Nevertheless, this report is not fully compatible with "Raport przejść", because this report provides just snapshot of prices on Ceneo on givenpoint in time. (The history is not accesible. The extraction of this report is switched-off. Contact josef@revolt.bi if you are interested in these data.


![alt text](https://www.revolt.bi/wp-content/uploads/2018/08/mail-logo-zluta.png "revolt.bi")
