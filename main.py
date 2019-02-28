#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 19 09:58:39 2019
Title: Ceneo Scraper
@author: Josef@revolt.bi
"""

import csv
import datetime
from datetime import date, timedelta, datetime
import pandas as pd
#from lxml import html
import time
#import mechanicalsoup
from bs4 import BeautifulSoup
#import requests, bs4  
from requests import Request, Session
import pytz
import json



### IN KBC
with open("/data/config.json", mode="r") as config_file:
    config_dict = json.load(config_file)
        



USERNAME = config_dict["parameters"]["username"]
PASSWORD = config_dict["parameters"]["#password"]
PAST_DAYS = config_dict["parameters"]["past"]
FROM = config_dict["parameters"]["from"]
TO = config_dict["parameters"]["to"]
VARLIST = config_dict["parameters"]["VARLIST"].replace(" ","").split(",")
OUTPUT_FILE = config_dict["parameters"]["Output_file_name"]
DESTINATION_BUCKET = config_dict["parameters"]["destination_bucket"]
INKREMENTAL = config_dict["parameters"]["incremental"]
PK = config_dict["parameters"]["PK"].replace(" ","").split(",")
DESTINATION = DESTINATION_BUCKET + "." + OUTPUT_FILE.replace(".csv","")



############# INPUT manipulation and chceks

if FROM == "" or TO == "":
    FROM_date = date.today()- timedelta(PAST_DAYS) ### tisíciny vteřiny
    TO_date = date.today()- timedelta(1)
else: 
    FROM_date = min(datetime.strptime(FROM, "%Y/%m/%d").date(),date.today()- timedelta(PAST_DAYS))
    TO_date = min(datetime.strptime(TO, "%Y/%m/%d").date(),date.today()- timedelta(1)) 
    
FROM_dt = datetime.combine(FROM_date, datetime.min.time())
TO_dt = datetime.combine(TO_date, datetime.min.time())
FROM_form =  datetime.strftime(FROM_dt, "%Y-%m-%d" )
TO_form = datetime.strftime(TO_dt, "%Y-%m-%d" )
FROM_un =  datetime.strftime(FROM_dt, "%Y_%m_%d" )
TO_un = datetime.strftime(TO_dt, "%Y_%m_%d" )


#FROM_ms = FROM_dt.timestamp() * 1000
#TO_ms = TO_dt.timestamp() * 1000


##########  PARAMETERS  #####################

WEB_login = "https://shops.ceneo.pl/Account/Login?ReturnUrl=/"
WEB_MyReports="https://shops.ceneo.pl/Reports/MyReports"
WEB_GeneratedReports = "https://shops.ceneo.pl/Reports/GeneratedReports"
WEB_csv = "https://shops.ceneo.pl/Reports/GeneratedReportFile?"
headings = 'english'

Today_str = datetime.strftime(date.today(), "%Y-%m-%d")

### !!! reports should be selected based on variables that I am interested in.
reports = [4, 4333]
report_headers = [["category_main","category", "product_name", "product_ID_Mall", "date"
                   , "cost_of_click", "cost_of_biding", "cost_total", "position"
                   , "IP", "Sum_definition", "cost_of_click_sum"
                   , "cost_of_biding_sum", "cost_total_sum"]
                    ,["category_main","category"
                      , "product_ID_Ceneo", "product_name"
                      , "product_ID_Mall", "brand"
                      , "price_lowest_offer_ZL", "price_highest_offer_ZL"
                      , "price_offer_Mall_ZL", "offer_rank_wrt_price"
                      , "number_of_shops_offering_product"]]




######## LOGIN #####

headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
           "Accept-Encoding" : 	"gzip, deflate, br",
           "Accept-Language" : "cs,sk;q=0.8,en-US;q=0.5,en;q=0.3",
           "Connection" : "keep-alive",
           "DNT" : "1",
           "Host" : "shops.ceneo.pl",
           "Upgrade-Insecure-Requests" : "1",
           "User-Agent" : "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.13; rv:65.0) Gecko/20100101 Firefox/65.0"
           }

login_form = {"UserName" : USERNAME, 
              "Password": PASSWORD,
              "RememberMe": "false"}


s = Session()  # opening session
s.headers.update(headers) # setting up headers
login_page = s.get(WEB_login) # getting login page to obtain cookie with verification tooken

#Login POST
log = s.post(WEB_login, data=login_form ) 

log.status_code == 200


########### request the required reports ##################


for i in reports:
    #generating report_form
    report_form = { "Parameters.ReportId": str(i),
                   "Parameters.From": FROM_form,
                   "Parameters.To" :	TO_form,
                   "Parameters.Category.Id" : "",	
                   "Parameters.Category.Name" : "",	
                   "Parameters.Category.isLeafValidaton" : "",	
                   "Parameters.OutputFormat":	"Csv"}
    # POST request
    r = s.post(WEB_MyReports, data=report_form)
    print(r.status_code==200)

tz = pytz.timezone('Europe/Warsaw')
req_time = datetime.strftime(datetime.now(tz), '%H-%M')
print("Time of request is " + req_time)
    
    
 
    
######### downloading and processing the reports

All_rep = pd.DataFrame([["Raport przejść" , 4, "Raport_przejsc_" + FROM_un + "_" + TO_un + ".csv"],
           ["Zestawienie opinii o Sklepie" , 5,"tbs"],
           ["Raport boksów reklamowych" , 6,"tbs"],
           ["Dynamika popularności" , 7,"tbs"],
           ["Popularność produktu Sklepu" , 9, "tbs"],
           ["Raport podsumowujący" , 10,"tbs"],
           ["Top 2000 rekomendowanych produktów" , 11,"tbs"],
           ["Raport promowanych ofert na stronie kategorii" , 4255,"tbs"],
           ["Raport obecności w Strefach polecanych ofert i produktów" , 4311,"tbs"],
           ["Wpłaty kupujących w usłudze Kup teraz" , 4312,"tbs"],
           ["Wypłaty środków z usługi Kup teraz" , 4313,"tbs"],
           ["Raport skuteczności rekomendacji" , 4315,"tbs"],
           ["Oferty na Ceneo wg. cen" , 4333, 'Oferty_na_Ceneo_wg._cen_' +  Today_str + '_' + req_time +'.csv']]
        , columns = ["report", "id", "csv"])



reports_csv= []
for rep in reports:
    reports_csv.append(All_rep.loc[All_rep["id"]==rep,"csv"].values[0])

OUTPUT = pd.DataFrame(columns= ["product_ID_Mall"])
while_check  = 0
report_check = [0] * len(reports)
downloaded_reports = []
run = 0
    
while while_check < len(reports):  # ověří všechny reporty jsou downloaded
    
    run +=1

    r = s.get(WEB_GeneratedReports)
    print(r.status_code)
    GeneratedReports = []
    GeneratedReports_trunc = []
    Grep = BeautifulSoup(r.text, "html.parser")
    for i, val in enumerate(Grep.find_all("a")):
        GeneratedReports.append(val.get("href").replace("/Reports/GeneratedReportFile?fileName=",""))
        GeneratedReports_trunc.append(GeneratedReports[i][0:-10])
        
        
    for i,rep in enumerate(reports_csv):
        
        if report_check[i] == 0: # while it is not downloaded
                
            if (rep in GeneratedReports) or ((reports[i]==4333) and (rep[0:-10] in GeneratedReports_trunc)) : # check whether it is already available
                if (reports[i]==4333): #for this report I am satisfied with report from the same day
                    rep_down = GeneratedReports[[i for i,x in enumerate(GeneratedReports_trunc) if x == rep[0:-10]][0]]
                else:
                    rep_down = rep #otherwise I want exact match
                    
                print("downloading:" + rep_down)
                r = s.get(WEB_csv, params={"fileName":rep_down})  # if yes download it
               
                cs = csv.reader(r.text.splitlines(), delimiter = ",")  
                temp_list = list(cs)
                
                
                #process it
                ## report of individual clicks
                if reports[i]==4:
                    temp_rep = pd.DataFrame.from_dict(temp_list[4:])
                    temp_rep.columns = temp_list[3]
                    if headings == 'english': temp_rep.columns = report_headers[i]
                    # remove  white spaces and currency, replace , to .
                    temp_rep[temp_rep.columns[5:8]] = temp_rep[temp_rep.columns[5:8]].replace( " zł", "", regex = True)
                    temp_rep[temp_rep.columns[5:8]] = temp_rep[temp_rep.columns[5:8]].replace( " ", "", regex = True)
                    temp_rep[temp_rep.columns[5:8]] = temp_rep[temp_rep.columns[5:8]].replace( ",", ".", regex = True)
                    
                    temp_rep[temp_rep.columns[5:8]] = temp_rep[temp_rep.columns[5:8]].apply(pd.to_numeric)
                    temp_rep["date"] = temp_rep["date"].str.slice(stop=10)

                    ##zgroupovat
                    temp_rep = temp_rep.groupby(by= ["product_ID_Mall","date"]).agg({"cost_of_click": ["sum", "count", "min","max"],
                                                                             "cost_of_biding": ["sum"],
                                                                             "cost_total" : "sum",
                                                                             "date": "min"})
                    temp_rep.columns = ["cost_of_clicks_sum","count_of_clicks", "cost_of_click_min", "cost_of_click_max"
                                        , "cost_of_biding_sum"
                                        , "cost_total_sum"
                                        , "date"]
        

                    ## 3341 groupovaných položek
                    # and store it
                    var_to_use = list(set(temp_rep.columns ).intersection(VARLIST))
                    OUTPUT = pd.merge(OUTPUT,  temp_rep[var_to_use], left_on = "product_ID_Mall", right_on = "product_ID_Mall", how = "outer" )

                ## REport cen na Ceneo    
                if reports[i]==4333:
                    temp_rep = pd.DataFrame.from_dict(temp_list[1:])
                    temp_rep.columns = temp_list[0]
                    #change headers to english
                    if headings == 'english': temp_rep.columns = report_headers[i]
                    # remove  white spaces and and currency 
                    temp_rep[temp_rep.columns[6:9]] = temp_rep[temp_rep.columns[6:9]].replace( " zł", "", regex = True)
                    temp_rep[temp_rep.columns[6:9]] = temp_rep[temp_rep.columns[6:9]].replace( " ", "", regex = True)
                    temp_rep[temp_rep.columns[6:9]] = temp_rep[temp_rep.columns[6:9]].replace( ",", ".", regex = True)
                    temp_rep["timestamp_of_prices"] = Today_str + ' ' + req_time
                    # remove duplicates before merge
                    temp_rep = temp_rep.drop_duplicates(subset=["product_ID_Mall"], keep = "first")
                    
                    # and store it
                    var_to_use = list(set(temp_rep.columns ).intersection(VARLIST))
                    
                    OUTPUT = pd.merge(OUTPUT,  temp_rep[var_to_use], left_on = "product_ID_Mall", right_on = "product_ID_Mall", how = "outer" )

                report_check[i] = 1
                print("report downloaded: "+ rep)
    
    print("run" + str(run))
    if run==40:
        raise Exception("I was not able to download the report even in 40 tries. Try it again!")
    while_check = sum(report_check)
    if while_check < len(reports): time.sleep(15) #wait 15s between every attempt 
   
   

  
##################### konec loopu ##########


#### COMPONENT:::
from keboola import docker

# initialize the library
cfg = docker.Config()

out_file = "out/tables/" + OUTPUT_FILE
COLUMNS = OUTPUT.columns.values.tolist()

with open( out_file , 'a') as f:
        cfg.write_table_manifest(out_file, destination=DESTINATION, 
                                    primary_key=PK, 
                                    incremental=INKREMENTAL,
                                    columns = COLUMNS)
        OUTPUT.to_csv(f,sep=",", header=False, index=False)



