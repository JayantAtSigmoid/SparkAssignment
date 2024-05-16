#!/usr/bin/env python
# coding: utf-8

# In[65]:


import sys;

print(sys.executable);


# In[82]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import pandas as pd
import logging
import os


# In[83]:


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# In[72]:


spark = SparkSession.builder.appName("Read COVID Data").getOrCreate()


# In[84]:

script_dir = os.path.dirname(os.path.abspath(__file__))
config_file_path = os.path.join(script_dir, '..', 'config.json')
with open(config_file_path, 'r') as f:
        config = json.load(f)

    
   
    
df = spark.read.csv(config['data']['csv_file'], header=True, inferSchema=True)

# Show DataFrame
df.show()


# In[85]:


pandas_covid_data = df.collect()
pandas_covid_data = pd.DataFrame(pandas_covid_data, columns=df.columns)
pandas_covid_data


# In[86]:


def calculate_death_to_cases_ratio(df):
    return df.withColumn("death_to_cases_ratio", col("Deaths") / col("Cases"))

calculate_death_to_cases_ratio(df)


# In[87]:


# 2.1) Most affected country among all the countries ( total death/total covid cases).
def most_affected_country(df):
    try:
        df_with_ratio = calculate_death_to_cases_ratio(df)
        most_affected_country_data = df_with_ratio.orderBy(col("death_to_cases_ratio").desc()).first()
        return pd.DataFrame([most_affected_country_data.asDict()])
    except Exception as e:
        logging.error(f'An error occurred in most_affected_country function: {e}')
        return None


print("Most affected Country:",most_affected_country(df)['Country'][0])
most_affected_country(df)


# In[88]:


# 2.2) Least affected country among all the countries ( total death/total covid cases).
def least_affected_country(df):
    try:
        df_with_ratio = calculate_death_to_cases_ratio(df)
        least_affected_country_data = df_with_ratio.orderBy(col("death_to_cases_ratio")).first()
        return pd.DataFrame([least_affected_country_data.asDict()])
    except Exception as e:
        logging.error(f'An error occurred in least_affected_country function: {e}')
        return None

print("Least affected Country:",least_affected_country(df)['Country'][0])
least_affected_country(df)


# In[90]:


# 2.3) Country with highest covid cases.
def country_with_highest_cases(df):
    try:
        country_highest_cases = df.orderBy(col("Cases").desc()).first()
        return pd.DataFrame([country_highest_cases.asDict()])
    except Exception as e:
        logging.error(f'An error occurred in country_with_highest_cases function: {e}')
        return None


print("Country with highest COVID cases:", country_with_highest_cases(df)['Country'][0])
country_with_highest_cases(df)


# In[91]:


# 2.4) Country with minimum covid cases.
def country_with_minimum_cases(df):
    try:
        country_minimum_cases = df.orderBy(col("Cases")).first()
        return pd.DataFrame([country_minimum_cases.asDict()])
    except Exception as e:
        logging.error(f'An error occurred in country_with_minimum_cases function: {e}')
        return None

print("Country with minimum COVID cases:", country_with_minimum_cases(df)['Country'][0])
country_with_minimum_cases(df)


# In[92]:


# 2.5) Total cases.
def total_cases(df):
    try:
        total_cases = df.selectExpr("sum(Cases)").collect()[0][0]
        return pd.DataFrame([{"total_cases": total_cases}])
    except Exception as e:
        logging.error(f'An error occurred in total_cases function: {e}')
        return None

print("Total cases:", total_cases(df)['total_cases'][0])
total_cases(df)


# In[93]:


# 2.6) Country that handled the covid most efficiently( total recovery/ total covid cases).
def most_efficient_country(df):
    try:
        df_with_ratio = df.withColumn("recovery_to_cases_ratio", col("Recovered") / col("Cases"))
        most_efficient_country = df_with_ratio.orderBy(col("recovery_to_cases_ratio").desc()).first()["Country"]
        return pd.DataFrame([{"most_efficient_country": most_efficient_country}])
    except Exception as e:
        logging.error(f'An error occurred in most_efficient_country function: {e}')
        return None


print("Country that handled the COVID most efficiently:", most_efficient_country(df)['most_efficient_country'][0])
most_efficient_country(df)


# In[94]:


# 2.7) Country that handled the covid least efficiently( total recovery/ total covid cases).

def least_efficient_country(df):
    try:
        df_with_ratio = df.withColumn("recovery_to_cases_ratio", col("Recovered") / col("Cases"))
        least_efficient_country = df_with_ratio.orderBy(col("recovery_to_cases_ratio")).first()["Country"]
        return pd.DataFrame([{"least_efficient_country": least_efficient_country}])
    except Exception as e:
        logging.error(f'An error occurred in least_efficient_country function: {e}')
        return None

print("Country that handled the COVID least efficiently:", least_efficient_country(df)['least_efficient_country'][0])
least_efficient_country(df)


# In[95]:


# 2.8) Country least suffering from covid ( least critical cases).
def country_least_critical_cases(df):
    try:
        country_least_critical_cases = df.orderBy(col("Critical Cases")).first()
        return pd.DataFrame([country_least_critical_cases.asDict()])
    except Exception as e:
        logging.error(f'An error occurred in country_least_critical_cases function: {e}')
        return None

print("Country least suffering from COVID (least critical cases):", country_least_critical_cases(df)['Country'][0])
country_least_critical_cases(df)


# In[96]:


# 2.9) Country still suffering from covid (highest critical cases).
def country_highest_critical_cases(df):
    try:
        country_highest_critical_cases = df.orderBy(col("Critical Cases").desc()).first()
        return pd.DataFrame([country_highest_critical_cases.asDict()])
    except Exception as e:
        logging.error(f'An error occurred in country_highest_critical_cases function: {e}')
        return None

print("Country still suffering from COVID (highest critical cases):", country_highest_critical_cases(df)['Country'][0])
country_highest_critical_cases(df)


# In[ ]:


# Restful APIs
def handle_request(path):
    if path == '/get-covid-data':
        return pandas_covid_data
    elif path == '/most-affected-country':
        return most_affected_country(df)
    elif path == '/least-affected-country':
        return least_affected_country(df)
    elif path == '/country-highest-cases':
        return country_with_highest_cases(df)
    elif path == '/country-minimum-cases':
        return country_with_minimum_cases(df)
    elif path == '/total-cases':
        return total_cases(df)
    elif path == '/most-efficient-country':
        return most_efficient_country(df)
    elif path == '/least-efficient-country':
        return least_efficient_country(df)
    elif path == '/country-least-critical-cases':
        return country_least_critical_cases(df)
    elif path == '/country-highest-critical-cases':
        return country_highest_critical_cases(df)
    else:
        return None


class RequestHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        if self.path == '/':
            # Send the HTML page
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            with open('src/index.html', 'rb') as file:
                self.wfile.write(file.read())
        else:
            data = handle_request(self.path)
            if data is not None:
                if isinstance(data, pd.DataFrame):
                    response = data.to_json(orient='records')
                    self.send_response(200)
                    self.send_header('Content-type', 'application/json')
                    self.end_headers()
                    self.wfile.write(response.encode('utf-8'))
                else:
                    response = json.dumps(data)
                    self.send_response(200)
                    self.send_header('Content-type', 'application/json')
                    self.end_headers()
                    self.wfile.write(response.encode('utf-8'))
            else:
                self.send_response(404)
                self.end_headers()
                self.wfile.write(b'404 Not Found')


def run_server():
    server_address = ('', 8000)
    httpd = HTTPServer(server_address, RequestHandler)
    print('Starting server on port 8000...')
    print('Visit http://localhost:8000/ to access the API links.')

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        httpd.server_close()
        print('Server stopped.')  



