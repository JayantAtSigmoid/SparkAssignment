import http.server
import socketserver
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, regexp_replace
import json
from pyspark.sql.types import *

# Initialize SparkSession
spark = SparkSession.builder.appName("CovidDataAnalyzer").getOrCreate()

schema = StructType([
    StructField("Country", StringType(), nullable=True),
    StructField("Total_Death", LongType(), nullable=True),
    StructField("Total_Recovered", LongType(), nullable=True),
    StructField("Total_Cases", LongType(), nullable=True),
    StructField("Critical", LongType(), nullable=True)
])

# Load the CSV file into a PySpark DataFrame
df = spark.read.option("header", "true").schema(schema).csv("my_file.csv")

collected_data=df.collect()
df = df.filter(~col('Country').isin(['All', 'Europe', 'Oceania', 'North-America','South-America','R&eacute;union','MS-Zaandam','Diamond-Princess','Caribbean-Netherlands','Sint-Maarten','CAR','French-Guiana','DPRK','Asia']))
df = df.filter(df['Total_Recovered'].isNotNull())
df = df.filter(df['Total_Cases'].isNotNull())
df = df.filter(df['Total_Death'].isNotNull())

df_filtered = df.filter(col('Critical').isNotNull())

# Calculate required metrics

most_affected_country = df.withColumn("affected_ratio", col("Total_Death") / col("Total_Cases")) \
    .orderBy(col("affected_ratio").desc()) \
    .select("Country", "affected_ratio") \
    .limit(1) \
    .collect()

least_affected_country = df.withColumn("affected_ratio", col("Total_Death") / col("Total_Cases")) \
    .orderBy(col("affected_ratio")) \
    .select("Country", "affected_ratio") \
    .limit(1) \
    .collect()

highest_cases_country = df.orderBy(col("Total_Cases").desc()) \
    .select("Country", "Total_Cases") \
    .limit(1) \
    .collect()

minimum_cases_country = df.orderBy(col("Total_Cases")) \
    .select("Country", "Total_Cases") \
    .limit(1) \
    .collect()

total_cases = df.agg(sum("Total_Cases")).collect()[0][0]

most_efficient_country = df.withColumn("efficiency", col("Total_Recovered") / col("Total_Cases")) \
    .orderBy(col("efficiency").desc()) \
    .select("Country", "efficiency") \
    .limit(1) \
    .collect()

least_efficient_country = df.withColumn("efficiency", col("Total_Recovered") / col("Total_Cases")) \
    .orderBy(col("efficiency")) \
    .select("Country", "efficiency") \
    .limit(1) \
    .collect()

least_suffering_country = df_filtered.orderBy(col("Critical")) \
    .select("Country", "Critical") \
    .limit(1) \
    .collect()

most_suffering_country = df_filtered.orderBy(col("Critical").desc()) \
    .select("Country", "Critical") \
    .limit(1) \
    .collect()

# Define a HTTP request handler class
class CovidDataHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/':
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            with open('covid_data.html', 'rb') as f:
                self.wfile.write(f.read())
        elif self.path == '/collected_data':
            data = {
                "collected_data": collected_data
            }
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(data,indent=4).encode())        
        elif self.path == '/most_affected_country':
            data = {
                "most_affected_country": most_affected_country
            }
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(data,indent=4).encode())
        elif self.path == '/least_affected_country':
            data = {
                "least_affected_country": least_affected_country
            }
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(data,indent=4).encode())
        elif self.path == '/highest_cases_country':
            data = {
                "highest_cases_country": highest_cases_country
            }
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(data,indent=4).encode())
        elif self.path == '/minimum_cases_country':
            data = {
                "minimum_cases_country": minimum_cases_country
            }
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(data,indent=4).encode())
        elif self.path == '/total_cases':
            data = {
                "total_cases": total_cases
            }
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(data).encode())
        elif self.path == '/most_efficient_country':
            data = {
                "most_efficient_country": most_efficient_country
            }
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(data,indent=4).encode())
        elif self.path == '/least_efficient_country':
            data = {
                "least_efficient_country": least_efficient_country
            }
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(data,indent=4).encode())
        elif self.path == '/least_suffering_country':
            data = {
                "least_suffering_country": least_suffering_country
            }
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(data,indent=4).encode())
        elif self.path == '/most_suffering_country':
            data = {
                "most_suffering_country": most_suffering_country
            }
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(data,indent=4).encode())
        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b'404 Not Found')

# Set up the HTTP server
PORT = 8000
with socketserver.TCPServer(("", PORT), CovidDataHandler) as httpd:
    print(f"Server running on port {PORT}")
    httpd.serve_forever()
