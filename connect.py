import requests
import csv
import pandas as pd

url = "https://covid-193.p.rapidapi.com/statistics"

headers = {
	"X-RapidAPI-Key": "69f9e016f4mshb1a9aa053eb6764p17e5c9jsn0cde046c6933",
	"X-RapidAPI-Host": "covid-193.p.rapidapi.com"
}

response = requests.get(url, headers=headers)
data=response.json()
data=data['response']
# my csv file name 
csv_file="my_file.csv" 

with open(csv_file, 'w', newline='') as file:
    writer = csv.writer(file)

    # Write header row
    # This will provide schema to dataframe later
    writer.writerow(['Country','Total_Death','Total_Recovered','Total_Cases','Critical'])

    # Iterate over jsonData and write each country data into csv file 

    for countryData in data:
                writer.writerow([countryData['country'],countryData['deaths']['total'],countryData['cases']['recovered'],
                              countryData['cases']['total'],countryData['cases']['critical']])
            

