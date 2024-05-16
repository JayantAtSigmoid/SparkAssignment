#!/usr/bin/env python
# coding: utf-8

# In[51]:


import requests
import csv
import json
import logging
import os


# In[65]:


# Configure logging
logging.basicConfig(level=logging.INFO)  # Set logging level to INFO


def fetch_country_data():
    # Get the absolute path to the directory of api_to_csv.py

    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_file_path = os.path.join(script_dir, '..', 'config.json')
    with open(config_file_path, 'r') as f:
        config = json.load(f)

    
    base_url = config['api']['base_url']
    

  
    url = base_url

    try:
        
        response = requests.get(url)

   
        if response.status_code == 200:
            
            logging.info("Successfully fetched COVID-19 data.")
            return response.json()
        else:
         
            logging.error(f"Failed to fetch COVID-19 data. Status code: {response.status_code}")
            return None
    except requests.RequestException as e:
        
        logging.error(f"An error occurred during data retrieval: {str(e)}")
        return None
# fetch_country_data()


# In[63]:


# Configure logging
logging.basicConfig(level=logging.INFO)  # Set logging level to INFO

def write_country_data_to_csv(csv_file):
    try:
        # Open CSV file for writing
        with open(csv_file, 'w', newline='') as file:
            writer = csv.writer(file)

            # Write header row
            writer.writerow(['Country', 'Cases', 'Deaths', 'Recovered', 'Active Cases', 'Critical Cases'])

            # Fetch country data
            countries_data = fetch_country_data()

            # Iterate over countries and fetch data
            for country_data in countries_data:
                try:
                    if country_data:
                        writer.writerow([
                            country_data['country'],
                            country_data['cases'],
                            country_data['deaths'],
                            country_data['recovered'],
                            country_data['active'],
                            country_data['critical']
                        ])
                except KeyError as e:
                    # Log error if required keys are missing in country_data
                    logging.error(f"KeyError: Missing key in country_data - {str(e)}")
                except Exception as e:
                    # Log general exception during data writing
                    logging.error(f"Error writing data to CSV: {str(e)}")

            logging.info("CSV file writing completed successfully.")

    except FileNotFoundError:
        # Log error if CSV file path is incorrect or file is not accessible
        logging.error(f"FileNotFoundError: CSV file '{csv_file}' not found or inaccessible.")
    except Exception as e:
        # Log general exception during CSV file operation
        logging.error(f"Error opening CSV file: {str(e)}")

# Example usage:


