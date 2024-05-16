# COVID-19 Data Analysis and API Project

This project fetches COVID-19 data from an external API, analyzes it using Apache Spark, and serves the analysis results as RESTful APIs. The analysis includes metrics such as the most affected country, least affected country, country with the highest COVID cases, total cases, efficiency in handling COVID, and critical cases.

## Project Structure

- **api_to_csv.py**: Handles interaction with the COVID-19 data API and writes data to a CSV file.
- **data_analyse.py**: Analyzes the COVID-19 data using Apache Spark and defines functions for various analyses.
- **main.py**: Orchestrates data fetching, analysis, and serving. It calls functions from `api_to_csv.py` and `data_analyse.py` and starts an HTTP server to serve the analysis results as RESTful APIs.
- **config.json**: Contains configuration parameters such as API base URL and CSV file path.
- **index.html**: HTML interface served by the RESTful API server to interact with the APIs.

## Getting Started

1. Clone the repository:

   ```bash
   git clone https://github.com/JayantAtSigmoid/SparkAssignment
   cd SparkAssignment
   
2. Install dependencies:
   ```bash
   pip install -r requirements.txt

3. Run the main script:
   ```bash
   python3 main.py


## Access the RESTful APIs:
Open a web browser and navigate to http://localhost:8000/ to access the API links.

# API Endpoints

- `/get-covid-data`: Returns the COVID-19 data for all countries.
- `/most-affected-country`: Returns the most affected country.
- `/least-affected-country`: Returns the least affected country.
- `/country-highest-cases`: Returns the country with the highest COVID cases.
- `/country-minimum-cases`: Returns the country with the minimum COVID cases.
- `/total-cases`: Returns the total COVID cases.
- `/most-efficient-country`: Returns the country that handled COVID most efficiently.
- `/least-efficient-country`: Returns the country that handled COVID least efficiently.
- `/country-least-critical-cases`: Returns the country least suffering from COVID (least critical cases).
- `/country-highest-critical-cases`: Returns the country still suffering from COVID (highest critical cases).


## Configuration
The API base URL and CSV file path are specified in config.json.

## Dependencies
Python 3.x
Apache Spark
pandas
requests
Contributing
