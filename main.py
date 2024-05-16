from src.api_to_csv import write_country_data_to_csv
from src.data_analyse import *


#Starting with problem statement

# 2.1) Most affected country among all the countries ( total death/total covid cases).
print("Most affected Country:",most_affected_country(df)['Country'][0])
most_affected_country(df)


# 2.2) Least affected country among all the countries ( total death/total covid cases).
print("Least affected Country:",least_affected_country(df)['Country'][0])
least_affected_country(df)


# 2.3) Country with highest covid cases.
print("Country with highest COVID cases:", country_with_highest_cases(df)['Country'][0])
country_with_highest_cases(df)

# 2.4) Country with minimum covid cases.
print("Country with minimum COVID cases:", country_with_minimum_cases(df)['Country'][0])
country_with_minimum_cases(df)


# 2.5) Total cases.
print("Total cases:", total_cases(df)['total_cases'][0])
total_cases(df)

# 2.6) Country that handled the covid most efficiently( total recovery/ total covid cases).
print("Country that handled the COVID most efficiently:", most_efficient_country(df)['most_efficient_country'][0])
most_efficient_country(df)


# 2.7) Country that handled the covid least efficiently( total recovery/ total covid cases).
print("Country that handled the COVID least efficiently:", least_efficient_country(df)['least_efficient_country'][0])
least_efficient_country(df)

# 2.8) Country least suffering from covid ( least critical cases).
print("Country least suffering from COVID (least critical cases):", country_least_critical_cases(df)['Country'][0])
country_least_critical_cases(df)

# 2.9) Country still suffering from covid (highest critical cases).
print("Country still suffering from COVID (highest critical cases):", country_highest_critical_cases(df)['Country'][0])
country_highest_critical_cases(df)

if __name__ == '__main__':
    # staring API Server
    run_server()
