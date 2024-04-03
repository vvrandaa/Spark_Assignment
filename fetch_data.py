import requests
import csv

# to fetch data
def fetch_covid_data():
    url = "https://disease.sh/v3/covid-19/countries"
    response = requests.request("GET", url)

    if response.status_code == 200:
        return response.json()
    else:
        print("Failed to fetch COVID-19 data:", response.text)
        return None


# convert fetched data to csv format
def write_to_csv(data):
    with open('covid_data.csv', 'w', newline='') as csvfile:
        fieldnames = []
        for i in data[0]:
            fieldnames.append(i)
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for country_data in data:
            writer.writerow(country_data)
