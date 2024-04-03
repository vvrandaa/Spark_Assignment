from http.server import BaseHTTPRequestHandler, HTTPServer
from fetch_data import *
from helper import *
import json

class MyHandler(BaseHTTPRequestHandler):
    
    # Handle GET requests
    def do_GET(self):

        # api to fetch data convert to csv and return links to all other questions
        if self.path == '/':
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()

            covid_data = fetch_covid_data()
            write_to_csv(covid_data)
            with open('index.html', 'r') as f:
                html_content = f.read()
            self.wfile.write(html_content.encode())
        
        else:
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            df = create_spark_dataframe()
            df = df.withColumn("total_deaths", col("deaths").cast("int")) \
                    .withColumn("total_cases", col("cases").cast("int")) \
                    .withColumn("total_recovered", df["recovered"].cast("int"))

            if self.path == '/most-affected-country':
                response = most_affected_country(df)

            elif self.path == '/least-affected-country':
                response = least_affected_country(df)
                
            elif self.path == '/country-with-highest-cases':    
                response = country_with_highest_cases(df)
                
            elif self.path == '/country-with-minimum-cases':
                response = country_with_minimum_cases(df)
                
            elif self.path == '/total-cases':
                response = total_cases(df)
                self.wfile.write(json.dumps(response).encode())
                
            elif self.path == '/most-efficient-country':
                response = most_efficient_country(df)
                
            elif self.path == '/least-efficient-country':
                response = least_efficient_country(df)
                
            elif self.path == '/country-with-least-critical-cases':
                response = country_with_least_critical_cases(df)
                
            elif self.path == '/country-with-highest-critical-cases':
                response = country_with_highest_critical_cases(df)

            rdd = response.rdd.map(lambda row: row.asDict())
            dict_list = rdd.collect()
            json_str = '\n'.join(json.dumps(d) for d in dict_list)
            self.wfile.write(json_str.encode())