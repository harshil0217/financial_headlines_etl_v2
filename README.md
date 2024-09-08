# Overview
The following ETL pipeline attempts to both quantify national financial sentiment as represented in news headlines and present that data as it updates in real-time.

# Features
The pipeline first extracts financial headlines from a Public RSS feed, classifies sentiment, and exports data to MySQL RDS server. The data is then visualized in a publicily available PowerBI dashboard.

# Installation
Copy the code in the lambda_function.py file and paste to a new lambda function. Create a new layer with the dependencies listed in the requirements.txt file and add to the lambda fucntion
Configure appropriate permissions for accessing external URLS (update outbound rules for security group, add a NAT gateway to VPC) and configure RDS server and RDS proxy in the same VPC for exporting data

# PowerBI Dashboard Link
## No longer active due to AWS costs
https://app.powerbi.com/view?r=eyJrIjoiY2NkYWI1NmUtOTU1Zi00ZjdlLTllYTAtM2E3NTE4ZDY2MzJmIiwidCI6IjIyMTc3MTMwLTY0MmYtNDFkOS05MjExLTc0MjM3YWQ1Njg3ZCIsImMiOjN9

