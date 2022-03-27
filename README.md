# audible-sales-tracker
This project is aimed to implement end-to-end data pipeline using Google Cloud Platform and create dashboard for tracking audio book sales.

## DATA PIPELINE ARCHITECTURE:
![data pipeline architecture](https://user-images.githubusercontent.com/48947748/160275716-03e84672-ec80-4a84-a225-6d1a7b2be0d6.png)
Data source come from RDBMS(MySQL) and Cloud Composer(Airflow) orchestrates its movement to a GCS. The pyspark script is then run to transform the data and aggregate with API to convert the Price column from USD to THB and then store the data back to GCS in parquet format. Airflow move the transformed data into a data warehouse hosted in Bigquery and then create a view for select certain column that use to create a dashboard.

## TRANSFORM TABLE SCHEMA
![audible data](https://user-images.githubusercontent.com/48947748/160276412-bb3139b6-47b2-4f83-9611-b38e115f92ab.png)</ br>
The transform data is contain only one table why? because the raw data in MySQL are contain two table then I think its better if I join two table and then store it in data warehouse

## ETL FLOW:
![airflow_DAG](https://user-images.githubusercontent.com/48947748/160271509-0445cc17-6cb1-4ec8-bf35-6475e83029b0.jpg)

## DASHBOARD:
The dashboard is publicly accessible here
Google data studio: https://datastudio.google.com/reporting/bd61006d-1cf1-4ff6-92fb-138d523c7399 <br />
Power BI: https://app.powerbi.com/view?r=eyJrIjoiZWVkZjRiZGYtZjBhOS00ODY2LTkyYTYtZGY5OWNiMmYyNTBmIiwidCI6IjZmNDQzMmRjLTIwZDItNDQxZC1iMWRiLWFjMzM4MGJhNjMzZCIsImMiOjEwfQ%3D%3D

Some examples screenshot of dashboard below!
![Google Data Studio Dashboard](https://user-images.githubusercontent.com/48947748/160271538-b3bfd9db-bcc4-46a6-aadc-5fdb5c53116b.jpg)
<p align="center"> Google data studio dashboard </p> </ br>

![Power BI Dashboard](https://user-images.githubusercontent.com/48947748/160271541-e917f2b8-540e-45f0-b6e6-88a574624953.jpg)
<p align="center"> Power BI dashboard </p>
