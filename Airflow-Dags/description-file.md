Repeatable NLP of online newspaper articles
Overview
Generate text corpus for calculating sentiment for multiple online news sources. ETL performed by Apache Airflow DAG; article keywords extracted using newspaper3k; data saved locally to JSON; added to a Quilt data package and upload to AWS S3 bucket using Quilt T4.

Online sources
The Guardian
The New York Times
CNN
Exploratory data analysis over time
Our dashboard displays Vega Word Cloud visualizations of keywords scraped from our news sources and updated daily.
