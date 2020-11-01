### About

A simple pipeline using:

- Kaggle Book Data
- AWS S3
- Google Books API
- Kafka
- Pandas
- Flask
- PostgreSQL

### Flow

1. CSV data is loaded into S3
2. CSV data is read from S3
    - Initiated via a GET request to an available endpoint (/loaddata)
    - Example: Scheduled batch job issues a request to the endpoint to load more data into the PSQL database. 
3. Additional book data is pulled from the Google Books API via a given ISBN
4. Additional book data is merged with the CSV data from S3
5. Merged data is sent to three different defined topics in a running Kafka cluster
6. Data in Kafka is consumed via a consumer
7. Consumed Kafka data is transformed into the appropriate table schemas defined in our PostgreSQL database
8. Data is inserted into the appropriate table in our PostgreSQL database

### Purpose

- To apply what I learned from Data Engineering resources online
