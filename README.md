### About

A simple ETL pipeline using:

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
3. Additional book data is pulled from the Google Books API via ISBN
4. Additional book data is merged with the CSV data from S3
5. Merged data is sent to a running Kafka cluster
6. Data in Kafka is consumed
7. Consumed Kafka data is transformed for PostgreSQL storage
8. Data is stored in a PostgreSQL relational database

### Purpose

- To apply what I learned from Data Engineering resources online
