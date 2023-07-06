# AWS Data Engineering Project: Spotify Data Pipeline

![Architectural Diagram](link-to-your-architectural-diagram-image)

## Objectives

The primary objectives of this project are:

- To collect daily global playlist data from Spotify in an automated and scalable manner.
- To process and transform the raw data into structured formats (CSV files) that are optimized for analytics.
- To store and manage large datasets effectively using Amazon S3.
- To create a data catalog that represents the processed data and is optimized for query performance.
- To enable users to run ad-hoc SQL queries on the data catalog using Amazon Athena, allowing for data exploration and analytics.

## Overview

1. **Extract**: Daily global playlist data is extracted from Spotify's API.
2. **Transform**: The raw data is transformed into 3 distinct CSV files: songs, albums, and artists.
3. **Load**: The CSV files are then loaded into Amazon S3, and AWS Glue is used to create a data catalog.
4. **Analyze**: Finally, Amazon Athena is used to perform queries and analyze the data.

## AWS Services Used

- **Amazon EventBridge**: It is used to schedule and trigger the Lambda function daily.
- **AWS Lambda**: Two Lambda functions are used in this project. The first Lambda function extracts data from Spotify's API. The second Lambda function is triggered by the creation of a new object in an S3 bucket and contains logic to transform the data into CSV files.
- **Amazon S3**: It is used for storing raw and transformed data.
- **AWS Glue**: AWS Glue Crawler is used to create a data catalog of the processed data stored in S3.
- **Amazon Athena**: Athena is used to perform SQL queries on the data catalog created by AWS Glue.

## Lambda Functions

1. **Data Extraction Lambda Function**: This function is responsible for extracting daily global playlist data from Spotify's API.
   - [Link to the code](https://github.com/mudit-mishra8/ETL-Spotify/blob/main/spotify_data_extraction.py)

2. **Data Transformation Lambda Function**: This function is invoked by an S3 trigger. It transforms the extracted data into 3 CSV files: songs, albums, and artists.
   - [Link to the code](https://github.com/mudit-mishra8/ETL-Spotify/blob/main/Spotify_data_transformation.py)
