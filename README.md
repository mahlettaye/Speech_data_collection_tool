# ETL pipeline for Speech-to-text-data-collection

This project aims to develop an open-source speech data collection tool that can be used by any language. We have created a new ETL pipeline a s3 bucket, Kafka, Spark and Airflow, which can automatically handle schema changes We have used apache Kafka as a streaming platform that allows for the creation of real-time data processing pipelines and streaming applications. Kafka, allows you to sequentially log streaming data into topic-specific feeds.  Apache Airflow allows us to create, orchestrate and monitor data workflows.  As a part of distributed data processing, we have used spark.

## Arcitecture

We have implemented this arcitecture
![alt text](https://github.com/mahlettaye/Speech_data_collection_tool/Arcitecture.png)

## Steps to Build ETL Pipline

- Build a simple web app that will help us to collect speech for specific text sent by Kafka producer.
- Setup  delta lake on s3 bucket
- Setup Kafka producer and consumer using Kafka-python
- Setup Airflow for scheduling
- Setup spark

### How to use and contribute

To be added

#### To use repository

Assuming that you are working in Project directory

`cd ~/Project`
`git clone https://github.com/Abuton/Speech-to-text-data-collection.git`
`git checkout main`

#### To contribute to the the repo

- Instead, you will create ***dev_yourname*** on your machine that exist for the purpose of solving singular issues.

`git checkout main`
`git checkout -b dev_yourname`

-Make changes.

`git add .`
`git commit -m "Updated Kafaka`

This adds any new files to be tracked and makes a commit. Now let's add them to your branch and pull request to merge.

`git checkout main`
`git push origin dev_yourname`
