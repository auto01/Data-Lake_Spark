
# Project: Data Lake

## Introduction

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.


## Project Description 

In this project, you'll  build an ETL pipeline for a data lake hosted on S3. To complete the project, you will need to load data from S3, process the data into analytics tables using Spark, and load them back into S3. You'll deploy this Spark process on a cluster using AWS.

## Datasets 

The datasets used are retrieved from the s3 bucket and are in the JSON format. There are two datasets namely log_data and song_data.
The song_data dataset is a subset of the the Million Song Dataset while the log_data contains generated log files based on the songs in song_data.

## Database schema design 
<br/>

The tables created follows star schema with one fact table and four dimensional tables

- Fact table : songplays
- Dimension tables : users,songs,artists,time

These tables contain clean data that is suitable for OLAP(Online Analytical Processing) operations which will be used by data analysts
to gain some useful insights from the data stored.

## ETL pipeline

The Extraction part will first gets all the desired data that are are stored in S3 and then perform some processing(ie.transformations)
with the help of Spark and load back that transformed data into S3 as destination tables in form of Parquet data files.

For instance the source data for timestamp is in unix format and that will need to be converted to timestamp from which the year, month, day, hour values etc can be extracted which will fit in the relevant target time and songplays table columns. The script will also need to cater for duplicates, ensuring that they aren't part of the final data that is loaded in the tables.


## The project template includes three files:

- etl.py : it reads data from S3, processes that data using Spark, and writes them back to S3
- dl.cfg : it contains your AWS credentials
- README.md : provides discussion on your process and decisions

## Prerequisites

- Python 2.7 or greater.
- AWS Account.
- Set your AWS access and secret key in the config file.
```
[AWS]
AWS_ACCESS_KEY_ID = <your aws key>
AWS_SECRET_ACCESS_KEY = <your aws secret>
```
- After performing above setup , simply execute etl.py