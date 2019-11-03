## Description of the project

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. They would like to set up a datalake using the power of Spark and amazon web services (S3,...).

In order to do so, several key topics/steps were achieved:
* Define fact and dimension tables for a star schema:
    + The goal is to make analytics on these tables afterwards, to bring business insights for Sparkify
* Write an ETL pipeline that transfers raw data contained in S3 buckets (songs datas and logs datas) to parquet files saved in S3 buckets. That's done in several steps:
    + Load input raw data from S3 buckets
    + Transform data to build the dimension and fact tables of our star schema, using spark API
    + Saved these dimension and fact tables in another S3 bucket, under parquet files format

## Datasets available
The two input datasets (raw data) are located in S3. Here are the S3 links for each:

* Song data: s3://udacity-dend/song_data
* Log data: s3://udacity-dend/log-data

## The star Schema 
### Fact table:
* songplays (parquet files partitioned by ("year","month")):
            + songplay_id long
            + start_time timestamp
            + user_id string
            + level string
            + song_id string
            + artist_id string
            + session_id long
            + location string  
            + user_agent string
            + year integer
            + month integer
            
### Dimension tables
* users:
    + user_id string,
    + first_name string,
    + last_name string,
    + gender string,
    + level string 

* songs (parquet files partitioned by ("year","artist_id")):
    + song_id string,
    + title  string,
    + artist_id strong,
    + year long,
    + duration double

* artists:
    + artist_id string,
    + name string,
    + location string,
    + latitude  double,
    + longitude double

* time (parquet files partitioned by ("year", "month")):
    + start_time timestamp,
    + hour integer,
    + day  integer,
    + week integer,
    + month  integer,
    + year  integer,
    + weekday string


## How to run the script:

Before running any codes in the terminal, be sure you have all the required AWS credentials in force (AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY) in dl.cfg file (which is a config file). These are personal data and must be filled in by each people willing to use the code. Replace the asterisk signs in the df.cfg file with your personal credentials.

Once it is done, you must also replace the "output data" variable in the main function of the script etl.py with an appropriate S3 bucket. I advice users to create a S3 bucket with a meaningfull name and an output folder in it, which will contain the outputed parquet files.

Once it is done, in the terminal, type the two following commands:
* python etl.py

After some time, the parquet files will be produced and saved in the output S3 bucket.
To fasten the computation, an EMR cluster could be set up and used instead of a local environment.

The five parquet files produced are called:
* songs.parquet
* artists.parquet
* users_table.parquet
* time_table.parquet
* songplays_table.parquet