
# Sparkify Data Warehouse ETL Project

## Project Overview

This project involves building an ETL pipeline that extracts data from S3, stages it in Redshift, and transforms it into a set of dimensional tables optimized for analytical queries. The data comes from a music streaming startup called Sparkify, which has grown its user base and song database and now wants to move its data and processes to the cloud.

The ETL pipeline will help Sparkify's analytics team to perform song play analysis to understand user behavior and trends in music streaming.

## Purpose of the Database

The purpose of this database is to enable Sparkify's analytics team to query and analyze data related to user activities and song plays. By moving the data to a data warehouse on AWS Redshift and structuring it in a star schema, the database is optimized for complex queries and aggregations, allowing Sparkify to gain valuable insights such as:

- Which songs are most popular among users?
- What time of day do users most frequently play songs?
- How many songs are played by each user?

These insights will help Sparkify make data-driven decisions to enhance user experience and optimize their music streaming service.

## Database Schema Design

### Schema Overview

The database is designed using a **star schema**. The star schema is a type of database schema that is optimized for online analytical processing (OLAP) and complex queries. It consists of a central **fact table** that references several **dimension tables**. This design is chosen for its simplicity and efficiency in querying large datasets, particularly for aggregate queries.

### Fact Table

- **songplays**: Records in event data associated with song plays. This table includes foreign keys to all the dimension tables.

  | Column Name  | Data Type | Description                                 |
  |--------------|-----------|---------------------------------------------|
  | songplay_id  | INTEGER   | Primary key (auto-incremented)              |
  | start_time   | TIMESTAMP | Timestamp of the song play event            |
  | user_id      | INTEGER   | ID of the user who played the song          |
  | level        | VARCHAR   | User level (e.g., free or paid)             |
  | song_id      | VARCHAR   | ID of the song being played                 |
  | artist_id    | VARCHAR   | ID of the artist                            |
  | session_id   | INTEGER   | ID of the user session                      |
  | location     | VARCHAR   | Location of the user                        |
  | user_agent   | TEXT      | User agent string (browser, OS, etc.)       |

### Dimension Tables

- **users**: Information about users in the app.

  | Column Name  | Data Type | Description                                 |
  |--------------|-----------|---------------------------------------------|
  | user_id      | INTEGER   | Primary key                                 |
  | first_name   | VARCHAR   | First name of the user                      |
  | last_name    | VARCHAR   | Last name of the user                       |
  | gender       | VARCHAR   | Gender of the user                          |
  | level        | VARCHAR   | User level (e.g., free or paid)             |

- **songs**: Information about songs in the music database.

  | Column Name  | Data Type | Description                                 |
  |--------------|-----------|---------------------------------------------|
  | song_id      | VARCHAR   | Primary key                                 |
  | title        | VARCHAR   | Title of the song                           |
  | artist_id    | VARCHAR   | ID of the artist                            |
  | year         | INTEGER   | Year the song was released                  |
  | duration     | FLOAT     | Duration of the song                        |

- **artists**: Information about artists in the music database.

  | Column Name  | Data Type | Description                                 |
  |--------------|-----------|---------------------------------------------|
  | artist_id    | VARCHAR   | Primary key                                 |
  | name         | VARCHAR   | Name of the artist                          |
  | location     | VARCHAR   | Location of the artist                      |
  | latitude     | FLOAT     | Latitude of the artist's location           |
  | longitude    | FLOAT     | Longitude of the artist's location          |

- **time**: Timestamps of records in `songplays` broken down into specific units.

  | Column Name  | Data Type | Description                                 |
  |--------------|-----------|---------------------------------------------|
  | start_time   | TIMESTAMP | Primary key                                 |
  | hour         | INTEGER   | Hour of the day                             |
  | day          | INTEGER   | Day of the month                            |
  | week         | INTEGER   | Week of the year                            |
  | month        | INTEGER   | Month of the year                           |
  | year         | INTEGER   | Year                                        |
  | weekday      | INTEGER   | Day of the week                             |

## ETL Pipeline

### ETL Pipeline Overview

The ETL pipeline extracts data from JSON files stored in S3, loads it into staging tables in Redshift, and then processes the data into the final fact and dimension tables. The pipeline is implemented in the `etl.py` script, which includes the following steps:

1. **Load data from S3 to Staging Tables**: 
   - Use the `COPY` command to load raw JSON data from S3 into the `staging_events` and `staging_songs` tables in Redshift.
   
2. **Transform and Load Data into Final Tables**:
   - Process the staging tables to extract relevant columns and load them into the `songplays`, `users`, `songs`, `artists`, and `time` tables.

### Example Queries for Song Play Analysis

Here are some example queries that could be run on the final tables to provide insights into song play patterns and user behavior:

1. **Most Played Songs**:
```
SELECT s.title, COUNT(sp.song_id) AS play_count
FROM songplays sp
JOIN songs s ON sp.song_id = s.song_id
GROUP BY s.title
ORDER BY play_count DESC
LIMIT 10;
```
   
Description: This query retrieves the top 10 most played songs in the Sparkify app.



