The purpose of this project is to build analytical capabilities for a hypothetical music streaming startup, Sparkify. In the course of its operations, Sparkify collects logs on the activity of the users of their new app. Sparkify also has metadata on songs and artists. All this data comes in json format.

This codebase contains modules to create a relational database schema optimized for analytical (OLAP) queries. It also contains an ETL pipeline to read in, parse and transform the log data and populate the relational database implemented in PostgreSQL.  

This infrastructure will enable Sparkify to extract business intelligence from the data. For example, they will be able to aggregate and see the activity of their users by artist, song, time period, 
location, subscription type, etc. 

## ETL and database design

ETL consists of a set of procedures, written in Python, which retrieve the json objects from the 
relevant json files and read them into pandas dataframes. Then, a series of data parsing and transformations are performed to transfer the data to the relational database implemented in PostgreSQL. 

The database is based on a star schema, where the the fact table contains information about the plays of songs, such as timestamp, user ID, user type, etc. There are also 4 dimension tables, namely, users, songs, artists and time. The latter is to enable analysis by months, days, of week, etc. 

## Requirements

- PostgreSQL database server
- Python `psycopg2` package as an API to PostgreSQL
- Python `pandas` package
- Jupyter Notebook 

## Running the code

- Run from your console `python create_tables.py` to create a database and the relevant tables.
- Run from your console `python etl.py` to extract the data from the `json` files and to parse, tranform and insert it into PostgreSQL database.
- Open in Jupyter Notebook the `etl` notebook (already populated with certain queries) to test the database and the data therein.  