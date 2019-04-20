The purpose of this project is to enable a hypothetical music streaming startup analyze data they've been collecting on songs and user activity on their new app. Currently, the data resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs. This codebase contains modules to create a relational database schema optimized for analytical (OLAP) queries. It also contains an ETL pipeline to read in, parse and transform the log data and populate the relational database implemented in PostgreSQL.  

## Requirements

- PostgreSQL database server
- Python `psycopg2` package as an API to PostgreSQL
- Python `pandas` package
- Jupyter Notebook 

## Running the code

- Run from your console `python create_tables.py` to create a database and the relevant tables.
- Run from your console `python etl.py` to extract the data from the `json` files and to parse, tranform and insert it into PostgreSQL database.
- Open in Jupyter Notebook the `etl` notebook (already populated with certain queries) to test the database and the data therein.  