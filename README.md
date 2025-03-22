## OECDapi - Resilient and Asynchronous 
This package is a convenient wrapper around the publicly available [OECD Data Explorer APIs](https://data-explorer.oecd.org/),
which adds resilience and extra consistency checks, while also making it easier to work with (see the important information below).

The main class is the (`OECD` class)[modules/oecd.py], which has two distinct modes of operating: 
 - Legacy mode: It can be run in 'legacy' mode, which involves sending URL requests to the OECD Data Explorer APIs. A rudimentary approach to retrials is implemented.
 - Celery mode: Alternatively it can be run in 'celery' mode, where a [Celery backend](https://docs.celeryq.dev/en/stable/getting-started/introduction.html) 
ensures proper task queuing, exceptions handling and asynchronous operation using [redis](https://redis.io/).

The necessary containers are all defined in the docker-compose file, which also includes an SQL database container
where results can be stored, and the interaction with SQL servers is conveniently build into the (`OECD` class)[modules/oecd.py].

## Installation
Clone the repository and follow the usage instructions below to integrate it into your workflow.

## Usage Instructions
The newest data available for a given statistical series from the publicly available OECD API can be 
queried using the method `oecd_query()`.

Some commonly used data series are predefined for convenience with their own methods:
 - key_economic_indicators
 - business_tendency_surveys
 - composite_leading_indicators
 - financial_indicators
 - production_and_sales
 - consumer_price_indices

 Find more data series here: https://data-explorer.oecd.org/

### Celery and redis backends (Celery mode)
In Celery mode, Async objects are collected and can afterward be processed once all data has 
been downloaded using the method `process_celery_results`.
A Docker container for the optional redis backend is defined in the docker-compose file, 
with environment variables defined in the `.env` file.
The necessary setup for Celery is already taken care of, ensuring that the tasks and celery installation
work seamlessly.

### Database backend (optional)
The `OECD` class is readily integrated with an SQL configuration defined through the environment variables defined in the `.env` file.
The downloaded data database can be written to the database using the `update_db`, where the input dictionary determines the table name.
A number of utils ensuring correct behaviour of the database operations are defined in `db_connection.py` module.

## Important information
This project is in no way associated, endorsed, or approved by OECD. 
It's an open-source tool that uses OECD's publicly available APIs, and is intended for research and educational purposes.
Please read [OECD's terms and conditions](https://www.oecd.org/en/about/terms-conditions.html).