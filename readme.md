# memesql4

This is prototype Python/Postgres implementation of [Memelang v4](https://memelang.net/04/). This Python script receives Memelang queries, converts them to SQL, executes them on a Postgres database, then returns results as a Memelang string.

## Files

* *db.py* Postgres database configurations and helper functions
* *memelang.py* library to decode Memelang queries and execute in Postgres
* *presidents.meme* example Memelang data for the U.S. presidents

## Installation

Installation on Ubuntu:

	# Install packages
	sudo apt install -y git postgresql python3 python3-psycopg2
	sudo systemctl start postgresql
	sudo systemctl enable postgresql
	
	# Download files
	git clone https://github.com/memelang-net/memesql4.git memesql
	cd memesql

	# Configure the conf.py file according to your Postgres settings
	# Create database and tables
	sudo python3 ./memelang.py install

	# (Optional) load example presidents data
	python3 ./memelang.py file ./presidents.meme


## Example CLI Usage

Execute a query:

	python3 ./memelang.py get "student=JohnAdams ="

	# Output:
	student=JohnAdams college=Harvard



## Legal

Copyright 2025 HOLTWORK LLC. Patents Pending.