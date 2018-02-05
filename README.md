# File Parser

This project takes in specification files data files described [here](https://gist.github.com/dmshann0n/6dfba5ebdebe1098d579) and loads them into a database.

# Prerequisites

This project uses Postgres as its datastore, and requires installation and setup before running the project.

To install Postgres on a Mac, please follow this [setup guide](https://gist.github.com/sgnl/609557ebacd3378f3b72)

Once Postgres is running on your machine, you'll need to create the tables necessary for this project:

``` 
cd file_parser
python create_tables.py
```

# Run the project

Python 2.7.10 was used to develop this project.

Create a virtual environment within this project and activate it.

``` 
cd file_parser
virtualenv venv
. venv/bin/activate
```

Then, install the requirements:

``` 
pip install -r requirements.txt
```

Run the file parser:

```
python run_file_parser.py
```

Drop a specification file into the spec directory or a data file into the data directory to see the file parser in action.

# Run the tests

```
python -m unittest discover .
```