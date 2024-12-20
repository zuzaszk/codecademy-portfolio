# Overview
*You’ll be working with a mock database of long-term cancelled subscribers for a fictional subscription company. This database is regularly updated from multiple sources, and needs to be routinely cleaned and transformed into usable shape with as little human intervention as possible.*

*For this project, you’ll build a data engineering pipeline to regularly transform a messy database into a clean source of truth for an analytics team.*

## Execution
To run the tests, execute the following command:
```
python tests.py
```
To run the main program, you can either run all the cells in the [Jupyter Notebook `(main.ipynb)`](/main.ipynb) or execute the following command:
```
python main.py
```
Another solution to run everything is just executing the `run.sh` script with the following Linux command:
```
./run.sh
```
Note that it is, as already mentioned, a Linux script.

## Explanation
The project contains the following folders and files:
- `dev/` Development folder. Here we have the original database and temporarily the new one, on which we are working, as well as the logs.
- `prod/` If the tests are passed and database is updated successfully, the cleaned database and the CSV file are moved here.
- `main.ipynb` Jupyter Notebook for working with the database. Contains all the processes, except testing and logging.
- `main.py` The same as above, but in a form of functions and enriched with logging.
- `tests.py` Tests for the functions from `main.py`.
- `run.sh` Bash script for executing the python scripts.