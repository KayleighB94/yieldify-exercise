# Yieldify-Exercise
This project contains my solution to Yieldify Home exercise written in Python 3.6.
Within it, there is a class called ETL_Metrics which takes in a gzip file of data and processes the IP and user agent
string variables into countries, cities, browser family and OS. 

From there it can then follow one of two paths, send the data to the API or print some top 5 statistics to the stdout.
For the top 5 stdout option, these are the following metrics produced:
* Top 5 countries per event
* Top 5 cities per event
* Top 5 browsers per unique user
* Top 5 OS per unique user

For the API, this will provide a percentage breakdown of the component used. These can be find by typing into the url:
* /stats/browser
* /stats/os

You can also have these within a certain time frame, if no time frame is set then it will give you the overall percentage
breakdown for the data within the database.
To set a time frame with a start date and end data, call the URL like the example below:
 ```shell
 /stats/os?start_date=<timestamp>&end_date=<timestamp>
```
To have just a start date, call the URL like the example below:
 ```shell
 /stats/os?start_date=<timestamp>
```
To have just a end date, call the URL like the example below:
 ```shell
 /stats/os?end_date=<timestamp>
```

# Setting Up the Environment
This package has been created with Python 3.6 so needs to be run on a Python 3.6 environment along with the extra
modules within the requirements.txt
You can build a clean python environment by following the [venv package](https://docs.python.org/3/tutorial/venv.html) or
if you are using intellij/pycharm you can create one when setting up you python interpreter.

After you've created your environment, you can run the following command in order to install the rest of the modules
needed to run this package.
```shell
pip install -r requirements.txt 
```
http://127.0.0.1:5000/stats/os?start_date=1422205285&end_date=1432573285
# Running the package
There are two ways of running this package, through the etl system and through the API. The API will read the data straight off of the databse, where as the etl system will parse the input data, write it to the database then will either call the API or print some metrics to stdout.


The etl_system takes in the following runtime parameters:
   * help -> can be called as -h, --help
   * path -> can be called as -p, --path
   * option -> can be called as -o, --option 

In order to run the ETL, open up a terminal in this project directory and run a command like the below example:
(To get metrics print to stdout)
```shell
python yieldify_exercise\etl_system.py --path /home/input_data/data1.gzip -o stdout
```
(To parse the data for API use)
```shell
python yieldify_exercise\etl_system.py --path /home/input_data/data1.gzip -o api
```

To just run the API, open up a terminal in this project directory and run a command like the below example:
```shell
python yieldify_exercise\api.py
```

# Running the tests
The test created for the methods within the ETL_Metrics class were written using pythons unittest library.
In order to run the tests for the methods within the class created, you need to navigate to the project
directory then run the command below in the appropriate environment.
```shell
python -m unittest
```
This will run all the tests within the test directory.
#Improvements
* Make the API look nicer
* More unit test
* Add more error handling into the methods
