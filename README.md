# yieldify-exercise
This project contains my solution to Yieldify Home exercise written in Python 3.6.
Within it, there is a class called ETL_Metrics which takes in a gzip file of data and processes the IP and user agent
string variables into countries, cities, browser family and OS. 

From there it then computes the top 5 statistics for:
* Top 5 countries per event
* Top 5 cities per event
* Top 5 browsers per unique user
* Top 5 OS per unique user

# Setting Up the Environment
This package has been created with Python 3.6 so needs to be run on a pYhton 3.6 enviroment along with the extra
modules within the requirements.txt
You can build a clean python enviorment by follwing the [venv package](https://docs.python.org/3/tutorial/venv.html) or
if you are using intellji/pycharm you can create one when setting up you python interpreter.

After you've created your enviorment, you can run the following command in order to install the rest of the modules
needed to run this package.
```shell
pip install -r requirements.txt 
```

# Running the package

The etl_system takes in the following runtime parameters:
   * help -> can be called as -h, --help
   * path -> can be called as -p, --path

In order to run the ETL, open up a terminal in this project directory and run a command like the below example:
```shell
python etl_system.py --path /home/input_data/data1.gzip
```

# Running the tests
The test created for the methods within the ETL_Metrics class were written using pythons unittest library.
In order to run the tests for the methods within the class created, you need to navigate to the test folder of this
project then run the command below in the appropriate environment
```shell
python -m unittest
```
This will run all the tests within the test directory.
#Improvements
* Add an API on top of the existing system
* Stored the output of the system
* More unit test
* Add more error handling into the methods
