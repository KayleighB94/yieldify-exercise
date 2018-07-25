# yieldify-exercise
This project contains my solution to Yieldify Home exercise written in Python 3.6.
Within it, there is a class called ETL_Metrics which

# Setting Up the Environment

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
