import getopt
import sys

import httpagentparser
import pandas
import sqlite3
from geolite2 import geolite2


class ETL_Metrics:
    """This class Runs a ETL service, which takes in the data and produces top 5 metrics """

    def __init__(self):
        """
        This method initializes three of the objects used throughout the class.
        """
        self.path = []
        self.top_5 = None
        self.df = None

    def read_file(self):
        """
        The method creates a list of strings, which will be used for columns names:
            ["date", "time", "user_id", "url", "IP", "user_agent_string"]

        This is then used along with the path to read in a gzip file to a pandas table.
        """
        columns = ["date", "time", "user", "url", "IP", "user_agent_string"]
        self.df = pandas.read_table(self.path, compression="gzip", names=columns)

    def none_check(self, value, method_type, item):
        """
        This method first checks whether the value parameter that has been passed in is not none, if it is then the
        method will return none.
        If it's not then if the method_type parameter is equal to "user_agent_str" then the method gets the name key's
        value from the dictionary. If the parameter is set to "IP"  then the dictionary is searched for keys which
        relate to the item parameter (example: item = country and there's no country as a key in the dictionary but
        there is registered_country as a key, this one would then be picked). It would then return the value from the
        relating key.

        :param value: Dictionary - This holds all the information relating to an IP or user agent string
        :param method_type: String - This is a string which either says user_agent_str or IP
        :param item: String - This is the name of what category we want out of the dictionary
        :return:
        """
        if (method_type == "user_agent_str") & (value is not None):
            return value.get("name")
        elif (method_type == "IP") & (value is not None):
            for key in value.keys():
                if item in key:
                    return value[key]['names']['en']
            return None
        else:
            return None

    def check_ip(self, ip):
        """
        This method splits the IP string into a list of string, provided that the IPs are separated by a comma space.
        It then searches through them to see if a IP is there or not. It takes the first available IP.

        :param ip: string - This is the string that holds all the IPs which have been separated by a comma
        :return:
        """
        ip_list = ip.split(", ")
        for i in ip_list:
            if i != "unknown":
                return i

    def setup_data(self):
        """
        This method uses the classes pandas dataframe and saves back to it.

        It first initializes the connection with geolite2, then for each IP in the data, it finds out the country and
        city of that IP and stores this as a new column onto our pandas dataframe. After it has does this we close the
        connection to geolite2.

        After that will call httpagentparser which converts our user_agent_string into its browser family and it's OS,
        this is done for each row and is saved as new columns onto our pandas dataframe.
        """
        reader = geolite2.reader()
        self.df["country"] = self.df.apply(lambda k: (
            self.none_check(reader.get(self.check_ip(k["IP"])), "IP", "country") if k["IP"] not in ["-",
                                                                                                    None] else None),
                                           axis=1)
        self.df["city"] = self.df.apply(lambda k: (
            self.none_check(reader.get(self.check_ip(k["IP"])), "IP", "city") if k["IP"] not in ["-", None] else None),
                                        axis=1)
        geolite2.close()
        self.df["browser_family"] = self.df.apply(
            lambda k: self.none_check(httpagentparser.detect(k["user_agent_string"], {}).get("browser"),
                                      "user_agent_str", "browser") if k["IP"] not in ["-", None] else None, axis=1)
        self.df["os_family"] = self.df.apply(
            lambda k: self.none_check(httpagentparser.detect(k["user_agent_string"], {}).get("os"), "user_agent_str",
                                      "os") if k["IP"] not in ["-", None] else None, axis=1)

    def compute_top(self, group_by_cols, unique=None):
        """
        This method first checks whether we are counting the unique rows of a column or counting all the rows,
        this is done through the unique parameter. If it hasn't been passed in then it defaults to None and the
        dataframe is then grouped by the groupByCols parameter and the rows are counted.

        If it has been passed then the dataframe is then grouped by the groupByCols parameter and the number of unique
        values in the unique parameter column is counted.

        The dataframe is then returned.

        :param groupByCols: List[Strings] - List of the column names you want to group by
        :param unique: String - Name of column to count unique values of, defaults to None if you want to count all the
                                rows
        :return: Pandas DataFrame
        """
        if unique is None:
            grouped_df = self.df.groupby(group_by_cols, as_index=False).size().reset_index(name='count')
        else:
            grouped_df = self.df.groupby(group_by_cols, as_index=False).agg({unique: pandas.Series.nunique})
        return grouped_df

    def write_to_db(self):
        conn = sqlite3.connect('data.db')
        pandas.io.sql.write_frame(self.df, name='', con=conn)
        conn.close()

    def main(self):
        """
        This method is the main one which first gets the arguments passed in through the command line. Then it calls
        the other methods in order and prints out the statistics of the top 5 of the following:
        Countries per event (row)
        Cities per event (row)
        Browser family based on unique users
        OS based on unique users


        """
        # Creating a help string, if the user inputs the wrong parameters or no parameters
        help_str = "etl_system.py -h <help> -p <path_to_input_data>"
        try:
            opts, args = getopt.getopt(sys.argv[1:], "hp:")
        except getopt.GetoptError as err:
            print(err)
            print(help_str)
            sys.exit(2)

        for opt, arg in opts:
            if opt in ("-h", "--help"):
                print(help_str)
                sys.exit()
            elif opt in ("-p", "--path"):
                self.path = arg
            else:
                print("Unhandled Options")
                print(help_str)

        print("These are the following parameters used: ", self.path)
        # Calling two of the method that read in the data and convert the columns from IP and user string to
        # countries, cities, browsers and os'
        self.read_file()
        self.setup_data()
        # This method saves the data to a database to be query
        self.write_to_db()

        # Top 5 per event
        top_5_countries = self.compute_top(["country"])
        top_5_cities = self.compute_top(["city"])

        # Top 5 per unique user
        top_5_browser_per = self.compute_top(["browser_family"], "user")
        top_5_os_per = self.compute_top(["os_family"], "user")

        print("Top 5 Countries per event:")
        print(top_5_countries.sort_values("count", ascending=False).head())
        print("Top 5 Cities per event:")
        print(top_5_cities.sort_values("count", ascending=False).head())
        print("Top 5 Browsers per unique user:")
        print(top_5_browser_per.sort_values("user", ascending=False).head())
        print("Top 5 OS per unique user:")
        print(top_5_os_per.sort_values("user", ascending=False).head())


if __name__ == "__main__":
    print("ETL pipeline for Top 5 metrics")
    ETL_Metrics().main()
