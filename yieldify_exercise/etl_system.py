import gzip
import pandas
import httpagentparser
from geoip import geolite2


class ETL_Metrics:
    """This class Runs a ETL service, which takes in the data and produces top 5 metrics """

    def __init__(self):
        """
        """
        self.path = []
        self.top_5 = None
        self.df = None


    def read_file(self):
        """

        :return: Pandas dataframe
        """
        columns = ["date", "time", "user_id", "url", "IP", "user_agent_string"]
        self.df = pandas.read_table(self.path, compression= "gzip",names=columns)


    def setup_data(self):
        """

        :return:
        """
        #self.df["country"] = self.df.apply(lambda k: geolite2.lookup(k["IP"]).get("country").get("name"))
        self.df["browser_family"] = self.df.apply(lambda k:httpagentparser.detect(k["user_agent_string"], {}).get("browser").get("name"), axis=1)
        self.df["os_family"] = self.df.apply(lambda k:httpagentparser.detect(k["user_agent_string"], {}).get("os").get("name"), axis=1)


    def compute_top_5(self, groupByCols):
        """"
        """
        top_5 = self.df #.groupBy(groupByCols).agg(["count"])
        return top_5


    def run(self):
        """

        :return:
        """
        self.read_file()
        self.setup_data()
        ## Top 5 Countries per event
        top_5_countries = self.compute_top_5()


        print("Top 5 Countries per event")
        print(top_5_countries)


if __name__ == "__main__":
    print("ETL pipeline for Top 5 metrics")
    print("These are the following parameters used: ")
    ETL_Metrics().run()