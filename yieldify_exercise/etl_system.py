import pandas
import httpagentparser
from geolite2 import geolite2
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
        The method creates a list of strings, which will be used for columns names:
            ["date", "time", "user_id", "url", "IP", "user_agent_string"]

        This is then used along with the path
        """
        columns = ["date", "time", "user_id", "url", "IP", "user_agent_string"]
        self.df = pandas.read_table(self.path, compression= "gzip",names=columns)


    def setup_data(self):
        """

        :return:
        """
        reader = geolite2.reader()
        self.df["country"] = self.df.apply(lambda k:reader.get(k["IP"])['country']['names']['en'], axis=1)
        self.df["city"] = self.df.apply(lambda k:reader.get(k["IP"])['city']['names']['en'], axis=1)
        geolite2.close()
        self.df["browser_family"] = self.df.apply(lambda k:httpagentparser.detect(k["user_agent_string"], {}).get("browser").get("name"), axis=1)
        self.df["os_family"] = self.df.apply(lambda k:httpagentparser.detect(k["user_agent_string"], {}).get("os").get("name"), axis=1)



    def compute_top_5(self, groupByCols):
        """"
        """
        top_5 = self.df.groupby(groupByCols).size().reset_index(name='count')
        return top_5


    def run(self):
        """

        :return:
        """
        self.read_file()
        self.setup_data()

        ## Top 5 per event
        top_5_countries = self.compute_top_5(["country"])
        top_5_cities = self.compute_top_5(["city"])

        ## Top 5 per unquie user
        top_5_browser_per = self.compute_top_5(["browser"])
        top_5_os_per = self.compute_top_5(["os"])

        print("Top 5 Countries per event")
        print(top_5_countries)
        return top_5_countries


if __name__ == "__main__":
    print("ETL pipeline for Top 5 metrics")
    print("These are the following parameters used: ")
    ETL_Metrics().run()