import gzip
import pandas
import httpagentparser

class ETL_Metrics:
    """This class Runs a ETL service, which takes in the data and produces top 5 metrics """

    def __init__(self):
        """
        """
        self.path = []
        self.file_content = []
        self.top_5 = []
        self.df = None


    def read_file(self):
        """

        :return: Pandas dataframe
        """
        with gzip.open(self.path, "r") as file:
            file_content = file.read()
        print(file_content)


    def setup_data(self):
        """

        :return:
        """
        df = pandas.read_csv(self.file_content, sep="\t")
        df = df.assign(browser= httpagentparser.simple_detect(df.user_agent_string)[0], os= httpagentparser.simple_detect(df.user_agent_string)[1])




    def compute_top_5(self):
        """"
        """


    def run(self):
        """

        :return:
        """
        self.read_file()
        self.setup_data()
        self.compute_top_5()

        print("Top 5")


if __name__ == "__main__":
    print("ETL pipeline for Top 5 metrics")
    print("These are the following parameters used: ")
    ETL_Metrics().run()