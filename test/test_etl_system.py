import unittest
import pandas

from yieldify_exercise.etl_system import ETL_Metrics

class TestEtlSystem(unittest.TestCase):
    """
    This class holds all the tests for each method within the elt_system class
    """

    def setUp(self):
        """
        This setup method allows us to setup an instance of our etl_metrics class and then call the individual methods
        within it for our tests below.
        """
        self.etl = ETL_Metrics()
        self.etl.path = r"C:\Users\Kayleigh Bellis\Desktop\yieldify-exercise\test\resources\input_data.gz"

    def test_read_file(self):
        """

        :return:
        """
        self.etl.read_file()

        #assert(expected.equals(self.etl.df)

    def test_setup_data(self):
        """

        :return:
        """
        ## Creating the input data for that method
        input_data = [
            {"IP": "92.238.71.109", "user_agent_string": "Mozilla/5.0 (iPad; CPU OS 7_1_2 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) Version/7.0 Mobile/11D257 Safari/9537.53"},
            {"IP": "2.26.44.196", "user_agent_string": "Mozilla/5.0 (Linux; Android 4.4.2; GT-I9505 Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.102 Mobile Safari/537.36"},
            {"IP": "194.81.33.57", "user_agent_string": "Mozilla/5.0 (Linux; Android 4.4.4; Nexus 7 Build/KTU84P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.102 Safari/537.36"}
        ]
        self.etl.df = pandas.DataFrame(input_data)

        ## Running the method
        self.etl.setup_data()

        ## Creating expected data
        expected_data = [
            {"IP": "92.238.71.109", "user_agent_string": "Mozilla/5.0 (iPad; CPU OS 7_1_2 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) Version/7.0 Mobile/11D257 Safari/9537.53",
             "country":"United Kingdom", "city":"Manchester", "browser_family": "Safari", "os_family":"iOS"},
            {"IP": "2.26.44.196", "user_agent_string": "Mozilla/5.0 (Linux; Android 4.4.2; GT-I9505 Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.102 Mobile Safari/537.36",
             "country":"United Kingdom", "city":"Harlech", "browser_family": "Chrome", "os_family": "Linux"},
            {"IP": "194.81.33.57", "user_agent_string": "Mozilla/5.0 (Linux; Android 4.4.4; Nexus 7 Build/KTU84P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.102 Safari/537.36",
             "country":"United Kingdom", "city":"Liverpool", "browser_family": "Chrome", "os_family": "Linux"}
        ]
        expected_df = pandas.DataFrame(expected_data)[["IP", "user_agent_string", "country", "city", "browser_family", "os_family"]]

        ## Asserting that the two dataset are equal to each other
        assert expected_df.equals(self.etl.df[["IP", "user_agent_string", "country", "city", "browser_family", "os_family"]])

    # def test_compute_top_5(self):
    #     """
    #
    #     :return:
    #     """
    #
    #     self.etl.compute_top_5()
    #
    #
    # def test_run(self):
    #     """
    #
    #     :return:
    #     """
    #     self.etl.run()