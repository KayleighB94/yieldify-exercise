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
        self.etl.path = r"C:\Users\Kayleigh Bellis\Desktop\yieldify-exercise\test\resources\test_input_data.gz"

    def test_read_file(self):
        """
        This test checks that the read_file method is working as it should by comparing it to the expected data.
        """
        expected_data = [
            {"date":"2014-10-12", "time":"17:01:01", "user":"f4fdd9e55192e94758eb079ec6e24b219fe7d71e"},
            {"date":"2014-10-12", "time":"17:01:01", "user":"0ae531264993367571e487fb486b13ea412aae3d"},
            {"date":"2014-10-12", "time":"17:01:01", "user":"c5ac174ee153f7e570b179071f702bacfa347acf"},
            {"date":"2014-10-12", "time":"17:01:01", "user":"2d86766f9908fde4153a1f0998777d3aa78c3ad5"},
            {"date":"2014-10-12", "time":"17:01:01", "user":"3938fffe5c0a131f51df5c4ce3128c5edaf572c8"}
        ]
        expected_df = pandas.DataFrame(expected_data)
        self.etl.read_file()

        print("expected")
        print(expected_df)
        print("output")
        print(self.etl.df)

        assert(expected_df.equals(self.etl.df))

    def test_setup_data(self):
        """
        This test checks that the setup_data method is converting IPs and user_agent_strings into their correct
        components.
        """
        ## Creating the input data for that method
        input_data = [
            {"IP": '92.238.71.10', "user_agent_string": "Mozilla/5.0 (iPad; CPU OS 7_1_2 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) Version/7.0 Mobile/11D257 Safari/9537.53"},
            {"IP": '2.26.44.196',"user_agent_string": "Mozilla/5.0 (Linux; Android 4.4.2; GT-I9505 Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.102 Mobile Safari/537.36"},
            {"IP": '194.81.33.57', "user_agent_string": "Mozilla/5.0 (Linux; Android 4.4.4; Nexus 7 Build/KTU84P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.102 Safari/537.36"}
        ]
        self.etl.df = pandas.DataFrame(input_data)

        ## Running the method
        self.etl.setup_data()

        ## Creating expected data
        expected_data = [
            {"IP": "92.238.71.109", "user_agent_string": "Mozilla/5.0 (iPad; CPU OS 7_1_2 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) Version/7.0 Mobile/11D257 Safari/9537.53",
             "country":"United Kingdom", "city":"Jarrow", "browser_family": "Safari", "os_family":"iOS"},
            {"IP": "2.26.44.196", "user_agent_string": "Mozilla/5.0 (Linux; Android 4.4.2; GT-I9505 Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.102 Mobile Safari/537.36",
             "country":"United Kingdom", "city":"Manchester", "browser_family": "Chrome", "os_family": "Linux"},
            {"IP": "194.81.33.57", "user_agent_string": "Mozilla/5.0 (Linux; Android 4.4.4; Nexus 7 Build/KTU84P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.102 Safari/537.36",
             "country":"United Kingdom", "city":"Liverpool", "browser_family": "Chrome", "os_family": "Linux"}
        ]
        expected_df = pandas.DataFrame(expected_data).reset_index()[["IP", "user_agent_string", "country", "city", "browser_family", "os_family"]]

        ## Asserting that the two dataset are equal to each other
        print("expected")
        print(expected_df)
        print("output")
        print(self.etl.df)
        print("expected")
        print(expected_df.columns.to_series().groupby(expected_df.dtypes).groups)
        print("output")
        print(self.etl.df.columns.to_series().groupby(self.etl.df.dtypes).groups)

        assert expected_df.equals(self.etl.df.reset_index()[["IP", "user_agent_string", "country", "city", "browser_family", "os_family"]])

    def test_compute_top(self):
        """
        This test checks that the compute_top method groups by the country then counts all the rows associated with
        that country. This is checked against expected data.
        """
        input_data = [
            {"user": "213343524", "country":"United Kingdom"},
            {"user": "213343524", "country":"United Kingdom"},
            {"user": "211323242", "country":"United Kingdom"},
            {"user": "216747575", "country": "United Kingdom"},
            {"user": "216747575", "country": "United Kingdom"},
            {"user": "219787576", "country": "United Kingdom"},
            {"user": "334343434", "country": "USA"},
            {"user": "343343434", "country": "USA"},
            {"user": "345454677", "country": "USA"},
            {"user": "325546566", "country": "USA"},
            {"user": "325757785", "country": "USA"},
            {"user": "459797867", "country": "Brazil"},
            {"user": "412143566", "country": "Brazil"},
            {"user": "423546664", "country": "Brazil"},
            {"user": "456362546", "country": "Brazil"},
            {"user": "524224823", "country": "France"},
            {"user": "543242732", "country": "France"},
            {"user": "543236736", "country": "France"},
            {"user": "623235378", "country": "Spain"},
            {"user": "634231545", "country": "Spain"},
            {"user": "734242463", "country": "Italy"},

        ]
        self.etl.df = pandas.DataFrame(input_data)

        ## Running the method
        top_5_countries = self.etl.compute_top("country")

        ## Creating expected data
        expected_data = [
            {"country":"United Kingdom", "count":6},
            {"country": "USA", "count":5},
            {"country": "Brazil", "count":4},
            {"country": "France", "count":3},
            {"country": "Spain", "count":2},
            {"country": "Italy", "count":1}
        ]
        expected_df = pandas.DataFrame(expected_data).sort_values("count").reset_index()[["country", "count"]]

        assert expected_df.equals(top_5_countries.sort_values("count").reset_index()[["country", "count"]])

    def test_compute_top_unique(self):
        """
        This test checks that the compute_top method groups by the country then counts all the unique users associated
        with that country. This is checked against expected data.
        """
        input_data = [
            {"user": "213343524", "country": "United Kingdom"},
            {"user": "213343524", "country": "United Kingdom"},
            {"user": "211323242", "country": "United Kingdom"},
            {"user": "216747575", "country": "United Kingdom"},
            {"user": "216747575", "country": "United Kingdom"},
            {"user": "219787576", "country": "United Kingdom"},
            {"user": "334343434", "country": "USA"},
            {"user": "343343434", "country": "USA"},
            {"user": "345454677", "country": "USA"},
            {"user": "325546566", "country": "USA"},
            {"user": "325757785", "country": "USA"},
            {"user": "459797867", "country": "Brazil"},
            {"user": "412143566", "country": "Brazil"},
            {"user": "423546664", "country": "Brazil"},
            {"user": "456362546", "country": "Brazil"},
            {"user": "524224823", "country": "France"},
            {"user": "543242732", "country": "France"},
            {"user": "543236736", "country": "France"},
            {"user": "623235378", "country": "Spain"},
            {"user": "634231545", "country": "Spain"},
            {"user": "734242463", "country": "Italy"},

        ]
        self.etl.df = pandas.DataFrame(input_data)

        ## Running the method
        top_5_countries = self.etl.compute_top("country", "user")

        ## Creating expected data
        expected_data = [
            {"country": "United Kingdom", "count": 4},
            {"country": "USA", "count": 4},
            {"country": "Brazil", "count": 4},
            {"country": "France", "count": 3},
            {"country": "Spain", "count": 2},
            {"country": "Italy", "count": 1}
        ]
        expected_df = pandas.DataFrame(expected_data).sort_values("count").reset_index()[["country", "count"]]

        assert expected_df.equals(top_5_countries.sort_values("count").reset_index()[["country", "count"]])
