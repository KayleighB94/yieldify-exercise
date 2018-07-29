import os
import unittest

import httpagentparser
import pandas
from geolite2 import geolite2

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
        self.etl.path = os.getcwd() + "/test/resources/test_input_data.gz"
        self.option = "stdout"

    def test_read_file(self):
        """
        This test checks that the read_file method is working as it should by comparing it to the expected data.
        """
        # Creating expected data
        expected_data = [
            {"date": "2014-10-12", "time": "17:01:01", "user": "f4fdd9e55192e94758eb079ec6e24b219fe7d71e",
             "url": "http://92392832.2323.2323", "IP": "192.34.246.23",
             "user_agent_string": "Mozilla/5.0 (iPad; CPU OS 7_1_2 like Mac OS X)"},
            {"date": "2014-10-12", "time": "17:01:01", "user": "0ae531264993367571e487fb486b13ea412aae3d",
             "url": "http://95454545.2323.2323", "IP": "867.454.34.65",
             "user_agent_string": "Mozilla/5.0 ((Linux; Android 4.4.2; GT-I9505 Build/KOT49H)"},
            {"date": "2014-10-12", "time": "17:01:01", "user": "c5ac174ee153f7e570b179071f702bacfa347acf",
             "url": "http://92354545.2323.2323", "IP": "142.45.245.23",
             "user_agent_string": "Chrome/5.0 (Linux; Android 4.4.2; GT-I9505 Build/KOT49H)"},
            {"date": "2014-10-12", "time": "17:01:01", "user": "2d86766f9908fde4153a1f0998777d3aa78c3ad5",
             "url": "http://94857364.2323.2323", "IP": "192.178.76.56",
             "user_agent_string": "Chrome/5.0 (iPad; CPU OS 7_1_2 like Mac OS X)"},
            {"date": "2014-10-12", "time": "17:01:01", "user": "3938fffe5c0a131f51df5c4ce3128c5edaf572c8",
             "url": "http://95937726.2323.2323", "IP": "192.34.563.64",
             "user_agent_string": "Chrome/5.0 (iPad; CPU OS 7_1_2 like Mac OS X)"}
        ]
        expected_df = pandas.DataFrame(expected_data)[["date", "time", "user", "url", "IP", "user_agent_string"]]
        # Calling the read in method
        self.etl.read_file()
        # Asserting that the two dataframe are equal
        assert (expected_df.equals(self.etl.df[["date", "time", "user", "url", "IP", "user_agent_string"]]))

    def test_none_check(self):
        """
        This test check whether the none_check returns what it is suppose to depending on the input parameters.
        """
        reader = geolite2.reader()
        # Asserting that the correct country comes out
        ip_right = reader.get("92.238.71.10")
        assert (self.etl.none_check(ip_right, "IP", "country") == "United Kingdom")
        # Asserting that when there is no key with that name or similar the method returns none
        assert (self.etl.none_check(ip_right, "IP", "browser") == None)
        geolite2.close()
        # Asserting the none_check gets the name correctly
        user_agent_right = httpagentparser.detect(
            "Mozilla/5.0 (iPad; CPU OS 7_1_2 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) Version/7.0 Mobile/11D257 Safari/9537.53",
            {}).get("browser")
        assert (self.etl.none_check(user_agent_right, "user_agent_str", "browser") == "Safari")

        # Asserting that when a none value is passed for both method types, then none is returned back
        value_none = None
        assert (self.etl.none_check(value_none, "IP", "country") == None)
        assert (self.etl.none_check(value_none, "user_agent_str", "browser") == None)

        # Asserting that a differnet method type has been chosen, it will reutrned back none
        assert (self.etl.none_check(value_none, "mutliply", "country") == None)

    def test_check_ip(self):
        """
        This test checks whether the check_ip method in the ETL class is producing the correct outputs for the given
        parameters
        """
        # Checking to see if there is no ip that the method returns none
        only_unknown = "unknown"
        assert (self.etl.check_ip(only_unknown) == None)
        # Checking to see if there is a unknown first in the string then a ip ,that the ip is returned
        first_unknown = "unknown, 92.238.71.10"
        assert (self.etl.check_ip(first_unknown) == "92.238.71.10")
        # Checking to see if there is a unknown second in the string then a ip, that the ip is returned
        second_unknown = "92.238.71.10, unknown"
        assert (self.etl.check_ip(second_unknown) == "92.238.71.10")
        # Checking to see if there are only ip in the string then the first ip is returned
        no_unknown = "92.238.71.10, 2.26.44.196"
        assert (self.etl.check_ip(no_unknown) == "92.238.71.10")

    def test_setup_data(self):
        """
        This test checks that the setup_data method is converting IPs and user_agent_strings into their correct
        components.
        """
        ## Creating the input data for that method
        input_data = [
            {"IP": '92.238.71.10, 2.26.44.196',
             "user_agent_string": "Mozilla/5.0 (iPad; CPU OS 7_1_2 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) Version/7.0 Mobile/11D257 Safari/9537.53"},
            {"IP": '2.26.44.196',
             "user_agent_string": "Mozilla/5.0 (Linux; Android 4.4.2; GT-I9505 Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.102 Mobile Safari/537.36"},
            {"IP": '194.81.33.57',
             "user_agent_string": "Mozilla/5.0 (Linux; Android 4.4.4; Nexus 7 Build/KTU84P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.102 Safari/537.36"},
            {"IP": '-', "user_agent_string": '-'}
        ]
        self.etl.df = pandas.DataFrame(input_data)

        # Running the method
        self.etl.setup_data()

        # Creating expected data
        expected_data = [
            {"IP": "92.238.71.10, 2.26.44.196",
             "user_agent_string": "Mozilla/5.0 (iPad; CPU OS 7_1_2 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) Version/7.0 Mobile/11D257 Safari/9537.53",
             "country": "United Kingdom", "city": "Jarrow", "browser_family": "Safari", "os_family": "iOS"},
            {"IP": "2.26.44.196",
             "user_agent_string": "Mozilla/5.0 (Linux; Android 4.4.2; GT-I9505 Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.102 Mobile Safari/537.36",
             "country": "United Kingdom", "city": "Manchester", "browser_family": "Chrome", "os_family": "Linux"},
            {"IP": "194.81.33.57",
             "user_agent_string": "Mozilla/5.0 (Linux; Android 4.4.4; Nexus 7 Build/KTU84P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.102 Safari/537.36",
             "country": "United Kingdom", "city": "Liverpool", "browser_family": "Chrome", "os_family": "Linux"},
            {"IP": '-', "user_agent_string": '-', "country": None, "city": None, "browser_family": None,
             "os_family": None}
        ]
        expected_df = pandas.DataFrame(expected_data).reset_index()[
            ["IP", "user_agent_string", "country", "city", "browser_family", "os_family"]]
        # Asserting that the two dataset are equal to each other
        assert expected_df.equals(
            self.etl.df.reset_index()[["IP", "user_agent_string", "country", "city", "browser_family", "os_family"]])

    def test_compute_top(self):
        """
        This test checks that the compute_top method groups by the country then counts all the rows associated with
        that country. This is checked against expected data.
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

        # Running the method
        top_5_countries = self.etl.compute_top("country")

        # Creating expected data
        expected_data = [
            {"country": "United Kingdom", "count": 6},
            {"country": "USA", "count": 5},
            {"country": "Brazil", "count": 4},
            {"country": "France", "count": 3},
            {"country": "Spain", "count": 2},
            {"country": "Italy", "count": 1}
        ]
        expected_df = pandas.DataFrame(expected_data).sort_values("count").reset_index()[["country", "count"]]
        # Asserting that the two dataset are equal to each other
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

        # Running the method
        top_5_countries = self.etl.compute_top("country", "user")

        # Creating expected data
        expected_data = [
            {"country": "United Kingdom", "user": 4},
            {"country": "USA", "user": 5},
            {"country": "Brazil", "user": 4},
            {"country": "France", "user": 3},
            {"country": "Spain", "user": 2},
            {"country": "Italy", "user": 1}
        ]
        expected_df = pandas.DataFrame(expected_data).sort_values(["user", "country"]).reset_index()[
            ["country", "user"]]

        # Asserting that the two dataset are equal to each other
        assert expected_df.equals(top_5_countries.sort_values(["user", "country"]).reset_index()[["country", "user"]])