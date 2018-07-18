import unittest
from yieldify_exercise.etl_system import etl_metrics
class TestEtlSystem(unittest.TestCase):
    """
    This class holds all the tests for each method within the elt_system class
    """

    def setUp(self):
        """
        This setup method allows us to setup an instance of our etl_metrics class and then call the individual methods
        within it for our tests below.
        """
        self.etl = etl_metrics()

    def test_read_file(self):
        """

        :return:
        """

    def test_setup_data(self):
        """

        :return:
        """

        self.etl.setup_data()

    def test_compute_top_5(self):
        """

        :return:
        """

        self.etl.compute_top_5()


    def test_run(self):
        """

        :return:
        """
        self.etl.run()