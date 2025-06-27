import unittest
from unittest.mock import patch

class TestFunctieIdOphalen(unittest.TestCase):

    @patch('pyodbc_connection.main.conn')
    def voorbeeld_test(self, mock_conn):
        response = ...
        self.assertEqual(response, 200)
