import unittest
import os
import random

from stocks.utils.db_journal import db_journal_setup, db_journal_fatal, db_journal_error, db_journal_warning, db_journal_info, db_journal_debug, db_journal_trace

class TestDbJournal(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        uri = os.getenv("STOCKS_DATABASE_URI")
        db_journal_setup(uri)


    def test_db_journal_fatal(self):
        db_journal_fatal(f"Fatal message: {random.randrange(1000)}")
        
 
    def test_db_journal_error(self):
        db_journal_error(f"Error message: {random.randrange(1000)}")
        
        
    def test_db_journal_warning(self):
        db_journal_warning(f"Warning message: {random.randrange(1000)}")
        
        
    def test_db_journal_info(self):
        db_journal_info(f"Info message: {random.randrange(1000)}")
        
        
    def test_db_journal_debug(self):
        db_journal_debug(f"Debug message: {random.randrange(1000)}")
        
        
    def test_db_journal_trace(self):
        db_journal_trace(f"Trace message: {random.randrange(1000)}")


if __name__ == "__main__":
    unittest.main()