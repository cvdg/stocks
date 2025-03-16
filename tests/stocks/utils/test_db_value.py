import unittest
import os
import random

import psycopg

from stocks.utils.db_value import db_value_setup, db_values_delete, db_values_exist, db_values_get, db_values_set

class TestDbValue(unittest.TestCase):
    def tearDown(self):
        uri = os.getenv("STOCKS_DATABASE_URI")
        with psycopg.connect(uri) as conn:
            with conn.cursor() as curr:
                curr.execute("DELETE FROM values WHERE key LIKE 'test_%'")
            conn.commit()

        return super().tearDown()
    
    def test_db_value_setup(self):
        uri = os.getenv("STOCKS_DATABASE_URI")
        db_value_setup(uri)

    def test_db_value_01(self):
        uri = os.getenv("STOCKS_DATABASE_URI")
        key = f"test_db_value_01_{random.randrange(1000)}"
        value = f"Something random: {random.randrange(1000)}"
        
        db_value_setup(uri)

        self.assertFalse(db_values_exist(key))


    def test_db_value_02(self):
        uri = os.getenv("STOCKS_DATABASE_URI")
        key = f"test_db_value_02_{random.randrange(1000)}"
        value = f"Something random: {random.randrange(1000)}"
        
        db_value_setup(uri)

        self.assertFalse(db_values_exist(key))
        db_values_set(key, value)
        self.assertTrue(db_values_exist(key))
        self.assertEqual(value, db_values_get(key))
        

    def test_db_value_03(self):
        uri = os.getenv("STOCKS_DATABASE_URI")
        key = f"test_db_value_03_{random.randrange(1000)}"
        value = f"Something random: {random.randrange(1000)}"
        
        db_value_setup(uri)

        db_values_set(key, value)
        self.assertTrue(db_values_exist(key))
        db_values_delete(key)
        self.assertFalse(db_values_exist(key))
 
        

if __name__ == "__main__":
    unittest.main()