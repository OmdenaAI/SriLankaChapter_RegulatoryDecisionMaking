import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import asyncio
import unittest
import pandas as pd
import os
import tempfile
from unittest.mock import patch
from main import main  # Import the main function to test

class TestMainFunction(unittest.TestCase):

    def setUp(self):
        # Create temporary directories for input and output
        self.temp_dir = tempfile.TemporaryDirectory()
        self.input_path = os.path.join(self.temp_dir.name, 'input.csv')
        self.expected_path = os.path.join(self.temp_dir.name, 'expected_output.csv')
        self.output_path = os.path.join(self.temp_dir.name, 'output.csv')

    def tearDown(self):
        # Clean up temporary directory
        self.temp_dir.cleanup()

   
    def test_empty_dataframe(self):
        """Test that the script handles an empty DataFrame."""
        empty_data = pd.DataFrame()
        empty_data.to_csv(self.input_path, index=False)
        self.assertIsNone(asyncio.run(main(self.input_path, self.output_path)))

   

    def test_invalid_dataframe(self):
        """Test that an invalid DataFrame raises an error."""
        with open(self.input_path, "w") as f:
            f.write("corrupted data")

        self.assertIsNone(asyncio.run(main(self.input_path, self.output_path)))

    def test_invalid_path(self):
        """Test that a non-existent file path raises an error."""
        self.assertIsNone(asyncio.run(main("non_existent_path.csv", self.output_path)))
        


if __name__ == "__main__":
    unittest.main()
