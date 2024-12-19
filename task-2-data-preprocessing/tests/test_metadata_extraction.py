import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import unittest
import ast
from unittest.mock import MagicMock

from src.metadata_extraction import clean_text_for_metadata_extraction, extract_metadata


class TestMetadataExtraction(unittest.TestCase):

    def test_clean_text_for_metadata_extraction(self):
        # Test removing copyright information
        text_with_copyright = "This is some text. Copyright 2024 OpenAI. More text."
        cleaned_text = clean_text_for_metadata_extraction(text_with_copyright)
        self.assertEqual(cleaned_text, "This is some text. ")

        # Test non-string input
        self.assertIsNone(clean_text_for_metadata_extraction(None))
        self.assertIsNone(clean_text_for_metadata_extraction(123))

        # Test empty string
        self.assertEqual(clean_text_for_metadata_extraction(""), "")

    
    def test_extract_metadata(self):
        # Mocking the client
        mock_client = MagicMock()

        # Mocking a successful API response
        mock_response = MagicMock()
        mock_response.choices = [
            MagicMock(message=MagicMock(content='{"parsed_title": "Document Title", "parsed_issue_date": "2024-11-24", "parsed_reference_number": "12345"}')),
        ]
        mock_client.chat.completions.create.return_value = mock_response

        row = {"markdown_content_short": "Document Title\n\nDate: 2024-11-24\nReference No: 12345"}
        
        # Call the function with the mock client
        metadata = extract_metadata(row, mock_client)
        metadata = ast.literal_eval(metadata)
        

        # Verify correct prompt and API response
        self.assertIn("parsed_title", metadata)
        self.assertIn("parsed_issue_date", metadata)
        self.assertIn("parsed_reference_number", metadata)
        self.assertEqual(metadata["parsed_title"], "Document Title")
        self.assertEqual(metadata["parsed_issue_date"], "2024-11-24")
        self.assertEqual(metadata["parsed_reference_number"], "12345")

        # Test missing text content (None or empty string)
        row_missing_content = {"markdown_content_short": None}
        metadata_missing = extract_metadata(row_missing_content, mock_client)
        self.assertIsNone(metadata_missing)

        # Test empty string content
        row_empty_content = {"markdown_content_short": ""}
        metadata_empty = extract_metadata(row_empty_content, mock_client)
        self.assertIsNone(metadata_empty)



if __name__ == "__main__":
    unittest.main()
