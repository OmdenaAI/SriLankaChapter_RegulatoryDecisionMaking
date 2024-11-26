import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import unittest
import pandas as pd
from src.cleaning_utils import (
    merge_markdown_content,
    parse_json_from_string,
    append_merged_df_to_all_data,
    remove_no_content_rows,
    normalize_markdown
)

class TestHelperFunctions(unittest.TestCase):

    def test_merge_markdown_content(self):
        df = pd.DataFrame({
            'file_name': ['file1', 'file1', 'file1'],
            'markdown_content': ['Content 1', 'Content 2', 'Content 3']
        })
        result = df.groupby('file_name').apply(merge_markdown_content)
        self.assertEqual(result.iloc[0]['markdown_content_long'], "Content 1\nContent 2\nContent 3")
        self.assertEqual(result.iloc[0]['markdown_content_short'], "Content 1 Content 2 Content 3")

        # Edge case: Single row
        df_single = pd.DataFrame({'file_name': ['file2'], 'markdown_content': ['Only Content']})
        result_single = df_single.groupby('file_name').apply(merge_markdown_content)
        self.assertEqual(result_single.iloc[0]['markdown_content_long'], "Only Content")

    def test_parse_json_from_string(self):
        self.assertEqual(parse_json_from_string("{'key': 'value'}"), {'key': 'value'})
        self.assertIsNone(parse_json_from_string("Invalid JSON"))
        self.assertIsNone(parse_json_from_string(""))
        self.assertIsNone(parse_json_from_string(None))

    def test_append_merged_df_to_all_data(self):
        all_data = pd.DataFrame({'filename': ['file1', 'file2']})
        merged_df = pd.DataFrame({
            'filename': ['file1', 'file3'],
            'markdown_content': ['Content 1', 'Content 3'],
            'llama_title': ['Title 1', 'Title 3'],
            'llama_issue_date':['24-12-2021','22-15-2025'],
            'llama_reference_number':['0855','7896']
        })
        result = append_merged_df_to_all_data(all_data, merged_df)
        self.assertIn('llama_title', result.columns)
        self.assertEqual(result.iloc[0]['llama_title'], 'Title 1')
        self.assertTrue(pd.isna(result.iloc[1]['llama_title']))

    def test_remove_no_content_rows(self):
        df = pd.DataFrame({
            'filename': ['file1', 'file2'],
            'markdown_content': ['NO_CONTENT_HERE', 'Some Content']
        })
        result = remove_no_content_rows(df)
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]['markdown_content'], 'Some Content')

    def test_normalize_markdown(self):
        self.assertEqual(normalize_markdown("  This IS  a Test  "), "this is a test")
        self.assertEqual(normalize_markdown(""), "")
        self.assertEqual(normalize_markdown("already normalized"), "already normalized")

if __name__ == '__main__':
    unittest.main()
