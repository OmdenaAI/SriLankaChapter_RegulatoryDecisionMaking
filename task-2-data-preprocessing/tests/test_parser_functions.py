import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import unittest
import pandas as pd
from io import StringIO
from llama_index.core.schema import Document
from src.parser_functions import (
    document_to_dict,
    documents_to_dicts,
    documents_to_dataframe,
    save_dataframe_to_csv
)

class TestHelperFunctions(unittest.TestCase):
    def test_document_to_dict(self):
        mock_document = Document(id_='123', text='Example text')
        expected_dict = {
            "id": '123',
            "embedding": None,
            "metadata": {},
            "excluded_embed_metadata_keys": [],
            "excluded_llm_metadata_keys": [],
            "relationships": {},
            "text": 'Example text',
            "mimetype": 'text/plain',
            "start_char_idx": None,
            "end_char_idx": None,
            "text_template": '{metadata_str}\n\n{content}',
            "metadata_template": '{key}: {value}',
            "metadata_seperator": '\n'
        }
        self.assertEqual(document_to_dict(mock_document), expected_dict)

    def test_documents_to_dicts(self):
        mock_documents = [
            Document(id_='123', text='Text1'),
            Document(id_='456', text='Text2')
        ]
        expected_list = [{'id': '123', 'embedding': None, 'metadata': {}, 'excluded_embed_metadata_keys': [], 'excluded_llm_metadata_keys': [], 'relationships': {}, 'text': 'Text1', 'mimetype': 'text/plain', 'start_char_idx': None, 'end_char_idx': None, 'text_template': '{metadata_str}\n\n{content}', 'metadata_template': '{key}: {value}', 'metadata_seperator': '\n'},
         {'id': '456', 'embedding': None, 'metadata': {}, 'excluded_embed_metadata_keys': [], 'excluded_llm_metadata_keys': [], 'relationships': {}, 'text': 'Text2', 'mimetype': 'text/plain', 'start_char_idx': None, 'end_char_idx': None, 'text_template': '{metadata_str}\n\n{content}', 'metadata_template': '{key}: {value}', 'metadata_seperator': '\n'}]

        
        self.assertEqual(documents_to_dicts(mock_documents), expected_list)
        self.assertEqual(documents_to_dicts([]), [])

    def test_documents_to_dataframe(self):
        mock_docs = [
            Document(id_='123', text='Text1'),
            Document(id_='456', text='Text2')
        ]
        df = documents_to_dataframe(mock_docs)
        self.assertEqual(len(df), 2)
        self.assertEqual(df.iloc[0]['id'], '123')

    def test_save_dataframe_to_csv(self):
        df = pd.DataFrame({"id": [1, 2], "text": ["a", "b"]})
        with StringIO() as buffer:
            save_dataframe_to_csv(df, buffer)
            buffer.seek(0)
            csv_content = buffer.read()
            self.assertIn("id,text", csv_content)
            self.assertIn("1,a", csv_content)

if __name__ == '__main__':
    unittest.main()
