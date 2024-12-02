import os
from dotenv import load_dotenv
import nest_asyncio
nest_asyncio.apply()

import json
import csv
import re
import pandas as pd
import numpy as np
import asyncio

from llama_parse import LlamaParse
from llama_index.core import SimpleDirectoryReader
from llama_index.core.schema import Document


load_dotenv()


# Initialize the parsers
pdf_parser = LlamaParse(result_type='markdown', num_workers=8)
text_parser = LlamaParse(result_type='markdown', Set_fast_mode=True, num_workers=8)

def reformat_path_(path):
    path = re.sub('/content/drive/MyDrive/Omdena_Challenge/' ,'preprocessing_pipeline/',path)
    return path




async def process_and_save_df(all_data , reformat_path = False):
    """Async function to process the DataFrame and save to CSV"""

    pdf_paths = all_data[all_data['PDF_or_text'] == 'PDF']['path'].tolist()
    if reformat_path:
        pdf_paths = [reformat_path_(path) for path in pdf_paths]

    if pdf_paths :
        pdf_file_extractor = {".pdf": pdf_parser}
        pdf_reader = SimpleDirectoryReader(input_files=pdf_paths, file_extractor=pdf_file_extractor)
        pdf_documents = await pdf_reader.aload_data()
    else:
        pdf_documents = []
    
    text_paths = all_data[all_data['PDF_or_text'] == 'Text']['text_path'].tolist()
    if reformat_path:
        text_paths = [reformat_path_(path) for path in text_paths]
    if text_paths :
        text_file_extractor = {".txt": text_parser}
        text_reader = SimpleDirectoryReader(input_files=text_paths, file_extractor=text_file_extractor)
        text_documents = await text_reader.aload_data()
    else :
        text_documents = []


    # Combine the results
    processed_documents = pdf_documents + text_documents

    if processed_documents:
        return processed_documents
    else:
        print("No documents found for processing.")
        return []
    


def document_to_dict(document: Document):
    """Helper function to convert a single Document to a dictionary"""
    doc_dict = {
        "id": document.id_,
        "embedding": document.embedding,  # May be None
        "metadata": document.metadata,  # Empty in your case
        "excluded_embed_metadata_keys": document.excluded_embed_metadata_keys,
        "excluded_llm_metadata_keys": document.excluded_llm_metadata_keys,
        "relationships": document.relationships,  # Empty in your case
        "text": document.text,  # The markdown content
        "mimetype": document.mimetype,
        "start_char_idx": document.start_char_idx,
        "end_char_idx": document.end_char_idx,
        "text_template": document.text_template,
        "metadata_template": document.metadata_template,
        "metadata_seperator": document.metadata_seperator
    }
    return doc_dict



def documents_to_dicts(documents: list):
    """Helper function to convert the list of Document objects
     to a list of dictionaries"""
    return [document_to_dict(doc) for doc in documents]


def documents_to_dataframe(documents: list):
    """Helper function to convert the list of dictionaries
     to a pandas DataFrame"""
    # Convert documents list to dicts
    doc_dicts = documents_to_dicts(documents)

    # Convert list of dictionaries to a DataFrame
    df = pd.DataFrame(doc_dicts)
    return df

def save_dataframe_to_csv(df, filename):
    df.to_csv(filename,
              index=False,  # Do not write row indices
              quoting=csv.QUOTE_MINIMAL,  # Quote fields that contain special characters
              quotechar='"',  # Use double quotes to wrap fields
              escapechar='\\',  # Escape any quotes inside markdown content
              lineterminator='\n',  # Make sure to preserve newlines in markdown content
              encoding='utf-8') 