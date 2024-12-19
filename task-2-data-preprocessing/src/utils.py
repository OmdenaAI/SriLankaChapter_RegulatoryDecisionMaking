import os
import ast
from tqdm.auto import tqdm
import pandas as pd
import asyncio

from src.cleaning_utils import (merge_markdown_content,
                   parse_json_from_string,
                   append_merged_df_to_all_data,
                   remove_no_content_rows,
                   normalize_markdown,
                   process_row)

from src.metadata_extraction import extract_metadata , clean_date

from src.parser_functions import (process_and_save_df , 
                              documents_to_dataframe,
                              save_dataframe_to_csv)

import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def log_errors(func):
    """Decorator to log errors for a function."""
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logging.error(f"Error in {func.__name__}: {e}")
            raise
    return wrapper


@log_errors
def read_csv(df_path):
    logging.info("Reading the CSV")
    df = pd.read_csv(df_path)
    df.fillna("", inplace=True)
    logging.info("Reading complete")
    return df


@log_errors
def process_documents(df):
    logging.info("Beginning of the parsing")
    processed_documents = asyncio.run(process_and_save_df(df,reformat_path=False))
    processed_df = documents_to_dataframe(processed_documents)
    logging.info("Parsing complete")
    return processed_df


@log_errors
def extract_and_merge_metadata(processed_df, client):
    logging.info("Beginning of metadata extraction")
    df_metadata = pd.DataFrame()

    # Extracting the 'filename' from the 'metadata' column
    df_metadata['file_name'] = processed_df['metadata'].apply(lambda x: x.get('file_name', None))
    # Extracting the 'content' 
    df_metadata['markdown_content'] = processed_df['text']

    merged_df = df_metadata.groupby('file_name', as_index=False).apply(merge_markdown_content)
    merged_df = merged_df.reset_index(drop=True)

    # Apply the metadata extraction to the DataFrame
    merged_df['metadata_returned'] = merged_df.progress_apply(lambda row :extract_metadata(row , client),
                                                        axis = 1)
    merged_df['parsed_metadata'] = merged_df['metadata_returned'].apply(parse_json_from_string)
    merged_df = merged_df.join(merged_df['parsed_metadata'].apply(pd.Series))

    merged_df = merged_df.drop(columns=['metadata_returned','parsed_metadata'])
    merged_df['parsed_issue_date'] = merged_df['parsed_issue_date'].apply(clean_date)

    # Cleaning the dataset
    merged_df = merged_df.rename(columns={
        'file_name':'filename',
        'markdown_content_long': 'markdown_content',
        'parsed_title': 'llama_title',
        'parsed_issue_date': 'llama_issue_date',
        'parsed_reference_number': 'llama_reference_number'
        })
    logging.info("Metadata extraction complete")
    return merged_df


@log_errors
def clean_and_save_data(df, merged_df, save_path):
    logging.info("Cleaning the dataset")
    df = append_merged_df_to_all_data(df, merged_df)
    df = remove_no_content_rows(df).drop(columns=["is_empty"])
    df["normalized_content"] = df["markdown_content"].apply(normalize_markdown)
    df['markdown_path'] = df.apply( process_row, axis=1)
    df["is_duplicate"] = df.duplicated(subset="normalized_content", keep="first")
    df = df[~df["is_duplicate"]].copy()
    df = df.drop(columns=["is_duplicate", "normalized_content"])
    logging.info("Dataset cleaning complete")

    logging.info("Saving the cleaned dataset")
    save_dataframe_to_csv(df, save_path)
    logging.info("Dataset saved successfully")