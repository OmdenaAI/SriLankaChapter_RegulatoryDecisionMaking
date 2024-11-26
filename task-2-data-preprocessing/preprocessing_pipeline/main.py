import os
import ast
from tqdm.auto import tqdm
import pandas as pd
import argparse
import asyncio

tqdm.pandas()

from groq import Groq

from dotenv import load_dotenv
from cleaning_utils import (merge_markdown_content,
                   parse_json_from_string,
                   append_merged_df_to_all_data,
                   remove_no_content_rows,
                   normalize_markdown)

from metadata_extraction import extract_metadata

from parser_functions import (process_and_save_df , 
                              documents_to_dataframe,
                              save_dataframe_to_csv)

# Set up logging
import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# loading the environnement variables
load_dotenv()
groq_api = os.getenv("GROQ_API_KEY")
client = Groq(api_key=groq_api)

parser = argparse.ArgumentParser(description="Pipeline to preprocess a csv file for RAG")
parser.add_argument("df_path",help='Path where the dataframe to process is located')
parser.add_argument('save_path',help = 'Path where to save the processed dataframe')

async def main(df_path , save_path):
    # read the csv
    logging.info('Reading the csv')
    df = pd.read_csv(df_path)
    df.fillna('', inplace=True)
    logging.info('Reading complete')

    # process the document using llama parse
    logging.info('Beginning of the parsing')
    processed_documents = await process_and_save_df(df,reformat_path = True)
    processed_df = documents_to_dataframe(processed_documents)
    logging.info('Parsing complete')


    #metadata extraction
    logging.info('Beginning of the metadata extraction')
    df_metadata = pd.DataFrame()

    # Extracting the 'filename' from the 'metadata' column
    df_metadata['file_name'] = processed_df['metadata'].apply(lambda x: x.get('file_name', None))
    # Extracting the 'content' 
    df_metadata['markdown_content'] = processed_df['text']
    print(df_metadata.columns)

    merged_df = df_metadata.groupby('file_name', as_index=False).apply(merge_markdown_content)
    merged_df = merged_df.reset_index(drop=True)

    # Apply the metadata extraction to the DataFrame
    merged_df['metadata_returned'] = merged_df.progress_apply(lambda row :extract_metadata(row , client),
                                                      axis = 1)
    merged_df['parsed_metadata'] = merged_df['metadata_returned'].apply(parse_json_from_string)
    merged_df = merged_df.join(merged_df['parsed_metadata'].apply(pd.Series))

    merged_df = merged_df.drop(columns=['metadata_returned','parsed_metadata'])

    # Cleaning the dataset
    merged_df = merged_df.rename(columns={
    'file_name':'filename',
    'markdown_content_long': 'markdown_content',
    'parsed_title': 'llama_title',
    'parsed_issue_date': 'llama_issue_date',
    'parsed_reference_number': 'llama_reference_number'
    })
    logging.info('Extraction complete')

    logging.info('Cleaning of the dataset')
    # combine the extracted data with the original dataset
    df = append_merged_df_to_all_data(df, merged_df)
    # remove the file with no content
    df =  remove_no_content_rows(df)
    df = df.drop(columns=['is_empty'])
    # Apply normalization to the 'markdown_content' column
    df['normalized_content'] = df['markdown_content'].apply(normalize_markdown)

    # Mark duplicates based on the normalized content, keeping the first instance and marking all others as duplicates
    df['is_duplicate'] = df.duplicated(subset='normalized_content', keep='first')

    #Remove duplicates
    df = df[df['is_duplicate'] == False].copy()
    df = df.drop(columns=['is_duplicate','normalized_content'])
    logging.info('Cleaning complete')

    logging.info('Saving the cleaned dataset')
    save_dataframe_to_csv(processed_df, save_path)
    logging.info('Saving complete')
    
if __name__ == '__main__':
    args = parser.parse_args()
    asyncio.run(main(args.df_path, args.save_path))
