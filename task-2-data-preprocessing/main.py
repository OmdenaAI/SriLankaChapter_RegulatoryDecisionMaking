import os
import ast
from tqdm.auto import tqdm
import pandas as pd
import argparse
import asyncio

tqdm.pandas()

from groq import Groq

from dotenv import load_dotenv
from src.utils import (process_documents,
                       extract_and_merge_metadata,
                       clean_and_save_data,
                       read_csv)

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

async def main(df_path, save_path):
    try:
        df = read_csv(df_path)
        processed_df = process_documents(df)
        print(processed_df.columns)
        merged_df = extract_and_merge_metadata(processed_df, client)
        clean_and_save_data(df, merged_df, save_path)
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
    
if __name__ == '__main__':
    args = parser.parse_args()
    asyncio.run(main(args.df_path, args.save_path))
