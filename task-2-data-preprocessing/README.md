
# Data preprocessing

This repository provides a complete pipeline for processing and extracting metadata from a dataset of documents. The pipeline is designed for preprocessing text-based data, extracting metadata using an llama_parse and groq , and producing a clean and non deduplicated dataset for downstream tasks such as Retrieval-Augmented Generation (RAG) or data analytics.

## Features

- **Pdf parsing and text formatting** : Use llama_parse and llama_index to extract the text from pdf in a markdown format and 
- **Metadata Extraction**: Uses an groq API to extract metadata fields such as title, issue date, and reference number.
- **Cleaning** : Applies various techniques to clean the such as duplicate removal , removal of excessive white space and other 

- **Logging**: Provides detailed logs for monitoring and debugging during the pipeline's execution.

## Requirements

For this code to work the .env file present in this repository must be populated with a GROQ_API_KEY and a LLAMA_CLOUD_API_KEY
you can check their page here
llama_cloud : https://docs.llamaindex.ai/en/stable/ <br>
groq : https://groq.com/

Install the required dependencies using:
```python
pip install -r requirements.txt
```

## Usage 

The main script processes a dataset and outputs a cleaned, enriched dataset. To run the pipeline:

1. Prepare your environment:
- Add your *GROQ_API_KEY* and *LLAMA_CLOUD_API_KEY* to a .env file
```markdown
LLAMA_CLOUD_API_KEY = your_api_key
GROQ_API_KEY = your_api_key
```

2. Run the script
```bash
cd preprocessing_pipeline
python main.py <input_csv_path> <output_csv_path>
```


### arguments
- **input_csv_path** : path to a csv containing at least the paths to the file to process and the nature of the document (txt or pdf) <br>
- **output_csv_path** : path where to save the result ,a processed csv with the extracted content and metadatasa path

