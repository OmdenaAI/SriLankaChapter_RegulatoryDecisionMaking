import os
import logging
import pandas as pd
from typing import List
from llama_index.core import Document, StorageContext
from llama_index.vector_stores.kdbai import KDBAIVectorStore
from llama_index.core.indices import VectorStoreIndex
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
import kdbai_client as kdbai

from google.colab import userdata

# Configure logging
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_data(csv_path: str, text_col: str, metadata_cols: List[str],  doc_metadata_keys: List[str], only_tri_circulars: bool = False) -> List[Document]:
    """
    Load documents from a CSV file and convert them to Document objects.
    
    Args:
        csv_path (str): Path to the CSV file.
        text_col (str): Column containing text data.
        metadata_cols (List[str]): Columns to include as metadata.

    Returns:
        List[Document]: List of Document objects.
    """
    try:
        df = pd.read_csv(csv_path)
        documents = [
            Document(
                text=str(row[text_col]),
                metadata={doc_metadata_keys[i]: row[col] for i, col in enumerate(metadata_cols)}
            )
            for _, row in df.iterrows()
        ]

        # Filtering only TRI Circulars
        if only_tri_circulars:
          documents = [doc for doc in documents if ((doc.metadata['class'] == 'circular') and (doc.metadata['issuing_authority'] == ('Tea Research Institute')))]

        # Converting issuing dates metadata into datetime format
        documents = convert_to_datetime64(documents)
        
        # logging.info(f"Loaded {len(documents)} documents from {csv_path}")
        print(f"Loaded {len(documents)} documents from {csv_path}")
        return documents
    except Exception as e:
        # logging.error(f"Error loading data: {e}")
        print(f"Error loading data: {e}")
        raise

def convert_to_datetime64(docs: List[Document]) -> List[Document]:
  """
  Convert the 'issue_date' column in the provided documents to a datetime64 type.
  """

  for idx, doc in enumerate(docs):
    doc_date = doc.metadata['issue_date']
    if not str(doc_date) == "nan":
      # Pick first date if multiple available
      doc_date = " ".join(doc_date.split()[0:2])
    try:
      doc.metadata['issue_date_ts'] = pd.to_datetime(doc_date, format="%B %Y")
    except:
      raise ValueError(f"Invalid date format: {doc_date} for iter-index: {idx}")

  return docs


def setup_kdbai_session() -> kdbai.Session:
    """
    Set up a session for KDBAI.

    Returns:
        kdbai.Session: Configured KDBAI session.
    """
    try:
        # kdbai_endpoint = os.getenv("KDBAI_SESSION_ENDPOINT")
        # kdbai_api_key = os.getenv("KDBAI_API_KEY")

        kdbai_endpoint = userdata.get('KDBAI_SESSION_ENDPOINT')
        kdbai_api_key = userdata.get('KDBAI_API_KEY')

        if not kdbai_endpoint:
          raise ValueError("Please set KDBAI_SESSION_ENDPOINT environment variable.")

        if not kdbai_api_key:
          raise ValueError("Please set KDBAI_API_KEY environment variable.")

        session = kdbai.Session(
            endpoint=f"https://cloud.kdb.ai/instance/{kdbai_endpoint}",
            api_key=kdbai_api_key
        )
        # logging.info("KDBAI session established.")
        print("KDBAI session established.")
        return session
    except Exception as e:
        # logging.error(f"Error setting up KDBAI session: {e}")
        print(f"Error setting up KDBAI session: {e}")
        raise

def setup_vector_store(documents: List[Document], session: kdbai.Session, db_name: str, table_name: str, embedding_model_name: str) -> None:
    """
    Set up the vector store using KDBAI.

    Args:
        documents (List[Document]): Documents to index.
        session (kdbai.Session): KDBAI session object.
        db_name (str): Database name.
        table_name (str): Table name.
        embedding_model_name (str): Name of the embedding model.
    """
    try:
        # Drop and recreate database
        try:
            session.database(db_name).drop()
        except kdbai.KDBAIException:
            pass
        db = session.create_database(db_name)

        # Check if table exists and drop if necessary
        try:
            if table_name in [table.name for table in db.list_tables()]:
                db.table(table_name).drop()
        except kdbai.KDBAIException:
            pass

        table_schema = [
            dict(name="document_id", type="bytes"),
            dict(name="text", type="bytes"),
            dict(name="embeddings", type="float32s"),
            dict(name="issue_date_ts", type="datetime64[ns]")
        ]

        index_flat = {
            "name": "flat_index",
            "type": "flat",
            "column": "embeddings",
            "params": {"dims": 384, "metric": "CS"}  # CS: Cosine Similarity metric
        }

        table = db.create_table(table_name, schema=table_schema, indexes=[index_flat])

        # Initialize embeddings
        embedding_model = HuggingFaceEmbedding(model_name=embedding_model_name)

        # Set up vector store
        vector_store = KDBAIVectorStore(
            table=table,
            index_name=f"{table_name}_index",
            embeddings=embedding_model
        )

        storage_context = StorageContext.from_defaults(vector_store=vector_store)
        index = VectorStoreIndex.from_documents(
            documents,
            storage_context=storage_context
        )
        # logging.info(f"Vector store setup complete. Indexed {len(documents)} documents.")
        print(f"Vector store setup complete. Indexed {len(documents)} documents.")

    except Exception as e:
        # logging.error(f"Error setting up vector store: {e}")
        print(f"Error setting up vector store: {e}")
        raise

if __name__ == "__main__":
    # Define paths and parameters
    CSV_PATH = "/content/drive/MyDrive/Omdena/Regulatory RAG (SL Chapter)/code/model dev/data/2024_11_28 v0_LK_tea_dataset.csv"
    TEXT_COL = "markdown_content" 
    METADATA_COLS = ['id', 'class', 'issuing_authority', 'llama_title', 'llama_issue_date', 'llama_reference_number']
    DOC_METADATA_KEYS = ['document_id', 'class', 'issuing_authority', 'title', 'issue_date', 'reference_number']
    
    DB_NAME = "srilanka_tri_circulars"
    TABLE_NAME = "rag_baseline"
    
    EMBEDDING_MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"

    try:
        # Load documents
        docs = load_data(CSV_PATH, TEXT_COL, METADATA_COLS, DOC_METADATA_KEYS, only_tri_circulars=True)

        # # Set up KDBAI session
        kdbai_session = setup_kdbai_session()

        # # Set up vector store
        setup_vector_store(docs, kdbai_session, DB_NAME, TABLE_NAME, EMBEDDING_MODEL_NAME)

        # logging.info(f"RAG setup process completed successfully. Processed {len(docs)} documents and stored in database '{DB_NAME}' with table '{TABLE_NAME}'.")
        print(f"RAG setup process completed successfully. Processed {len(docs)} documents and stored in database '{DB_NAME}' with table '{TABLE_NAME}'.")
    except Exception as e:
        # logging.error(f"Failed to complete RAG setup: {e}")
        print(f"Failed to complete RAG setup: {e}")