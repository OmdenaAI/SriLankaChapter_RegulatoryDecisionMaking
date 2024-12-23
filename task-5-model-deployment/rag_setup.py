import logging
import pandas as pd
from typing import List
from llama_index.core import Document, StorageContext, Settings
from llama_index.core.node_parser import MarkdownNodeParser
from llama_index.vector_stores.kdbai import KDBAIVectorStore
from llama_index.core.indices import VectorStoreIndex
from llama_index.core.tools import QueryEngineTool
from llama_index.core.query_engine import RouterQueryEngine
from llama_index.core.selectors import LLMMultiSelector, PydanticMultiSelector
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
from llama_index.llms.groq import Groq
import kdbai_client as kdbai

from dotenv import load_dotenv
import os

#from google.colab import userdata

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s : %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    force=True
    )

def load_data(
    csv_path: str, 
    text_col: str, 
    metadata_cols: List[str],  
    doc_metadata_keys: List[str]
    ) -> List[Document]:
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
        df.fillna("nan", inplace=True)
        all_documents = [
            Document(
                text=str(row[text_col]),
                metadata={doc_metadata_keys[i]: row[col] for i, col in enumerate(metadata_cols)}
            )
            for _, row in df.iterrows()
        ]

        circulars_docs = [doc for doc in all_documents if doc.metadata['class'] == 'circular']
        logging.info(f"Loaded {len(circulars_docs)} Circular documents")

        guideline_docs = [doc for doc in all_documents if doc.metadata['class'] == 'guideline']
        logging.info(f"Loaded {len(guideline_docs)} Guidline documents")

        regulatory_docs = [doc for doc in all_documents if doc.metadata['class'] == 'regulatory']
        logging.info(f"Loaded {len(regulatory_docs)} Regulatory documents")

        all_docs_dict = {
            'circular': circulars_docs,
            'guideline': guideline_docs,
            'regulatory': regulatory_docs
        }
        
        all_documents_count = sum([len(all_docs_dict[class_name]) for class_name in all_docs_dict])

        logging.info(f"Loaded {all_documents_count} documents from {csv_path}")
        return all_docs_dict
    except Exception as e:
        logging.error(f"Error loading data: {e}")
        raise


def setup_kdbai_session() -> kdbai.Session:
    """
    Set up a session for KDBAI.

    Returns:
        kdbai.Session: Configured KDBAI session.
    """
    try:

        kdbai_endpoint = os.getenv('KDBAI_SESSION_ENDPOINT')
        kdbai_api_key = os.getenv('KDBAI_API_KEY')

        if not kdbai_endpoint:
          raise ValueError("Please set KDBAI_SESSION_ENDPOINT environment variable.")

        if not kdbai_api_key:
          raise ValueError("Please set KDBAI_API_KEY environment variable.")

        session = kdbai.Session(
            endpoint=kdbai_endpoint,
            api_key=kdbai_api_key
        )
        logging.info("KDBAI session established.")
        return session
    except Exception as e:
        logging.error(f"Error setting up KDBAI session: {e}")
        raise

def setup_groq_llm(
    groq_model_name="llama-3.1-8b-instant",
    temp=0.0
    ):
    """
    Setup Groq LLM API
    """
    groq_api_key = os.getenv('GROQ_API_KEY')
    if not groq_api_key:
        raise ValueError("Please set GROQ_API_KEY environment variable")

    return Groq(
        api_key=groq_api_key,
        model=groq_model_name,
        temperature=temp
    )

def setup_vector_store(
    documents_dict: dict, 
    session: kdbai.Session, 
    db_name: str, 
    circular_table_name: str, 
    regulation_table_name: str, 
    guideline_table_name: str, 
    embedding_model_name: str, 
    groq_llm_model_name: str
    ):
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
            db.table(circular_table_name).drop()
            db.table(regulation_table_name).drop()
            db.table(guideline_table_name).drop()
        except kdbai.KDBAIException:
            pass

        # Same table schema and table index for each class
        table_schema = [
                dict(name="document_id", type="bytes"),
                dict(name="text", type="bytes"),
                dict(name="embeddings", type="float32s"),
                dict(name="issue_date", type="str"),
                dict(name="issuing_authority", type="str"),
                dict(name="class", type="str"),
                dict(name="reference_number", type="str"),
                dict(name="title", type="str"),
                dict(name="original_doc_id", type="int16"),
            ]

        index_flat = {
            "name": "flat_index",
            "type": "flat",
            "column": "embeddings",
            "params": {"dims": 384, "metric": "CS"}  # CS: Cosine Similarity metric
        }

        # Create tables for all 3 classes of documents
        circular_table = db.create_table(circular_table_name, table_schema, indexes=[index_flat])
        regulation_table = db.create_table(regulation_table_name, table_schema, indexes=[index_flat])
        guideline_table = db.create_table(guideline_table_name, table_schema, indexes=[index_flat])

        # Initialize embeddings
        logging.info(f"Setting up embeddings model endppoint for {embedding_model_name}")
        embedding_model = HuggingFaceEmbedding(
            model_name=embedding_model_name,
            cache_folder=os.getcwd()+'/models'
            )

        # Setup Groq API
        logging.info(f"Setting up Groq LLM endppoint for {groq_llm_model_name}")
        llm = setup_groq_llm(groq_llm_model_name)

        Settings.llm = llm
        Settings.embed_model = embedding_model

        # Create Vector Store for all 3 classes
        logging.info(f"Creating vector stores for Guidelines, Regulations and Circulars.")
        guideline_vector_store = KDBAIVectorStore(table=guideline_table, index_name=f"{guideline_table_name}_index")
        regulation_vector_store = KDBAIVectorStore(table=regulation_table, index_name=f"{regulation_table_name}_index")
        circular_vector_store = KDBAIVectorStore(table=circular_table, index_name=f"{circular_table_name}_index")

        # Create storage context for each vector stores
        logging.info(f"Creating storage context for Guidelines, Regulations and Circulars.")
        guideline_storage_context = StorageContext.from_defaults(vector_store=guideline_vector_store)
        regulation_storage_context = StorageContext.from_defaults(vector_store=regulation_vector_store)
        circular_storage_context = StorageContext.from_defaults(vector_store=circular_vector_store)

        # Get list of documents for each class
        logging.info(f"Getting list of documents for Guidelines, Regulations and Circulars.")
        guideline_docs = documents_dict['guideline']
        circular_docs = documents_dict['circular']
        regulation_docs = documents_dict['regulatory']

        # Create Guideline Vector Store Index
        logging.info(f"Creating vector store index for Guidelines. Indexing {len(guideline_docs)} Guideline documents.")
        guideline_index = VectorStoreIndex.from_documents(
            guideline_docs,
            storage_context=guideline_storage_context,
            transformations=[MarkdownNodeParser()]
        )
        logging.info(f"Successfully created vector store index for guidelines.")

        # Create Regulations Vector Store Index
        logging.info(f"Creating vector store index for Regulations. Indexing {len(regulation_docs)} Circular documents.")
        regulation_index = VectorStoreIndex.from_documents(
            regulation_docs,
            storage_context=regulation_storage_context,
            transformations=[MarkdownNodeParser()]
        )
        logging.info(f"Successfully created vector store index for Circulars.")

        # Create Circular Vector Store Index
        logging.info(f"Creating vector store index for Circulars. Indexing {len(circular_docs)} Regulatory documents.")
        circular_index = VectorStoreIndex.from_documents(
            circular_docs,
            storage_context=circular_storage_context,
            transformations=[MarkdownNodeParser()]
        )
        logging.info(f"Successfully created vector store index for Regulations.")

        # Consolidate each Vector Store Indexes
        indexes = {
            'guideline': guideline_index,
            'regulation': regulation_index,
            'circular': circular_index
        }
        
        all_documents_count = sum([len(guideline_docs) + len(circular_docs) + len(regulation_docs)])
        logging.info(f"Vector store setup complete. Indexed {all_documents_count} documents.")
        return llm, indexes

    except Exception as e:
        logging.error(f"Error setting up vector store: {e}")
        raise

def setup_router_query_engine(indexs: dict, llm: Groq):
    """
    Set up the router query engine using KDBAI Vector Indexes and Groq LLM.
    """
    try:
      # Set topk value
      K = 20

      # Get Vector Store Indexes for each class
      guideline_index = indexs['guideline']
      regulation_index = indexs['regulation']
      circular_index = indexs['circular']

      # Create Query Engine for Guidelines
      guideline_query_engine = guideline_index.as_query_engine(
          similarity_top_k=K,
          llm=llm,
          vector_store_kwargs={"index": "flat_index"}
          )

      # Create Query Engine for Regulations
      regulation_query_engine = regulation_index.as_query_engine(
          similarity_top_k=K,
          llm=llm,
          vector_store_kwargs={"index": "flat_index"}
          )

      # Create Query Engine for Circulars
      circular_query_engine = circular_index.as_query_engine(
          similarity_top_k=K,
          llm=llm,
          vector_store_kwargs={"index": "flat_index"}
          )
      
      # Create Query Engine Tool for Guidelines
      guideline_tool = QueryEngineTool.from_defaults(
          query_engine=guideline_query_engine,
          name  = "guidelines_tool",
          description=(
              "Provide information about Sri Lanka Guidelines documents."
          ),
      )

      # Create Query Engine Tool for Regulations
      regulation_tool = QueryEngineTool.from_defaults(
          query_engine=regulation_query_engine,
          name = "regulations_tool",
          description=(
              "Provide information about Sri Lanka Regulations documents and Acts."
          ),
      )

      # Create Query Engine Tool for Circulars
      circular_tool = QueryEngineTool.from_defaults(
          query_engine=circular_query_engine,
          name = "circulars_tool",
          description=(
              "Provide information about Sri Lanka Circular documents."
          ),
      )

      # Create Router Query Engine    
      query_engine = RouterQueryEngine(
          selector=PydanticMultiSelector.from_defaults(),
          query_engine_tools=[
              guideline_tool,
              regulation_tool,
              circular_tool
          ],
      )

      logging.info(f"Router Query Engine setup complete.")
      return query_engine

    except Exception as e:
      logging.error(f"Error setting up query engine: {e}")
      raise

def interactive_chat(query_engine: VectorStoreIndex, llm: Groq):
    """
    Provide a user interface for interacting with the RAG system.

    Args:
        query_engine (VectorStoreIndex): The query engine to use for responses.
        llm (Groq): The LLM to generate responses.
    """
    logging.info("Starting interactive chatbot. Type 'exit' to quit.")

    Settings.llm = llm
    chat_history = ""
    while True:
        user_input = input("You: ")
        chat_history += f"\n\nUser: {user_input}"
        if user_input.lower() in ["exit", "quit"]:
            logging.info("Exiting chatbot.")
            break

        try:
            retrieval_result = query_engine.query(chat_history+"\n\nBot: ")
            response = retrieval_result.response
            print(f"Bot: {response}")
            chat_history += f"\n\nBot: {response}"
        except Exception as e:
            logging.error(f"Error during query execution: {e}")
            print("Bot: Sorry, something went wrong. Please try again.")


if __name__ == "__main__":
    # Define File Path and Column Names
    CSV_PATH = os.getenv('DATA_FOLDER')
    TEXT_COL = "markdown_content"
    METADATA_COLS = ['id', 'class', 'issuing_authority', 'llama_title', 'llama_issue_date', 'llama_reference_number']
    DOC_METADATA_KEYS = ['original_doc_id', 'class', 'issuing_authority', 'title', 'issue_date', 'reference_number']

    # Define KDBAI Database and Table Names
    DB_NAME = "test_srilanka_rag_database"
    TABLE_NAME_CIRCULAR = "test_circular_baseline"
    TABLE_NAME_REGULATION = "test_regulation_baseline"
    TABLE_NAME_GUIDELINE = "test_guideline_baseline"

    # Define HuggingFace Model and Groq LLM Names
    EMBEDDING_MODEL_NAME = "BAAI/bge-small-en-v1.5"
    GROQ_LLM_NAME = "llama3-groq-70b-8192-tool-use-preview"

    try:
        # Load documents
        docs_dict = load_data(
            CSV_PATH, 
            TEXT_COL, 
            METADATA_COLS, 
            DOC_METADATA_KEYS
            )

        # Set up KDBAI session
        kdbai_session = setup_kdbai_session()

        # Set up vector store
        llm, indexes = setup_vector_store(
            docs_dict,
            kdbai_session,
            DB_NAME,
            TABLE_NAME_CIRCULAR,
            TABLE_NAME_REGULATION,
            TABLE_NAME_GUIDELINE,
            EMBEDDING_MODEL_NAME,
            GROQ_LLM_NAME
            )

        # Set up query engine
        router_query_engine = setup_router_query_engine(indexes, llm)

        # Begin querying chatbot
        try:
          interactive_chat(router_query_engine, llm)
        except Exception as e:
          logging.info(f"Error during querying chatbot: {e}")

        logging.info(f"RAG setup process completed successfully. Processed {sum([len(docs_dict[class_name]) for class_name in docs_dict])} documents and stored in database '{DB_NAME}' with tables '{[TABLE_NAME_CIRCULAR, TABLE_NAME_REGULATION, TABLE_NAME_GUIDELINE]}'.")
    except Exception as e:
        logging.info(f"Failed to complete RAG setup: {e}")