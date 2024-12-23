import streamlit as st
import os
import time
from llama_index.core import Document, StorageContext, Settings
from dotenv import load_dotenv
from rag_setup import setup_router_query_engine, setup_vector_store, setup_kdbai_session, setup_groq_llm
from llama_index.vector_stores.kdbai import KDBAIVectorStore
from llama_index.core.indices import VectorStoreIndex
from llama_index.core import Document, StorageContext, Settings
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
import json

load_dotenv()

st.set_page_config(
    page_title="Sri Lanka Regulatory Archives Q&A",
    page_icon="📜",
    layout="wide"
)
# Define KDBAI Database and Table Names
DB_NAME = "test_srilanka_rag_database"
TABLE_NAME_CIRCULAR = "test_circular_baseline"
TABLE_NAME_REGULATION = "test_regulation_baseline"
TABLE_NAME_GUIDELINE = "test_guideline_baseline"

# Define HuggingFace Model and Groq LLM Names
EMBEDDING_MODEL_NAME = "BAAI/bge-small-en-v1.5"
GROQ_LLM_NAME = "llama3-groq-70b-8192-tool-use-preview"

# Set up KDBAI session
kdbai_session = setup_kdbai_session()

db = kdbai_session.database(DB_NAME)

#set up vector store
circular_table = db.table(TABLE_NAME_CIRCULAR)
regulation_table = db.table(TABLE_NAME_REGULATION)
guideline_table = db.table(TABLE_NAME_GUIDELINE)

embedding_model = HuggingFaceEmbedding(
            model_name=EMBEDDING_MODEL_NAME,
            cache_folder=os.getcwd()+'/models'
            )

llm = setup_groq_llm(GROQ_LLM_NAME)

Settings.llm = llm
Settings.embed_model = embedding_model

circular_vector_store = KDBAIVectorStore(circular_table)
circular_index = VectorStoreIndex.from_vector_store(vector_store=circular_vector_store)

guideline_vector_store = KDBAIVectorStore(guideline_table)
guideline_index = VectorStoreIndex.from_vector_store(vector_store=guideline_vector_store)

regulation_vector_store = KDBAIVectorStore(regulation_table)
regulation_index = VectorStoreIndex.from_vector_store(vector_store=regulation_vector_store)
indexes = {
            'guideline': guideline_index,
            'regulation': regulation_index,
            'circular': circular_index
        }

# Set up query engine
query_engine = setup_router_query_engine(indexes, llm)

# streamed response emulator
def response_generator(response):
    for word in response.split():
        yield word + " "
        time.sleep(0.05)

# Sidebar Navigation
with st.sidebar:
    st.image("images/SriLankaChapterLogo.png")
    st.title("Sri Lanka Chapter Project")
    st.markdown("""
    **Navigate:**
    - **Home**: Main Q&A Application
    - **Collaborators**: Learn about our contributors
    - **Chapter Details**: About this chapter
    """)

page = st.sidebar.radio("Select Page", ["Home", "Collaborators", "Chapter Details"])

if page == "Home":
    # Home Page
    st.title("📜 Sri Lanka Tea Estate Digital Regulatory Archive Q&A System")
    st.markdown("""
    This tool digitizes physical archives and provides an AI-powered Q&A system to retrieve relevant documents for decision-making in industries like tea.
    """)

    # initialize chat history
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # display chat messages from history on app rerun
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # react to user input
    if prompt:=st.chat_input("Ask me something"):
        # display user input in chat message container
        st.chat_message("user").markdown(prompt)
        #add user message to chat history
        st.session_state.messages.append({"role":"user","content":prompt})

        with st.chat_message("assistant"):
            messages = [
                    {"role":m["role"], "content": m["content"]}
                    for m in st.session_state.messages
                ]
            # instruction = f'''Use only the context provided to provide a response to the latest user question:
            # context:
            # {json.dumps(messages)}
            # '''
            #retrieval_result = query_engine.query(instruction)

            retrieval_result = query_engine.query(json.dumps(messages))
            stream = retrieval_result.response
            #print('result is ',retrieval_result)
            response = st.write_stream(response_generator(stream))
        st.session_state.messages.append({"role":"assistant", "content":response})            

 
elif page == "Collaborators":
    st.title("👥 Collaborators")
    st.markdown("""
    ### Project Contributors:
    - **[Your Name]**: Lead Developer  
    - **[Contributor 2]**: Data Specialist  
    - **[Contributor 3]**: AI Researcher  
    - **[Contributor 4]**: Domain Expert  
    """)

elif page == "Chapter Details":
    st.title("🌍 About the Sri Lanka Chapter")
    st.markdown("""
    This initiative is part of the Sri Lanka Chapter's efforts to leverage AI for solving local challenges.  
    By digitizing and enhancing access to archival records, we aim to revolutionize regulatory decision-making processes in critical industries.
    """)
    st.image("images/SriLankaChapterLogo.png", width=300)
