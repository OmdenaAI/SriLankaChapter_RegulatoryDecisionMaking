import streamlit as st
import os
import time
from llama_index.core import Document, StorageContext, Settings
from rag_setup import setup_query_engine, setup_vector_store, setup_kdbai_session
import json

st.set_page_config(
    page_title="Sri Lanka Regulatory Archives Q&A",
    page_icon="📜",
    layout="wide"
)
DB_NAME = "srilanka_tri_circulars"
TABLE_NAME = "rag_baseline"

EMBEDDING_MODEL_NAME = "BAAI/bge-small-en-v1.5"

# Set up KDBAI session
kdbai_session = setup_kdbai_session()

#set up vector store
docs = []
llm, index = setup_vector_store(docs, kdbai_session, DB_NAME, TABLE_NAME, EMBEDDING_MODEL_NAME)

# Set up query engine
query_engine = setup_query_engine(index, llm)

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
            instruction = f'''Use only the context provided to provide a response to the latest user question:
            context:
            {json.dumps(messages)}
            '''
            retrieval_result = query_engine.query(instruction)
            stream = retrieval_result.response
            print('result is ',retrieval_result)
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
