import streamlit as st
import streamlit as st
from langchain.embeddings import HuggingFaceEmbeddings
from langchain.vectorstores import FAISS
from langchain.schema import Document
from langchain.chains import RetrievalQA
from langchain.llms import HuggingFacePipeline
from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline
from PIL import Image
import os
import time

st.set_page_config(
    page_title="Sri Lanka Regulatory Archives Q&A",
    page_icon="📜",
    layout="wide"
)

# Sidebar Navigation
with st.sidebar:
    st.image("SriLankaChapterLogo.jpeg", use_container_width=True)
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

    st.header("📂 Upload Documents")
    uploaded_file = st.file_uploader("Upload a Text file", type=["txt"])
    
    loading_placeholder = st.empty()
    
    if uploaded_file:
        file_name = uploaded_file.name
        loading_placeholder.info("📜 Processing the document...")
        with st.spinner("Reading text from the document..."):
            document_text = uploaded_file.read().decode("utf-8")
        with st.spinner("Generating embeddings..."):
            # Initialize the HuggingFace embeddings model
            embeddings = HuggingFaceEmbeddings(model_name="sentence-transformers/all-MiniLM-L6-v2")
            document_chunks = [Document(page_content=document_text)]
            vector_store = FAISS.from_documents(document_chunks, embeddings)
        with st.spinner("Setting up RAG system..."):
            retriever = vector_store.as_retriever()
            tokenizer = AutoTokenizer.from_pretrained('google/flan-t5-xl')
            model = AutoModelForSeq2SeqLM.from_pretrained('google/flan-t5-xl')
            hf_pipeline = pipeline(
                "text2text-generation",
                model=model,
                tokenizer=tokenizer,
                max_length=512,
                temperature=0.1,
                top_p=0.9,
                repetition_penalty=1.2
            )
            llm = HuggingFacePipeline(pipeline=hf_pipeline)
            
            qa_chain = RetrievalQA.from_chain_type(
                llm=llm,
                retriever=retriever,
                chain_type="stuff",
            )
        
        st.success("Document processed successfully!")
        loading_placeholder.empty()
        st.header("💬 Ask Questions")
        question = st.text_input("Ask a question about the uploaded document:")
        if question:
            with st.spinner("Searching for the best answer..."):
                answer = qa_chain.run(question)
            st.markdown(f"**Answer:** {answer}")
    
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
    st.image("SriLankaChapterLogo.jpeg", use_container_width=True)
