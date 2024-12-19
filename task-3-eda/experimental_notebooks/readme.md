# Tokenization Statistical Analysis

## 1. Introduction
This analysis focuses on exploring tokenization and semantic chunking for a regulatory knowledge base. The goal is to understand token distributions, document statistics, and chunk-level properties across the dataset. 

The dataset includes fields such as:
- **`class`**: Category of the document (e.g., regulatory, circular, guideline).
- **`markdown_content`**: Full markdown text content for each document.
- **`tokens`**: Tokenized representation of the markdown text.
- **Chunk Statistics**: Metrics related to semantic chunking.

---

## 2. Dataset Overview
**Key Statistics:**
- The dataset comprises multiple columns, including:
  - `class`
  - `quality`
  - `tokens`
  - `chunk_count`
- Summary statistics include the shape (number of rows and columns) and basic feature descriptions.

---

## 3. Class Distribution
This section examines the distribution of documents across different classes, origins, and quality indicators.

**Plot: Count of Each Class in Different Columns**  
![Class Distribution](plots/EDA_document_counts_per_class.png)

---

## 4. Document Statistics
Analysis of document-level statistics such as document counts, token counts, and unique token counts across different classes.

**Plot: Document Statistics for Each Class**  
![Document Statistics](plots/EDA_all_document_stats.png)

---

## 5. Token Statistics

### Tokenization Overview
Analyzing the total and unique token counts for each document. This helps to understand the textual variability within the dataset.

**Plot: Token Distribution**  
![Token Distribution](plots/EDA_token_distribution.png)

### Class-wise Token Statistics
Examining token distributions across individual document classes to highlight intra-class variability.

**Plot: Token Distribution per Class**  
![Token Distribution per Class](plots/EDA_token_distribution_per_class.png)

---

## 6. Text Length Insights
Exploration of the length of documents (in tokens) across different classes using boxplots. This helps to identify variations in document sizes.

**Plot: Token Count Boxplot per Class**  
![Token Count Boxplot](plots/EDA_token_boxplot_per_class.png)

---

## 7. Chunk Analysis

### Chunk Counts
Investigation of the number of semantic chunks created per document.

**Plot: Chunks per Document**  
![Chunks per Document](plots/EDA_chunks_per_doc_countplot.png)

### Chunk Sizes
Exploration of average and maximum chunk sizes in terms of token counts.

- **Plot: Average Chunk Tokens**  
  ![Average Chunk Tokens](plots/EDA_avg_chunk_tokens_per_doc.png)

- **Plot: Maximum Chunk Tokens**  
  ![Maximum Chunk Tokens](plots/EDA_max_chunk_tokens_per_doc.png)

---

## 8. Word Clouds

### All Documents
Visualization of frequently occurring terms across the entire dataset.

**Plot: Word Cloud for All Document Tokens**  
![Word Cloud for All Tokens](plots/EDA_all_tokens_wordcloud.png)

### Class-Specific Word Clouds
Highlighting the most frequent terms for specific document classes:
- **Regulatory Documents**  
  ![Regulatory Word Cloud](plots/EDA_regulatory_tokens_wordcloud.png)

- **Circular Documents**  
  ![Circular Word Cloud](plots/EDA_circular_tokens_wordcloud.png)

- **Guideline Documents**  
  ![Guideline Word Cloud](plots/EDA_guideline_tokens_wordcloud.png)