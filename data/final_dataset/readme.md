# Dataset README

## Overview

This dataset contains a collection of legal and regulatory documents, which include various types of texts like Acts, Circulars, Regulations, and Guidelines. The data is used for advanced text parsing and document processing, focusing on extracting key details from these documents and presenting them in a structured format. The dataset is available in CSV format with various columns that describe the documents, including metadata about their extraction, quality, and format.

## Total Statistics

- **Total Words in 'markdown_content'**: 173,706
- **Total Characters in 'markdown_content'**: 1,074,790

## Dataset Columns

| Column Name             | Description                                                                                                     |
|-------------------------|-----------------------------------------------------------------------------------------------------------------|
| `id`                    | A unique identifier assigned to each document, starting from 0 and incrementing by 1 for each subsequent document. |
| `class`                 | Defines the type of the document. Current values are: `ACT`, `Circulers`, `Regulation`, and `Guideline`.        |
| `filename`              | The name of the PDF file or the text file (with `.txt` extension).                                              |
| `path`                  | The file path to the original document (PDF or text).                                                           |
| `url`                   | The URL from which the document was scraped or collected.                                                      |
| `language_label`        | The detected language of the document, currently only 'en' (English).                                           |
| `required_ocr`          | Indicates whether OCR (Optical Character Recognition) was necessary to extract text (`'yes'`, `'no'`, `'unextractable'`). |
| `has_tables`            | Whether the document contains tables (detected via the `gmft` library). Values: `'yes'` or `'no'`.              |
| `text_path`             | Path to the extracted text from the document (either from OCR or direct extraction).                            |
| `quality`               | Indicates the quality of the extracted text: `'good'` or `'bad'`.                                               |
| `data_origin`           | Describes the source of the document: `'original_dataset'` or `'scraped'`.                                      |
| `retrieved_date_of_issuance` | The date the document was issued, extracted during scraping.                                                  |
| `issuing_authority`     | The department or organization that issued the document, e.g., "TRI Circulers".                                 |
| `retrieved_topic`       | The topic or subject of the document, either inferred or extracted during scraping.                             |
| `text_content`          | The raw text content extracted from the document. This is often messy and requires further parsing.             |
| `PDF_or_text`           | Specifies whether the document is a PDF (`'pdf'`) or direct text (`'text'`).                                    |
| `llama_markdown_content`| The cleaned content with layout information, extracted via `llamaparse`. This content is suitable for markdown formatting. |
| `llama_title`           | The main title of the document, extracted via `llamaparse`.                                                    |
| `llama_issue_date`      | The issue date of the document, extracted via `llamaparse`.                                                     |
| `llama_reference_number`| The reference or serial number of the document, extracted via `llamaparse`.                                    |

## Dataset Formats

### Text Content (`text_content`)
- Raw text content extracted directly from documents, typically unclean and unformatted. This column is used for understanding and requires advanced parsing for correct structure and table extraction.

### Cleaned Markdown Content (`llama_markdown_content`)
- Cleaned content that includes layout information extracted using `llamaparse`. This is the preferred format for viewing the documents as it retains their structure. You can use [Markdown Live Preview](https://markdownlivepreview.com/) to view the formatted content.

  
