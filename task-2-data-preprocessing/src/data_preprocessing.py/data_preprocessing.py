# data labelling, annotation, selection

import os
import pandas as pd
from tqdm import tqdm
import logging
import os
import pandas as pd
import numpy as np
from google.colab import drive
from pdf2image import convert_from_path
from PIL import Image
import cv2
import PyPDF2
from langdetect import detect, DetectorFactory
import pytesseract
import logging
from tqdm import tqdm
import csv
import gmft
from gmft.pdf_bindings import PyPDFium2Document
from gmft.auto import CroppedTable, TableDetector
from gmft.auto import AutoTableFormatter
from gmft.auto import AutoFormatConfig



def read_csv(file_path):
    """
    Reads a CSV file and returns its contents as a list of dictionaries.

    This function attempts to open and read the provided CSV file. If the file
    doesn't exist or is unreadable, an error will be logged and an empty list
    will be returned. If there are issues with the file format (e.g., missing
    columns or invalid data), an error will be logged.

    Args:
        file_path (str): The path to the CSV file to be read.

    Returns:
        list of dict: A list of dictionaries representing the rows in the CSV
        file, where each dictionary corresponds to a row with keys as column
        headers.

    Raises:
        FileNotFoundError: If the specified file cannot be found.
        csv.Error: If there is an error while reading or parsing the CSV file.

    """
    try:
        with open(file_path, mode="r", newline="", encoding="utf-8") as file:
            csv_reader = csv.DictReader(file)
            rows = [row for row in csv_reader]
            logging.info(f"Successfully read the CSV file: {file_path}")
            return rows
    except FileNotFoundError as e:
        logging.error(f"File not found: {file_path} - {str(e)}")
        return []
    except csv.Error as e:
        logging.error(f"CSV error while reading {file_path} - {str(e)}")
        return []
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
        return []

def process_pdf_for_color_correction(pdf_path):
    """
    Processes a PDF for color correction and saves the processed images to a new PDF.

    Args:
        pdf_path (str): The path to the PDF file to process.

    Returns:
        str: The path to the processed PDF file.
    """
    try:
        original_dir = os.path.dirname(pdf_path)
        original_filename = os.path.basename(pdf_path)

        processed_dir = os.path.join(original_dir, "processed")
        if not os.path.exists(processed_dir):
            os.makedirs(processed_dir)

        images = convert_from_path(pdf_path, dpi=300)
        processed_images = []

        # Loop through all images to apply color correction with tqdm progress bar
        for i, image in tqdm(enumerate(images), total=len(images), desc="Processing images", unit="page"):
            image_np = np.array(image)
            gray_image = cv2.cvtColor(image_np, cv2.COLOR_BGR2GRAY)
            blurred_image = cv2.GussianBlur(gray_image, (5, 5), 0)
            kernel = np.array([[0, -1, 0], [-1, 5, -1], [0, -1, 0]])
            sharpened_image = cv2.filter2D(blurred_image, -1, kernel)
            binary_image = cv2.adaptiveThreshold(
                sharpened_image,
                255,
                cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
                cv2.THRESH_BINARY,
                11,
                2,
            )

            clahe = cv2.createCLAHE(clipLimit=3.0, tileGridSize=(8, 8))
            clahe_image = clahe.apply(sharpened_image)
            final_image = cv2.cvtColor(clahe_image, cv2.COLOR_GRAY2BGR)

            processed_pil_image = Image.fromarray(final_image)
            processed_images.append(processed_pil_image)

        # Save the processed images as a new PDF
        processed_pdf_path = os.path.join(
            processed_dir, original_filename.replace(".pdf", "_processed.pdf")
        )
        processed_images[0].save(
            processed_pdf_path,
            save_all=True,
            append_images=processed_images[1:],
            resolution=300,
            quality=95,
        )

        logging.info(f"Processed PDF saved to {processed_pdf_path}")
        return processed_pdf_path
    except Exception as e:
        logging.error(f"Error processing PDF {pdf_path}: {e}")
        return None




import logging
from tqdm import tqdm

def add_empty_columns_to_dataframe(df):
    """
    Adds empty columns to the DataFrame for quality, OCR status, and text content.
    If any of the columns already exist, raises a ValueError.

    Args:
        df (pd.DataFrame): The existing DataFrame to enhance.

    Returns:
        pd.DataFrame: The DataFrame with the new columns added.

    Raises:
        ValueError: If any of the columns to be added already exist in the DataFrame.
    """
    try:
        # Define the new columns with initial values
        new_columns = {
            "language_label": 'undetermined',
            "text_path": "",
            "text_content": "",
            "required_ocr": 'undetermined',
            "quality": 'undetermined',
            "has_tables": 'undetermined',
        }

        # Check if any of the columns already exist and raise an error
        existing_columns = set(df.columns)
        conflicting_columns = [col for col in new_columns if col in existing_columns]

        if conflicting_columns:
            raise ValueError(f"Columns already exist in DataFrame: {', '.join(conflicting_columns)}")

        # Add each new column to the DataFrame with the default values
        for column, default_value in tqdm(new_columns.items(), desc="Adding columns", unit="column"):
            df[column] = default_value

        return df

    except ValueError as ve:
        logging.error(f"Error adding columns: {ve}")
        raise  # Re-raise the error to propagate it further
    except Exception as e:
        logging.error(f"Unexpected error adding columns: {e}")
        return df  # Return the original DataFrame if an unexpected error occurs




def mark_non_english_pdfnames(row):
    """
    Mark a PDF as 'non-en' if it either:
    - Has a space followed by 's' before '.pdf'
    - Contains the word 'sinhala'

    The result is returned as 'non-en' or None.

    Args:
        row (pd.Series): A row of the DataFrame containing the 'filename' column.

    Returns:
        str: 'sin' for Sinahala language if the condition is met, otherwise None.
    """
    filename = row['filename']
    if isinstance(filename, str) and (pd.Series(filename).str.lower().str.contains(r'\s+s\.pdf$|sinhala', case=False, na=False).any()):
        return 'sin'
    return None

def clean_text_from_copyright(text):
    """
    Clean and preprocess the extracted text content to make it suitable for metadata extraction.
    This will remove unwanted text such as 'Copyright' and any excessive spaces or line breaks.
    """
    if not isinstance(text, str):
        return None

    # Remove specific unwanted text like 'Copyright' (case-insensitive) and everything after it
    text = re.sub(r'copyright[\s\S]*?(\n|\r|\Z)', '', text, flags=re.IGNORECASE)  # Remove copyright section and text after it

    return text

def sanitize_title(title):
    # Remove the substring "(lawnet)" from the title
    sanitized_title = title.replace("(lawnet)", "")
    return sanitized_title



def save_text_to_file(file_path, text):
    """
    Saves the extracted text into a text file.

    Args:
        file_path (str): The path where the text file should be saved.
        text (str): The text to be written to the file.
    """
    try:
        with open(file_path, "w", encoding="utf-8") as file:
            file.write(text)
        logging.info(f"Text saved successfully to {file_path}")
    except Exception as e:
        logging.error(f"Error saving text to {file_path}: {e}")

def extract_text_from_pdf(pdf_path):
    """
    Extracts text content from a PDF file using PyPDF2.

    Args:
        pdf_path (str): The path to the PDF file from which text should be extracted.

    Returns:
        str: The extracted text content.
    """
    try:
        reader = PyPDF2.PdfReader(pdf_path)
        text = ""
        for page in reader.pages:
            text += page.extract_text()
        return text.strip()  # Return extracted text
    except Exception as e:
        logging.error(f"Error extracting text from {pdf_path}: {e}")
        return ""

def process_pdf_pdfreader(df):
    """
    Processes PDFs in the DataFrame by extracting text from files where
    the language_label is not 'non-en'. The results are saved back into the DataFrame
    with columns 'text_path' and 'text_content'.

    Args:
        df (pandas.DataFrame): The DataFrame containing the paths and language labels.

    Returns:
        pandas.DataFrame: The updated DataFrame with 'text_path' and 'text_content' columns.
    """
    for idx, row in df.iterrows():
        if row['language_label'] == 'undetermined':  # Only process rows with non-'non-en' language_label
            pdf_path = row['path']
            if os.path.exists(pdf_path):
                logging.info(f"Processing PDF: {pdf_path}")
                extracted_text = extract_text_from_pdf(pdf_path)

                if extracted_text:  # If text is successfully extracted
                    # Define the text file path
                    text_file_path = os.path.splitext(pdf_path)[0] + ".txt"
                    # Save the extracted text to a text file
                    save_text_to_file(text_file_path, extracted_text)

                    # Update the DataFrame with the path to the text file and the extracted text
                    df.at[idx, 'text_path'] = text_file_path
                    df.at[idx, 'text_content'] = extracted_text
                    df.at[idx, 'required_ocr'] = 'no'
                    df.at[idx, 'quality'] = 'good'
                else:
                    logging.warning(f"No text extracted from {pdf_path}")
            else:
                logging.warning(f"PDF file does not exist: {pdf_path}")
    return df



def langdetect_language_in_txt(row):
    """
    Detects the language of a text file based on its path and updates the 'language_label' column in the DataFrame.

    Args:
        row (pd.Series): A row of the DataFrame containing the 'text_path' column.

    Returns:
        str: The detected language code (e.g., 'en' for English).
    """
    text_path = row['text_path']  # Extract the text file path from the row

    try:
        with open(text_path, "r", encoding="utf-8", errors="ignore") as file:
            text = file.read()

            if text.strip() == "":
                logging.warning(f"The file is empty: {text_path}")
                return "undetermined"
            else:
                language = detect(text)
                return language
    except Exception as e:
        logging.error(f"Error reading {text_path}: {e}")
        return "undetermined"




# def is_pdf_english(pdf_path):

#     """
#     Detects the language of a PDF file by extracting its text.

#     Args:
#         pdf_path (str): The path to the PDF file to analyze.

#     Returns:
#         tuple: A tuple containing the detected language and the type of text extracted (e.g., "text", "undetermined" or "error").
#     """
#     try:
#         with open(pdf_path, "rb") as file:
#             reader = PyPDF2.PdfReader(file)
#             text = ""
#             for page in reader.pages:
#                 text += page.extract_text() or ""

#             if text.strip() == "":
#                 return "undetermined", ""

#             language = detect(text)
#             return language, "text"
#     except Exception as e:
#         logging.error(f"Error detecting language for {pdf_path}: {e}")
#         return "undetermined", "error"





import logging
import os
from pdf2image import convert_from_path
import pytesseract

# Assuming you have the save_text_to_file function and read_text_file function already defined.

def ocr(pdf_path):
    """
    Applies Optical Character Recognition (OCR) to a PDF to extract text from its images.

    Args:
        pdf_path (str): The path to the PDF file on which OCR should be applied.

    Returns:
        str: The OCR extracted text.
    """
    text = ""
    try:
        images = convert_from_path(pdf_path)
        for img in images:
            text += pytesseract.image_to_string(img)
        return text
    except Exception as e:
        logging.error(f"OCR failed for {pdf_path}: {e}")
        return ""  # Ensure we return an empty string if OCR fails.

def OCR_on_pdf(pdf_path):
    """
    Performs OCR on a PDF file if no text was extracted from it.

    Args:
        pdf_path (str): The path to the PDF file to perform OCR on.

    Returns:
        tuple: A tuple containing the file path of the text file and the extraction method (either "OCR" or "not OCR extractable").
    """
    text_file_path = os.path.splitext(pdf_path)[0] + ".txt"
    try:
        text = ocr(pdf_path)
        if text.strip() != "":  # If text was extracted, save it to a file.
            save_text_to_file(text_file_path, text)
            return text_file_path, "OCR"
        else:
            logging.warning(f"No text found during OCR for {pdf_path}")
            return "", "not OCR extractable"
    except Exception as e:
        logging.error(f"Error during OCR for {pdf_path}: {e}")
        return "", "error"  # Return a clear error status.

def process_pdf_ocr(row):
    """
    Process a single row of the DataFrame based on conditions related to OCR and text extraction.

    Args:
        row (pd.Series): A row of the DataFrame containing PDF paths and related information.

    Returns:
        pd.Series: The updated row with 'text_content', 'required_ocr', and 'type_of_pdf' columns updated.
    """
    pdf_path = row['pdf_path']

    # Check if OCR is required based on the type_of_pdf
    if row['type_of_pdf'] in ["error", "undetermined"]:
        # Only pass the relevant pdf_path to OCR_on_pdf
        text_file, extraction_method = OCR_on_pdf(pdf_path)

        if text_file:  # If OCR is successful
            row['text_content'] = read_text_file(text_file)  # Assuming read_text_file is implemented.
            row['required_ocr'] = extraction_method
        else:
            row['text_content'] = ""  # Make sure we set an empty string if no OCR was successful
            row['required_ocr'] = extraction_method

    return row



def ingest_pdf(pdf_path):
    """
    Extracts tables and text content from a PDF.

    Args:
        pdf_path (str): The path to the PDF file to ingest.

    Returns:
        tuple: A tuple containing a list of detected tables and the document object.
    """
    config = AutoFormatConfig()
    config.semantic_spanning_cells = True  # [Experimental] better spanning cells
    config.enable_multi_header = True  # multi-indices
    formatter = AutoTableFormatter(config)
    detector = TableDetector()

    doc = PyPDFium2Document(pdf_path)

    tables = []
    for page in doc:
        tables += detector.extract(page)
    return tables, doc

def add_or_rename_id_column(df):
    """
    Adds or renumbers the 'id' column in the DataFrame. If 'id' already exists,
    it renumbers the values from 0 to the length of the DataFrame. If it doesn't exist,
    it creates the 'id' column and numbers it from 0 to the length of the DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to add or renumber the 'id' column.

    Returns:
        pd.DataFrame: The DataFrame with the 'id' column added or renumbered.
    """
    try:
        # Reset the index of the DataFrame first (optional)
        df.reset_index(drop=True, inplace=True)

        # Check if 'id' column exists
        if 'id' in df.columns:
            # If 'id' exists, renumber from 0 to len(df) - 1
            df['id'] = range(0, len(df))
        else:
            # If 'id' doesn't exist, add it and number from 0 to len(df) - 1
            df['id'] = range(0, len(df))

        return df

    except Exception as e:
        logging.error(f"Error while adding or renumbering the 'id' column: {e}")
        return df  # Return the original DataFrame if something goes wrong

# Example usage:
df_PDF_data = add_or_rename_id_column(df_PDF_data)


def reorder_columns(df):
    """
    Reorders the columns of the DataFrame according to the specified preferred column names.

    Args:
        df (pd.DataFrame): The DataFrame to reorder.
        preferred_column_names (list): A list of column names in the desired order.

    Returns:
        pd.DataFrame: The DataFrame with columns reordered.
    """
    preferred_column_names = [
    'id', 'class', 'filename', 'file_type', 'file_path', 'data_origin', 'url','retrieved_date_of_issuance', 'retrieved_title', 'issuing_authority', 'language_label',
    'has_tables',  'required_ocr', 'extracted_text_path', 'text_content', 'quality'

    ]
    # Ensure the DataFrame contains all the required columns
    missing_columns = [col for col in preferred_column_names if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing columns: {', '.join(missing_columns)}")

    # Reorder the columns of the DataFrame
    return df[preferred_column_names]



def detect_tables(pdf_path):
    """
    Detects if a PDF contains tables.

    Args:
        pdf_path (str): The path to the PDF file to check.

    Returns:
        str: 'yes' if tables are detected, 'no' if no tables are found, or 'error' if there was an issue.
    """
    try:
        tables, _ = ingest_pdf(pdf_path)
        return "yes" if len(tables) > 0 else "no"
    except Exception as e:
        logging.error(f"Error detecting tables in {pdf_path}: {e}")
        return "error"

def write_csv(df, output_path):
    """
    Save the DataFrame to a CSV file.

    Args:
        df (pd.DataFrame): The DataFrame to save.
        output_path (str): The file path where the CSV file should be saved.

    Raises:
        Exception: If there is an error while saving the DataFrame to CSV.
    """
    try:
        logging.info(f"Saving DataFrame to CSV: {output_path}")
        df.to_csv(output_path, index=False)  # Save to CSV without the index column
        logging.info(f"CSV file saved to {output_path}")

    except Exception as e:
        logging.error(f"Error saving CSV file to {output_path}: {e}")
        raise


def main(raw_csv_path, output_csv_path):
    """
    Main function to process PDFs from the given directory and save the results to a CSV.

    Args:
        pdf_directory (str): Path to the directory containing PDF files to process.
        output_csv (str): Path where the output CSV file with metadata will be saved.

    Returns:
        None
    """

    all_data = read_csv(raw_csv_path)
    df = all_data[all_data['PDF_or_text'] == 'PDF']

    df['processed_paths'] = df['path'].apply(process_pdf_for_color_correction)

    df['language_label'] = df.apply(mark_non_english_pdfnames, axis=1)

    # Step 3: Add empty columns for quality, OCR status, and text content
    logging.info("Adding empty columns to DataFrame...")
    df = add_empty_columns_to_dataframe(df)

    df = process_pdf_pdfreader(df)


    # Step 4: Perform OCR where necessary, and fill the OCR, text_content, and language columns
    logging.info("Performing OCR and text extraction...")
    df = df.apply(process_pdf_ocr, axis=1)

    # Apply the langdetect_language_in_txt function to each row and update the 'language_label' column
    df['language_label'] = df.apply(langdetect_language_in_txt, axis=1)


    # Step 5: Check if tables are detected and update the DataFrame
    # there are no tables in text, so only path which has
    logging.info("Detecting tables in PDFs...")
    # Apply detect_tables only where 'pdf_or_text' is 'pdf'
    df.loc[df['pdf_or_text'] == 'pdf', 'tables_detected'] = df[df['pdf_or_text'] == 'pdf']['path'].apply(detect_tables)



    # Apply the function to reorder the columns of the DataFrame
    df_PDF_data = reorder_columns(df_PDF_data, preferred_column_names)


    # Step 6: Save the resulting DataFrame to a CSV
    logging.info(f"Saving results to {output_csv}...")
    write_csv(df, output_csv_path)

    logging.info("Processing completed.")

# Example usage
if __name__ == "__main__":

    # Initialize global variables
    DetectorFactory.seed = 0

    # Setup logging
    logging.basicConfig(
        filename="app.log",
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    raw_csv_path = "path/to/pdf/directory"  # Path where your PDFs are located
    output_csv_path = "output_metadata.csv"  # Output CSV file path
    main(raw_csv_path, output_csv_path)
