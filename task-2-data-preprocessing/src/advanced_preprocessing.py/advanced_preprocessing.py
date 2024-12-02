import os
import pandas as pd
import numpy as np
import logging
import csv
import cv2
from pdf2image import convert_from_path
from PIL import Image
import PyPDF2
from langdetect import detect, DetectorFactory
import pytesseract
from tqdm import tqdm
from gmft.pdf_bindings import PyPDFium2Document
from gmft.auto import TableDetector, AutoTableFormatter, AutoFormatConfig

# Set seed for reproducibility
DetectorFactory.seed = 0

# Configure logging
logging.basicConfig(
    filename="app.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

def read_csv(file_path):
    """
    Reads a CSV file and returns its contents as a list of dictionaries.

    Args:
        file_path (str): Path to the CSV file.

    Returns:
        list of dict: List of dictionaries representing CSV rows.
    """
    try:
        with open(file_path, mode="r", newline="", encoding="utf-8") as file:
            csv_reader = csv.DictReader(file)
            rows = [row for row in csv_reader]
        logging.info(f"Successfully read the CSV file: {file_path}")
        return rows
    except FileNotFoundError as e:
        logging.error(f"File not found: {file_path} - {str(e)}")
    except csv.Error as e:
        logging.error(f"CSV error while reading {file_path} - {str(e)}")
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
    return []

def process_pdf_for_color_correction(pdf_path):
    """
    Applies color correction to the pages of a PDF and saves the corrected PDF.

    Args:
        pdf_path (str): Path to the input PDF file.

    Returns:
        str: Path to the processed PDF.
    """
    try:
        original_dir = os.path.dirname(pdf_path)
        original_filename = os.path.basename(pdf_path)
        processed_dir = os.path.join(original_dir, "processed")
        os.makedirs(processed_dir, exist_ok=True)

        images = convert_from_path(pdf_path, dpi=300)
        processed_images = []

        for i, image in tqdm(enumerate(images), total=len(images), desc="Processing images", unit="page"):
            image_np = np.array(image)
            gray_image = cv2.cvtColor(image_np, cv2.COLOR_BGR2GRAY)
            sharpened_image = cv2.filter2D(gray_image, -1, np.array([[0, -1, 0], [-1, 5, -1], [0, -1, 0]]))
            binary_image = cv2.adaptiveThreshold(
                sharpened_image, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 11, 2
            )
            processed_image = Image.fromarray(binary_image)
            processed_images.append(processed_image)

        processed_pdf_path = os.path.join(processed_dir, original_filename.replace(".pdf", "_processed.pdf"))
        processed_images[0].save(
            processed_pdf_path, save_all=True, append_images=processed_images[1:], resolution=300, quality=95
        )
        logging.info(f"Processed PDF saved to {processed_pdf_path}")
        return processed_pdf_path
    except Exception as e:
        logging.error(f"Error processing PDF {pdf_path}: {e}")
    return None

def add_empty_columns_to_dataframe(df):
    """
    Adds empty columns to the DataFrame for text extraction status, quality, and language.

    Args:
        df (pd.DataFrame): DataFrame to enhance.

    Returns:
        pd.DataFrame: DataFrame with added columns.
    
    Raises:
        ValueError: If any of the columns to be added already exist.
    """
    try:
        new_columns = {
            "language_label": "undetermined",
            "text_path": "",
            "text_content": "",
            "required_ocr": "undetermined",
            "quality": "undetermined",
            "has_tables": "undetermined",
        }
        existing_columns = set(df.columns)
        conflicting_columns = [col for col in new_columns if col in existing_columns]
        if conflicting_columns:
            raise ValueError(f"Columns already exist: {', '.join(conflicting_columns)}")

        for column, default_value in tqdm(new_columns.items(), desc="Adding columns", unit="column"):
            df[column] = default_value

        return df
    except ValueError as ve:
        logging.error(f"Error adding columns: {ve}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error adding columns: {e}")
    return df

def extract_text_from_pdf(pdf_path):
    """
    Extracts text content from a PDF using PyPDF2.

    Args:
        pdf_path (str): Path to the PDF file.

    Returns:
        str: Extracted text content.
    """
    try:
        reader = PyPDF2.PdfReader(pdf_path)
        text = ""
        for page in reader.pages:
            text += page.extract_text()
        return text.strip()
    except Exception as e:
        logging.error(f"Error extracting text from {pdf_path}: {e}")
    return ""

def save_text_to_file(file_path, text):
    """
    Saves extracted text to a file.

    Args:
        file_path (str): Path where the text will be saved.
        text (str): Text to save.

    Returns:
        None
    """
    try:
        with open(file_path, "w", encoding="utf-8") as file:
            file.write(text)
        logging.info(f"Text saved successfully to {file_path}")
    except Exception as e:
        logging.error(f"Error saving text to {file_path}: {e}")

def ocr(pdf_path):
    """
    Performs Optical Character Recognition (OCR) on a PDF file to extract text from images.

    Args:
        pdf_path (str): Path to the PDF file.

    Returns:
        str: OCR extracted text.
    """
    text = ""
    try:
        images = convert_from_path(pdf_path)
        for img in images:
            text += pytesseract.image_to_string(img)
        return text
    except Exception as e:
        logging.error(f"OCR failed for {pdf_path}: {e}")
    return ""

def detect_language_in_text(text):
    """
    Detects the language of the provided text.

    Args:
        text (str): Text to analyze.

    Returns:
        str: Detected language code (e.g., 'en' for English).
    """
    try:
        return detect(text)
    except Exception as e:
        logging.error(f"Language detection failed: {e}")
    return "undetermined"

def ingest_pdf(pdf_path):
    """
    Extracts tables and text content from a PDF using GMFT.

    Args:
        pdf_path (str): Path to the PDF file.

    Returns:
        tuple: List of detected tables and the document object.
    """
    config = AutoFormatConfig()
    config.semantic_spanning_cells = True
    config.enable_multi_header = True
    formatter = AutoTableFormatter(config)
    detector = TableDetector()

    doc = PyPDFium2Document(pdf_path)
    tables = []
    for page in doc:
        tables += detector.extract(page)
    return tables, doc

def detect_tables(pdf_path):
    """
    Detects whether a PDF contains tables.

    Args:
        pdf_path (str): Path to the PDF file.

    Returns:
        str: 'yes' if tables are detected, 'no' if none are found, 'error' on failure.
    """
    try:
        tables, _ = ingest_pdf(pdf_path)
        return "yes" if len(tables) > 0 else "no"
    except Exception as e:
        logging.error(f"Error detecting tables in {pdf_path}: {e}")
    return "error"

def write_csv(df, output_path):
    """
    Saves the DataFrame to a CSV file.

    Args:
        df (pd.DataFrame): DataFrame to save.
        output_path (str): Path to save the CSV file.

    Returns:
        None
    """
    try:
        logging.info(f"Saving DataFrame to CSV: {output_path}")
        df.to_csv(output_path, index=False)
        logging.info(f"CSV file saved to {output_path}")
    except Exception as e:
        logging.error(f"Error saving CSV file to {output_path}: {e}")
        raise

def main(raw_csv_path, output_csv_path):
    """
    Main function to process PDFs and generate metadata CSV.

    Args:
        raw_csv_path (str): Path to input raw CSV with PDF details.
        output_csv_path (str): Path to output the final CSV.

    Returns:
        None
    """
    rows = read_csv(raw_csv_path)
    if not rows:
        logging.error("No data to process.")
        return

    df = pd.DataFrame(rows)
    df = add_empty_columns_to_dataframe(df)

    for i, row in tqdm(df.iterrows(), total=len(df), desc="Processing PDFs"):
        pdf_path = row["file_path"]
        if not os.path.exists(pdf_path):
            logging.warning(f"PDF does not exist: {pdf_path}")
            continue

        # Detect language
        text = extract_text_from_pdf(pdf_path) or ocr(pdf_path)
        df.at[i, "text_content"] = text
        df.at[i, "language_label"] = detect_language_in_text(text)

        # Process PDF for tables and text quality
        df.at[i, "has_tables"] = detect_tables(pdf_path)
        df.at[i, "quality"] = "high" if df.at[i, "has_tables"] == "yes" else "low"

        # Save text to file
        text_path = f"{os.path.splitext(pdf_path)[0]}.txt"
        save_text_to_file(text_path, text)
        df.at[i, "text_path"] = text_path

    # Save DataFrame to CSV
    write_csv(df, output_csv_path)

if __name__ == "__main__":
    # Example Usage
    raw_csv_path = "input.csv"
    output_csv_path = "output.csv"
    main(raw_csv_path, output_csv_path)
