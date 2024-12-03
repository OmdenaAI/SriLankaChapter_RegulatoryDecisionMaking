import os
import shutil
import pandas as pd
import img2pdf
from PyPDF2 import PdfWriter, PdfReader
import glob
from pathlib import Path
import logging
from tqdm import tqdm  # Import tqdm for progress bar

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def read_image_files_from_directory(image_dir_path):
    """
    Read all image files (jpg/PNG) from the given directory and return as a DataFrame.

    Args:
        image_dir_path (str): Path to the directory containing image files.

    Returns:
        pd.DataFrame: DataFrame containing image file names, paths, and their respective class folders.

    Raises:
        Exception: If there is an error while reading the image files or processing them.
    """
    try:
        logging.info(f"Reading image files from directory: {image_dir_path}")
        # Get List of all images (both .jpg and .PNG files)
        img_files = glob.glob(image_dir_path + "/**/*.jpg", recursive=True)
        img_files.extend(glob.glob(image_dir_path + "/**/*.PNG", recursive=True))

        # Create an empty list to store the file names and paths
        file_list = []

        # Use tqdm to show progress while iterating over files
        for file in tqdm(img_files, desc="Reading images", unit="file"):
            filename = Path(file).name  # Extract the file name
            class_folder_name = os.path.basename(os.path.dirname(file))  # Get the class folder name

            # Append the file name, path, and class to the list
            file_list.append(
                {"image_name": filename, "image_path": file, "image_class": class_folder_name}
            )

        # Create and return a DataFrame from the list
        df_img_data = pd.DataFrame(file_list)
        logging.info(f"Found {len(df_img_data)} image files.")
        return df_img_data

    except Exception as e:
        logging.error(f"Error reading image files from directory: {e}")
        raise


def create_template(df_img_data):
    """
    Create additional columns to store metadata about images.

    Args:
        df_img_data (pd.DataFrame): DataFrame containing image file metadata.

    Returns:
        pd.DataFrame: The DataFrame with added columns for image metadata.

    Raises:
        Exception: If there is an error while adding columns to the DataFrame.
    """
    try:
        logging.info("Creating template with additional columns.")
        # Initialize a 'not_needed' column with 0 values (default state)
        df_img_data["not_needed"] = 0

        # Initialize an 'is_duplicate_of' column with 0 values (default state)
        df_img_data["is_duplicate_of"] = 0

        # Initialize a column for page number and name of the image (set to 0 by default)
        df_img_data["if image is part of another, add page number and name"] = 0

        logging.info("Template created successfully.")
        return df_img_data

    except Exception as e:
        logging.error(f"Error creating template for DataFrame: {e}")
        raise


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


def read_csv(path):
    """
    Read data from an existing CSV file.

    Args:
        path (str): The file path to the CSV file.

    Returns:
        pd.DataFrame: The DataFrame containing the CSV data.

    Raises:
        FileNotFoundError: If the specified CSV file is not found.
        Exception: If there is an error while reading the CSV file.
    """
    try:
        logging.info(f"Reading CSV file: {path}")
        df = pd.read_csv(path)  # Read the CSV into a DataFrame
        df.fillna(0, inplace=True)  # Replace NaN values with 0
        logging.info(f"CSV file read successfully. Loaded {len(df)} rows.")
        return df

    except FileNotFoundError:
        logging.error(f"CSV file not found: {path}")
        raise
    except Exception as e:
        logging.error(f"Error reading CSV file {path}: {e}")
        raise


def remove_duplicates(df):
    """
    Remove rows marked as 'not_needed' or marked as duplicates.

    Args:
        df (pd.DataFrame): The DataFrame containing the image metadata.

    Returns:
        pd.DataFrame: The DataFrame after removing duplicates.

    Raises:
        KeyError: If required columns for filtering duplicates are missing.
        Exception: If there is an error while filtering duplicates.
    """
    try:
        logging.info("Removing duplicates and unnecessary rows.")
        # Filter out rows marked as 'not_needed' or marked as duplicates
        df = df[df["not_needed"] == 0]
        df = df[df["is_duplicate_of"] == "0"]
        df.reset_index(drop=True, inplace=True)  # Reset the index after filtering
        logging.info(f"Removed duplicates. Remaining rows: {len(df)}")
        return df

    except KeyError as e:
        logging.error(f"Missing required columns for duplicate removal: {e}")
        raise
    except Exception as e:
        logging.error(f"Error removing duplicates: {e}")
        raise


def clean_image_names(df):
    """
    Clean the image names by removing page numbers and other text.

    Args:
        df (pd.DataFrame): The DataFrame containing the image metadata.

    Returns:
        pd.DataFrame: The DataFrame with cleaned image names and page numbers.

    Raises:
        Exception: If there is an error cleaning the image names.
    """
    try:
        logging.info("Cleaning image names to prepare for merging PDFs.")
        # Clean the image names by removing text like 'page 1 of N'
        df["cleaned_image_name"] = df["if image is part of another, add page number and name"].str.replace(
            r"page \d+ of ", "", regex=True
        )

        # Extract and convert page number information
        df["page_number"] = (
            df["if image is part of another, add page number and name"]
            .str.extract(r"page (\d+) of")[0]
            .astype(int)
        )
        logging.info("Image names cleaned successfully.")
        return df

    except Exception as e:
        logging.error(f"Error cleaning PDF names: {e}")
        raise


def convert_image_to_pdf(image_path):
    """
    Convert a single image to PDF using img2pdf.

    Args:
        image_path (str): The path to the image file.

    Returns:
        str: The path to the generated PDF.

    Raises:
        FileNotFoundError: If the image file is not found.
        Exception: If there is an error during conversion.
    """
    try:
        logging.info(f"Converting image {image_path} to PDF.")
        # Replace image extension with .pdf to generate the PDF path
        pdf_path = image_path.replace(".jpg", ".pdf").replace(".PNG", ".pdf")

        # Convert image to PDF using img2pdf
        with open(pdf_path, "wb") as f:
            f.write(img2pdf.convert(image_path))

        logging.info(f"Image converted to PDF: {pdf_path}")
        return pdf_path

    except FileNotFoundError:
        logging.error(f"Image file not found: {image_path}")
        raise
    except Exception as e:
        logging.error(f"Error converting image to PDF: {e}")
        raise


def merge_pdfs_from_subset(image_subset, image_class):
    """
    Merge PDFs from a subset of images and save the merged PDF directly in the class folder.

    Args:
        image_subset (pd.DataFrame): DataFrame of image subset to merge.
        image_class (str): The class name of the image files.

    Returns:
        str: The path to the merged PDF.

    Raises:
        Exception: If there is an error while merging the PDFs.
    """
    try:
        logging.info(f"Merging PDFs from image subset for class: {image_class}")
        pdf_writer = PdfWriter()  # Initialize a PdfWriter object to merge PDFs

        # Temporary list to store paths of PDFs for cleanup after merging
        temp_pdfs = []

        # Use tqdm to show progress while iterating over image subset
        for index, row in tqdm(image_subset.iterrows(), desc=f"Merging PDFs for {image_class}", unit="file"):
            image_path = row["image_path"]

            # Convert each image to a temporary PDF using img2pdf
            temp_pdf_path = convert_image_to_pdf(image_path)

            # Add the temporary PDF path to the list for cleanup
            temp_pdfs.append(temp_pdf_path)

            # Check if the temporary PDF exists before trying to read it
            if os.path.exists(temp_pdf_path):
                pdf_reader = PdfReader(temp_pdf_path)  # Read the PDF

                # Append all pages of the temporary PDF to the writer
                for page in range(len(pdf_reader.pages)):
                    pdf_writer.add_page(pdf_reader.pages[page])
            else:
                logging.warning(f"Warning: The temporary PDF file '{temp_pdf_path}' does not exist.")

        # Generate output PDF path and save the merged PDF
        # output_pdf_path = os.path.join(image_class, f"{image_class}_merged.pdf")
        output_pdf_path = image_path.replace(".jpg", ".pdf").replace(".PNG", ".pdf")
        with open(output_pdf_path, "wb") as output_pdf:
            pdf_writer.write(output_pdf)

        # Clean up temporary PDF files after merging
        for temp_pdf in temp_pdfs:
            if os.path.exists(temp_pdf):
                os.remove(temp_pdf)

        logging.info(f"Merged PDF saved to: {output_pdf_path}")
        return output_pdf_path

    except Exception as e:
        logging.error(f"Error merging PDFs: {e}")
        raise


def process_images_to_pdf(df):
    """
    Process and convert images to PDFs, handle single-page and multi-page subsets.

    Args:
        df (pd.DataFrame): DataFrame containing image metadata.

    Returns:
        pd.DataFrame: The DataFrame containing the generated PDF paths.

    Raises:
        Exception: If there is an error during the entire image-to-PDF processing.
    """
    try:
        logging.info("Starting image-to-PDF processing.")

        # Handle single-page images
        single_page_df = df[df["if image is part of another, add page number and name"] == "0"]
        single_page_df = convert_single_image_to_pdf(single_page_df)

        # Filter out single-page images for multi-page processing
        filtered_df = df[df["if image is part of another, add page number and name"] != "0"]
        filtered_df = convert_multi_image_to_pdf(filtered_df)

        # Concatenate the results
        combined_df = pd.concat([single_page_df, filtered_df], ignore_index=True)
        combined_df.reset_index(drop=True, inplace=True)

        logging.info(f"Image-to-PDF processing complete. Total rows: {len(combined_df)}")
        return combined_df

    except Exception as e:
        logging.error(f"Error during image-to-PDF processing: {e}")
        raise


if __name__ == "__main__":
    try:
        logging.info("Starting the main script execution.")

        image_dir_path = "/content/drive/MyDrive/Omdena_Challenge/creating_new_LK_tea_dataset/tea_board_circulars"
        output_path_csv_template = "/content/drive/MyDrive/Colab Notebooks/Omdena RAG for Sri Lanka Tea/Task 1 - data scraping and data preprocessing/Week 1 - Add to Github/task1/collaborative csvs/phyarchive_circulers_image_annotation_template.csv"
        input_csv_path = "/content/drive/MyDrive/Colab Notebooks/Omdena RAG for Sri Lanka Tea/Task 1 - data scraping and data preprocessing/Week 1 - Add to Github/task1/collaborative csvs/phyarchive_circulers_image_annotation.csv"
        output_path_csv_image = "/content/drive/MyDrive/Colab Notebooks/Omdena RAG for Sri Lanka Tea/Task 1 - data scraping and data preprocessing/Week 1 - Add to Github/task1/collaborative csvs/phyarchive_circulers_image_data.csv"

        # Step 1: Read image files from the directory
        df_img_data = read_image_files_from_directory(image_dir_path)

        # Step 2: Create a template for the CSV file
        df_img_data = create_template(df_img_data)

        # Step 3: Write the template DataFrame to CSV
        write_csv(df_img_data, output_path_csv_template)

        # Step 4: Get the filled CSV from collaborators
        df_img_data = read_csv(input_csv_path)

        # Step 5: Remove duplicates
        df_img_data = remove_duplicates(df_img_data)

        # Step 6: Process images to PDF
        processed_df = process_images_to_pdf(df_img_data)

        # Step 7: Save processed data to a CSV
        write_csv(processed_df, output_path_csv_image)

        logging.info("Script execution completed successfully.")

    except Exception as e:
        logging.error(f"Error during main execution: {e}")
