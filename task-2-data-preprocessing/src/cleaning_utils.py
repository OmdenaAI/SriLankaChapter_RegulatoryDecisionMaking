import ast
import re
import os
import pandas as pd


def merge_markdown_content(group):
    """Helper function to group the dataset by 'file_name' and
     apply a custom function to each group"""

    # Combine all markdown content in the group with line breaks
    combined_markdown_all = "\n".join(group['markdown_content'])

    # Take the first row's markdown content
    first_row = group.iloc[0]

    # Take the last two rows (if there are at least two)
    last_two_rows = group.tail(2)

    # Combine the first row text_content with the last two rows text_content
    combined_markdown_first_last = first_row['markdown_content'] + " " + " ".join(last_two_rows['markdown_content'])

    # Return a new series with 'file_name' and the combined markdown content
    return pd.Series({
        'file_name': first_row['file_name'],  # Keeping 'file_name' from the first row
        'markdown_content_long': combined_markdown_all,
        'markdown_content_short': combined_markdown_first_last
    })


def parse_json_from_string(metadata_string):
    """Helper function to clean and parse metadata string"""
    try:
        parsed_dict = ast.literal_eval(metadata_string)
        return parsed_dict  # Return the parsed JSON object
    except Exception as e:
        print(f"Error processing string as dict: {e}")
        return None
    
def append_merged_df_to_all_data(all_data, merged_df):
  """
  Appends the merged_df columns based on the 'file_name' to the all_data DataFrame.
  """
  # all_data = all_data.merge(merged_df[['file_name', 'markdown_content_long', 'parsed_title','parsed_issue_date', 'parsed_reference_number']], on='file_name', how='left')
  all_data = all_data.merge(merged_df[['filename', 'markdown_content', 'llama_title','llama_issue_date', 'llama_reference_number']], on='filename', how='left')

  return all_data


def remove_no_content_rows(df):
    """Removes rows from a DataFrame where 'markdown_content' contains only 'NO CONTENT HERE'."""

    # First, identify rows where 'markdown_content' contains only 'NO CONTENT HERE'
    empty_rows = df[df['markdown_content'].str.contains(r'^NO_CONTENT_HERE$', na=False)]

    # Print filenames for these rows
    print("Rows removed (filename with NO_CONTENT_HERE):")
    for filename in empty_rows['filename']:
        print(filename)

    # Remove rows with 'NO CONTENT HERE'
    df = df[df['markdown_content'] != 'NO_CONTENT_HERE']

    # Add 'is_empty' column, marking rows where the content was 'NO CONTENT HERE'
    df['is_empty'] = df['markdown_content'] == 'NO_CONTENT_HERE'

    return df


def normalize_markdown(text):
    """Function to normalize the markdown content"""
    # Normalize: Convert to lowercase, remove extra whitespace, etc.
    text = str(text).lower()  # Convert to lowercase
    text = re.sub(r'\s+', ' ', text)  # Replace multiple spaces with a single space
    text = text.strip()  # Remove leading/trailing whitespace
    return text


# Function to save markdown content to a file
def save_markdown(content, markdown_path):
    with open(markdown_path, 'w') as f:
        f.write(content)


def reformat_path_(path):
    path = re.sub('/content/drive/MyDrive/Omdena_Challenge/' ,'preprocessing_pipeline/',path)
    return path

# Function to process each row and save markdown
def process_row(row , reformat_path = False):
    # Extract filename and path
    pdf_filename = row['filename']  # e.g., 'file1.pdf'

    # Strip '.pdf' or '.txt' extension and replace with '.md'
    filename_without_extension = os.path.splitext(pdf_filename)[0]  # e.g., 'file1'
    markdown_filename = f"{filename_without_extension}.md"          # e.g., 'file1.md'

    # Determine the source file path for markdown
    if row['PDF_or_text'] == 'PDF':
        markdown_path = os.path.join(os.path.dirname(row['path']), markdown_filename)  # Use path for PDF
        markdown_content = row['markdown_content']  # Directly use markdown_content for PDF
    elif row['PDF_or_text'] == 'Text':
        markdown_path = os.path.join(os.path.dirname(row['text_path']), markdown_filename)  # Use text_path for Text
        markdown_content = row['markdown_content']  # Directly use markdown_content for Text (already extracted)

    if reformat_path:
        markdown_path = reformat_path_(markdown_path)
        
    # Save the markdown content to the file
    save_markdown(markdown_content, markdown_path)

    # Return the constructed markdown path to be used in the DataFrame
    return markdown_path