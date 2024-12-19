import re
from datetime import datetime



def clean_text_for_metadata_extraction(text):
    """
    Clean and preprocess the extracted text content to make it suitable for metadata extraction.
    This will remove unwanted text such as 'Copyright' and any excessive spaces or line breaks.
    """
    if not isinstance(text, str):
        return None

    # Remove specific unwanted text like 'Copyright' (case-insensitive) and everything after it
    text = re.sub(r'copyright[\s\S]*?(\n|\r|\Z)', '', text, flags=re.IGNORECASE)  # Remove copyright section and text after it

    return text

def extract_metadata(row,client):
    """
    Extract title and published date from the document's text using the Groq API.
    The prompt is designed to instruct the Groq model to return a JSON object containing these two pieces of information.
    """
    text =  row['markdown_content_short']

    # Clean and preprocess the text before passing to the Groq API
    cleaned_text = clean_text_for_metadata_extraction(text)
    if not cleaned_text:
        return None

    prompt = (
        "Please process the following document and only extract the following details in English, "
        "in valid JSON format:\n\n"
        "- parsed_title: A single main title of the document (if available, otherwise return 'None').\n"
        "- parsed_issue_date: A single main published date of the document (if available, otherwise return 'None').\n"
        "- parsed_reference_number: The reference number of the document (if available, otherwise return 'None').\n\n"
        "Document:\n\n"
        f"{cleaned_text}\n\n"
        "Example output format:\n"
        '{"parsed_title": "<extracted title or None>", "parsed_issue_date": "<extracted date or None>", "parsed_reference_number": "<extracted reference number or None>"}\n'
    )


    # Send the prompt to the Groq API
    try:
        completion = client.chat.completions.create(
            # model="llama3-70b-8192",
            # model="llama3-8b-8192",
            model = "llama-3.1-8b-instant", # Make sure this model is appropriate for your task
            messages=[
                {"role": "user", "content": prompt},
                {"role": "assistant", "content": "```json"}
            ],
            stop="```",
        )

        # Try to extract the JSON response
        return completion.choices[0].message.content.strip("```json").strip()

    except (AttributeError, IndexError) as e:
        print(f"Error processing row: {e}")
        return None
    
import re
from datetime import datetime

def clean_date(date_str):
    """
    Reformats a given date string into a standardized format (YYYY-MM-DD).
    Handles inconsistent date orders such as DDMMYYYY, MMDDYYYY, and YYYYMMDD.
    Also corrects common misspellings like "augu" (August) or "mar" (March).

    Parameters:
        date_str (str): The input date string.

    Returns:
        str: A cleaned and reformatted date string, or None if the input is invalid.
    """
    try:
        # Normalize input: remove unnecessary characters and lowercase
        date_str = date_str.lower()
        date_str = re.sub(r'(issued in|of|,|st|nd|rd|th)', '', date_str)
        date_str = re.sub(r'(monday|tuesday|wednesday|thursday|friday|saturday|sunday)', '', date_str, flags=re.IGNORECASE)
        date_str = re.sub(r'[^a-zA-Z0-9\s]', '', date_str).strip()

        # Fix common spelling errors
        date_str = re.sub(r'\baugu\b', 'august', date_str)  # Correct 'augu' to 'august'
        date_str = re.sub(r'\bmar\b', 'march', date_str)    # Correct 'mar' to 'march'

        # Handle purely numeric strings
        if date_str.isdigit():
            if len(date_str) == 8:
                # Detect YYYYMMDD
                try:
                    year, month, day = int(date_str[:4]), int(date_str[4:6]), int(date_str[6:])
                    parsed_date = datetime(year, month, day)
                    return parsed_date.strftime("%Y-%m-%d")
                except ValueError:
                    pass

            elif len(date_str) == 8:  # Assume DDMMYYYY or MMDDYYYY if not YYYYMMDD
                day, month, year = None, None, None

                # Try DDMMYYYY first
                try:
                    day, month, year = int(date_str[:2]), int(date_str[2:4]), int(date_str[4:])
                    parsed_date = datetime(year, month, day)
                except ValueError:
                    # If invalid, fallback to MMDDYYYY
                    try:
                        month, day, year = int(date_str[:2]), int(date_str[2:4]), int(date_str[4:])
                        parsed_date = datetime(year, month, day)
                    except ValueError:
                        return None

                return parsed_date.strftime("%Y-%m-%d")

        # Handle other date formats using datetime
        possible_formats = [
            "%d %m %Y",  # DD MM YYYY
            "%m %d %Y",  # MM DD YYYY
            "%B %d %Y",  # MonthName DD YYYY
            "%d %B %Y",  # DD MonthName YYYY
            "%Y-%m-%d",  # YYYY-MM-DD
            "%m/%d/%Y",  # MM/DD/YYYY
            "%d/%m/%Y",  # DD/MM/YYYY
        ]

        for fmt in possible_formats:
            try:
                parsed_date = datetime.strptime(date_str, fmt)
                return parsed_date.strftime("%Y-%m-%d")
            except ValueError:
                continue

        # If no format matched, return None
        return None

    except Exception as e:
        print(f"Error processing date: {e}")
        return None
