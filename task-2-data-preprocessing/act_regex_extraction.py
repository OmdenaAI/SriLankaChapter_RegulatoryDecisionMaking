"""
Original code by Memoona in https://colab.research.google.com/drive/
1X52Y0RhuIUUONq1AoYGd8Hd6hh5-9BwF#scrollTo=E8XPUxMf-uPo

This file loads the raw csv for Acts scrpaed from lawnet, and uses regex to extract
the topic and date of issue from the text content of each file. It saves the result
in a csv in the task2_preprocessed_data's lawnet folder.
"""
import pandas as pd
import re
import os

# Initialize paths
SCRIPT_PATH = os.path.dirname(__file__)
RAW_INPUT_REL_PATH = "..\\data\\task1_raw_input\\"
LAWNET_V0 = "data_source_lawnet\\v0_0"
CSV_LAWNET = os.path.join(
    SCRIPT_PATH, RAW_INPUT_REL_PATH, LAWNET_V0, "v0_0_LK_tea_lawnet_raw.csv"
)

TASK2_REL_PATH = "..\\data\\task2_preprocessed_data\\"
CSV_DEST = os.path.join(
    SCRIPT_PATH, TASK2_REL_PATH, LAWNET_V0, "v0_0_LK_tea_lawnet_regex.csv"
)


def clean_text(text):
    """
    Formats the incoming text for easier parsing.

    Args:
        text (str): Text content of a file.

    Returns:
        str: Text with punctuation fixes.
    """
    # Convert to lowercase
    text = text.lower()
    # Remove specified punctuation
    text = text.replace(",", "").replace(":", "").replace('"', "").replace("'", "")
    # Replace multiple spaces with a single space
    text = re.sub(r"\s+", " ", text)
    # Strip leading and trailing spaces
    text = text.strip()

    return text


def read_file_content(file_path):
    """
    Check if a file exists and read its content using fallback encodings.

    Args:
        file_path (str): Path to the text file.

    Returns:
        str: The content of the file, or a message if the file does not exist.
    """
    try:
        absolute_file_path = SCRIPT_PATH + "\\..\\" + file_path
        if os.path.exists(absolute_file_path):
            encodings = ["utf-8", "ISO-8859-1", "Windows-1252"]
            for encoding in encodings:
                try:
                    with open(absolute_file_path, "r", encoding=encoding) as file:
                        txt = file.read()
                        cln_txt = clean_text(txt)
                        return cln_txt
                except UnicodeDecodeError:
                    continue
            return "Could not decode the file with common encodings."
        else:
            return f"File not found: {absolute_file_path}"
    except Exception as e:
        print(f"Error reading file '{absolute_file_path}': {e}")
        return ""


def extract_act_topic(string):
    """
    Extracts the topic from the text using regex.

    Args:
        file_path (str): Cleaned text of the file.

    Returns:
        str: The topic of the text.
    """
    try:
        # Updated topic patterns
        topic_regex = [
            (
                r"(?:this|the) act may be cited as (.*?) act"
                r"[.,]?\s*no[.,-]?\s*(\d+)\s*of\s*(\d{4})"
            ),  # long regex split into 2 lines
            r"this law may be cited as (.*?) law\s*no\.\s*(\d+)\s*of\s*(\d{4})",
            r"this act may be cited as (.*?) act[.,]",
        ]

        matched_topics = []
        matched_topics_str = ""
        for pattern in topic_regex:
            matches = re.findall(pattern, string)
            # print(f"Matches for pattern {matches}")  # Debugging output
            if len(matches) > 0:
                for match in matches:
                    matched_topics.append(match)
                    # matched_topics is a list of 1 tuple e.g.
                    # [('the food (amendment)', '29', '2011')]
                if isinstance(
                    matched_topics[0], str
                ):  # for the row with id 17 that uses the 3rd regex,
                    # the match contains just a string instead of a tuple.
                    matched_topics_str = matched_topics[0] + " act"
                elif len(matched_topics[0]) == 3:
                    matched_topics_str = (
                        matched_topics[0][0]
                        + " act no. "
                        + matched_topics[0][1]
                        + " of "
                        + matched_topics[0][2]
                    )
                elif len(matched_topics[0]) == 2:
                    matched_topics_str = (
                        matched_topics[0][0] + " act no. " + matched_topics[0][1]
                    )
                elif len(matched_topics[0]) == 1:
                    matched_topics_str = matched_topics[0][0] + " act"
                # if a pattern is found, no need to check the next ones from topic_regex
                break
        return matched_topics_str
    except Exception as e:
        print(f"Error extracting topic: {e}")
        return ""


def format_date(date):
    """
    Convert a date in format1 [year_only] or format2 (day, suffix, month, year)
    into a standardized DD/MM/YYYY string.

    Args:
        date (list or tuple): The date information.
            - format1: [year_only] e.g., ['1958']
            - format2: (day, suffix, month, year) e.g., ('12', 'th', 'august', '1997')

    Returns:
        str: The date string in DD/MM/YYYY format.
    """
    if len(date) == 1:
        if isinstance(date[0], tuple) and len(date[0]) == 4:
            # format2: [(day, suffix, month, year)]
            day, _, month, year = date[0]

            # Convert day to integer
            day = int(day)

            # Map month to its numeric value
            month_map = {
                "january": 1,
                "february": 2,
                "march": 3,
                "april": 4,
                "may": 5,
                "june": 6,
                "july": 7,
                "august": 8,
                "september": 9,
                "october": 10,
                "november": 11,
                "december": 12,
            }
            month_number = month_map[
                month.lower()
            ]  # Convert month to lowercase for matching

            return f"{day:02d}/{month_number:02d}/{year}"
        else:
            # format1: [year_only]
            year = date[0]
            return f"01/01/{year}"
    else:
        # return empty string instead of an exception
        return ""


def extract_act_year(string):
    """
    Extracts the year / date from the text using regex.

    Args:
        file_path (str): Cleaned text of the file.

    Returns:
        str: The date of issuance of the text. If we get the year only, we put the
        date as 1 Jan
    """
    try:
        # Regex patterns to find the year in the specified format
        year_regex = [
            r"\[\*\*(\d{1,2})\*\*([a-zA-Z]{2})\s*(\w+)\s*(\d{4})\s*\]",  # changed the order
            r"this act may be cited as (.*?) act\s*no\.\s*(\d+)\s*of\s*(\d{4})",
            r"this law may be cited as (.*?) law\s*no\.\s*(\d+)\s*of\s*(\d{4})",
        ]

        matched_year = []
        all_matches = []

        # Collect all matches from each pattern
        for pattern in year_regex:
            matches = re.findall(pattern, string)
            if len(matches) > 0:
                all_matches.extend(matches)  # Add all found matches to the list
                break  # found a match, no need to check the other year_regex patterns

        # Check lengths of collected matches
        for match in all_matches:
            if len(match) == 4:  # Third regex pattern returns a tuple of 4
                # Append the full match (all captured groups)
                matched_year.append(match)
            elif len(match) == 3:  # First two regex patterns return a tuple of 3
                # Extract the year from group 2 (the year is in group 2)
                matched_year.append(match[2])

        formatted_date = format_date(matched_year)
        return formatted_date
    except Exception as e:
        print(f"Error extracting year: {e}")
        return ""


def extract_act_topic_date_regex(lawnet_csv=CSV_LAWNET, destination_csv=CSV_DEST):
    """
    Extracts the date and topic from the text using regex.

    Args:
        lawnet_csv (str): Absolute path to the raw lawnet csv.
        destination_csv (str): Absolute path to the desired destination csv.

    Returns:
        None
    """
    try:
        if not os.path.exists(lawnet_csv):
            raise FileNotFoundError(f"The file '{lawnet_csv}' does not exist.")
        data = pd.read_csv(lawnet_csv)

        if "path" not in data.columns:
            raise KeyError("path column is missing from the input CSV file.")

        data["text_content"] = data["path"].apply(read_file_content)
        data["regex_topic"] = data["text_content"].apply(extract_act_topic)
        data["regex_date_of_issuance"] = data["text_content"].apply(extract_act_year)
        data.to_csv(destination_csv, index=False)
        print(f"Topic and Date extracted for acts and stored in {destination_csv}")

    except FileNotFoundError as fnf_error:
        print(fnf_error)
    except KeyError as key_error:
        print(key_error)
    except pd.errors.EmptyDataError:
        print(f"The file '{lawnet_csv}' is empty.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


# Entry point for the script
if __name__ == "__main__":
    extract_act_topic_date_regex()
