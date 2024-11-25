import pandas as pd
import json
import os
import re


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