import bz2
import re
from pathlib import Path
from time import sleep
from typing import Dict, Optional, Union

import pandas as pd
import wikitextparser as wtp
from html2text import html2text as htt
from tqdm import tqdm  # for progress tracking


def format_wiki_text(filename: Union[str, Path], savepath: Union[str, Path]) -> None:
    """Process a Wikipedia XML dump file and save articles into a Parquet file.

    Args:
        filename: Path to the bzip2 compressed XML dump file
        savepath: Path to the output Parquet file
    """
    # Convert to Path objects
    filename = Path(filename)
    savepath = Path(savepath)

    # Ensure save directory exists
    savepath.parent.mkdir(parents=True, exist_ok=True)

    # Count total number of articles (pages) in the file
    total_pages = _count_pages_in_file(filename)
    print(f"Total unique articles to process: {total_pages}")
    sleep(0.1)  # Allow print to be shown before tqdm

    articles = []
    article = ""

    with bz2.open(filename, "rt", encoding="utf-8") as infile:
        # Initialize tqdm for progress tracking
        with tqdm(
            total=total_pages, desc="Processing articles", unit="article"
        ) as pbar:
            for line in infile:
                if "<page>" in line:
                    article = ""
                elif "</page>" in line:  # end of article
                    doc = _analyze_chunk(article)
                    if doc:
                        articles.append(doc)
                    pbar.update(1)  # Update progress bar for each processed article
                else:
                    article += line

    # Convert list of articles to a DataFrame
    df = pd.DataFrame(articles)

    # Save DataFrame as Parquet file
    df.to_parquet(savepath, index=False)
    print(f"Processed data saved to {savepath}")


def _analyze_chunk(text: str) -> Optional[Dict[str, str]]:
    """Analyze a Wikipedia article chunk and extract relevant information.

    Args:
        text: Raw XML chunk containing a Wikipedia article.

    Returns:
        Dictionary containing article title, content, and ID if valid.
        Returns None if the article should be skipped.
    """
    try:
        # Skip articles that are redirects or disambiguation pages
        if '<redirect title="' in text:  # this is not the main article
            return None
        if "(disambiguation)" in text:  # this is not an article
            return None

        # Extract the title and validate it
        title = text.split("<title>")[1].split("</title>")[0]
        title = htt(title)
        # Skip articles with colons in their title (likely metadata or lists)
        if ":" in title:
            return None

        # Extract the article's unique ID
        serial = text.split("<id>")[1].split("</id>")[0]

        # Extract and process the article's content
        content = text.split("</text")[0].split("<text")[1].split(">", maxsplit=1)[1]
        content = _dewiki(content)

        # Add the title to the beginning of the content, separated by a newline
        content = f"= {title.strip()} =\n\n{content.strip()}"

        # Return a dictionary with the extracted data
        return {"title": title.strip(), "text": content, "id": serial.strip()}

    except Exception as oops:
        # Handle unexpected errors during parsing
        print(oops)
        return None


def _dewiki(text: str) -> str:
    """
    Convert raw wiki markup text into clean plain text.

    This function processes a given wiki markup string to remove excess formatting,
    HTML entities, and extraneous whitespace, while retaining readable plain text.
    It also ensures proper formatting around section titles.

    Args:
        text (str): The input string containing raw wiki markup.

    Returns:
        str: The processed plain text with wiki markup removed and formatting cleaned.
    """
    # Parse the wiki markup to extract plain text
    text = wtp.parse(text).plain_text()

    # Decode any HTML entities, such as &amp; -> &
    text = htt(text)

    # Replace newlines with spaces for better readability
    text = text.replace("\n", " ")

    # Replace multiple spaces or tabs with a single space
    text = re.sub(r"\s+", " ", text)

    # Add blank lines before and after section titles (levels 1 to 5)
    text = re.sub(r"(={1,5}\s*[^=]+\s*={1,5})", r"\n\n\1\n\n", text)

    return text


def _count_pages_in_file(filename: Union[str, Path]) -> int:
    """Count the total number of pages in a bzip2 compressed XML file.

    Args:
        filename: Path to the bzip2 compressed XML file

    Returns:
        Total number of pages found in the file
    """
    page_count = 0
    with bz2.open(filename, "rt", encoding="utf-8") as infile:
        for line in infile:
            if "<page>" in line:
                page_count += 1
    return page_count
