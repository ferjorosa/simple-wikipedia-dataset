import bz2
import json
import re
from pathlib import Path
from threading import Thread
from time import sleep
from typing import Dict, Optional, Union

import wikitextparser as wtp
from html2text import html2text as htt
from tqdm import tqdm  # for progress tracking


def dewiki(text: str) -> str:
    """Convert wiki markup to plain text.

    Args:
        text: Raw wiki markup text

    Returns:
        Clean plain text with markup and excess whitespace removed
    """
    text = wtp.parse(text).plain_text()  # wiki to plaintext
    text = htt(text)  # remove any HTML
    text = text.replace("\\n", " ")  # replace newlines
    text = re.sub(r"\s+", " ", text)  # replace excess whitespace
    return text


def analyze_chunk(text: str) -> Optional[Dict[str, str]]:
    """Analyze a Wikipedia article chunk and extract relevant information.

    Args:
        text: Raw XML chunk containing a Wikipedia article

    Returns:
        Dictionary containing article title, content and ID if valid,
        None if article should be skipped
    """
    try:
        if '<redirect title="' in text:  # this is not the main article
            return None
        if "(disambiguation)" in text:  # this is not an article
            return None
        else:
            title = text.split("<title>")[1].split("</title>")[0]
            title = htt(title)
            if (
                ":" in title
            ):  # most articles with : in them are not articles we care about
                return None
        serial = text.split("<id>")[1].split("</id>")[0]
        content = text.split("</text")[0].split("<text")[1].split(">", maxsplit=1)[1]
        content = dewiki(content)
        return {"title": title.strip(), "text": content.strip(), "id": serial.strip()}
    except Exception as oops:
        print(oops)
        return None


def save_article(article: str, savedir: Path) -> None:
    """Save a processed article to a JSON file.

    Args:
        article: Raw XML chunk containing article content
        savedir: Directory path where the JSON file should be saved
    """
    doc = analyze_chunk(article)
    if doc:
        filename = savedir / f"{doc['id']}.json"
        with open(filename, "w", encoding="utf-8") as outfile:
            json.dump(doc, outfile, sort_keys=True, indent=1, ensure_ascii=False)


def count_pages_in_file(filename: Union[str, Path]) -> int:
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


def process_file_text(filename: Union[str, Path], savedir: Union[str, Path]) -> None:
    """Process a Wikipedia XML dump file and save articles as individual JSON files.

    Args:
        filename: Path to the bzip2 compressed XML dump file
        savedir: Directory path where processed articles should be saved
    """
    # Convert to Path objects
    filename = Path(filename)
    savedir = Path(savedir)

    # Ensure save directory exists
    savedir.mkdir(parents=True, exist_ok=True)

    # Count total number of articles (pages) in the file
    total_pages = count_pages_in_file(filename)
    print(f"Total unique articles to process: {total_pages}")
    sleep(0.1)  # Allow print to be shown before tqdm

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
                    Thread(target=save_article, args=(article, savedir)).start()
                    pbar.update(1)  # Update progress bar for each processed article
                else:
                    article += line
