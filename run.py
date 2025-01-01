from pathlib import Path

import yaml
from dotenv import load_dotenv

from src.download_wiki_file import download_simplewiki_dump
from src.format_wiki_text import format_wiki_text

# Load environment variables (e.g., API keys)
load_dotenv()


def run():
    # Load paths from the YAML file
    config_path = Path(__file__).parent / "config.yaml"
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)

    raw_folder = config["raw_folder"]
    processed_folder = config["processed_folder"]
    output_file = Path(processed_folder) / config["output_file"]

    # Download the dump file and get the metadata
    print("Starting download of the Simple Wikipedia dump...")
    file_path, dump_date, metadata_file = download_simplewiki_dump(raw_folder)

    print(f"Download complete! File saved to {file_path}")
    print(f"Dump date: {dump_date}")
    print(f"Metadata saved in: {metadata_file}")

    # Step 2: Process the downloaded file
    print(f"Processing the dump file {file_path}...")
    format_wiki_text(file_path, output_file)
    print(f"Processing complete! Processed file saved as {output_file}")

    # Step 3: Iteratively transform articles' text into Markdown
    # and insert it into a SQLite DB


# Main block to run the script
if __name__ == "__main__":
    run()
