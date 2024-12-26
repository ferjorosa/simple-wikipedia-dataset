from src.download_file import download_simplewiki_dump
from src.process_file import process_file_text


def run():
    # Step 1: Download the Simple Wikipedia dump
    raw_folder = "dump/raw"
    processed_folder = "dump/processed/"

    # Download the dump file and get the metadata
    print("Starting download of the Simple Wikipedia dump...")
    file_path, dump_date, metadata_file = download_simplewiki_dump(raw_folder)

    print(f"Download complete! File saved to {file_path}")
    print(f"Dump date: {dump_date}")
    print(f"Metadata saved in: {metadata_file}")

    # Step 2: Process the downloaded file
    print(f"Processing the dump file {file_path}...")
    process_file_text(file_path, processed_folder)
    print(f"Processing complete! Processed files saved in {processed_folder}")


# Main block to run the script
if __name__ == "__main__":
    run()
