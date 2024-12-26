import json
from datetime import datetime
from pathlib import Path
from typing import Tuple

import requests
from tqdm import tqdm


def download_simplewiki_dump(destination_folder: str) -> Tuple[str, str, str]:
    """
    Downloads the latest dump from Simple Wikipedia and retrieves the
    associated date.

    Args:
        destination_folder (str): The folder where the file will be saved.

    Returns:
        Tuple[str, str, str]:
            - Path to the downloaded file.
            - The date associated with the dump file.
            - Path to the metadata file.
    """
    destination_folder = Path(destination_folder)

    url = (
        "https://dumps.wikimedia.org/simplewiki/latest/"
        "simplewiki-latest-pages-articles-multistream.xml.bz2"
    )

    response = requests.head(url)
    if response.status_code != 200:
        raise Exception(f"Failed to access URL. Status code: {response.status_code}")

    last_modified = response.headers.get("Last-Modified")
    if not last_modified:
        raise Exception("Could not retrieve the last modified date from headers.")

    dump_date = datetime.strptime(last_modified, "%a, %d %b %Y %H:%M:%S %Z")
    formatted_date = dump_date.strftime("%Y-%m-%d")

    response = requests.get(url, stream=True)
    if response.status_code != 200:
        raise Exception(f"Failed to download file. Status code: {response.status_code}")

    destination_folder.mkdir(parents=True, exist_ok=True)

    file_name = url.split("/")[-1]
    output_file = destination_folder / file_name

    total_size = int(response.headers.get("Content-Length", 0))

    with open(output_file, "wb") as file:
        with tqdm(total=total_size, unit="B", unit_scale=True, desc=file_name) as pbar:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    file.write(chunk)
                    pbar.update(len(chunk))

    metadata = {
        "file_name": file_name,
        "download_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "last_modified_date": formatted_date,
        "file_size": total_size,
        "downloaded_file_path": str(output_file),
    }

    metadata_file = destination_folder / "download_metadata.json"
    with open(metadata_file, "w") as f:
        json.dump(metadata, f, indent=4)

    return str(output_file), formatted_date, str(metadata_file)


if __name__ == "__main__":
    destination = "./downloads"
    file_path, dump_date, metadata_file = download_simplewiki_dump(destination)
    print(f"Downloaded file: {file_path}")
    print(f"Dump date: {dump_date}")
    print(f"Metadata saved in: {metadata_file}")
