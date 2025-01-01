import sqlite3


def initialize_db(db_path="output.db"):
    """
    Initializes the SQLite database and creates the table if it doesn't exist.

    Args:
        db_path (str): Path to the SQLite database file. Default is 'output.db'.
    """
    # Connect to the SQLite database (or create it if it doesn't exist)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create the table if it doesn't exist
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS articles (
            id INTEGER PRIMARY KEY,
            title TEXT,
            text TEXT,
            markdown_text TEXT
        )
        """
    )

    # Commit the changes and close the connection
    conn.commit()
    conn.close()
    print(f"Database initialized successfully at {db_path}")


def filter_rows_not_in_db(df, db_path, id_column="id"):
    """
    Filters rows of a DataFrame to include only those whose IDs are not
    present in the database.

    Args:
        df (pd.DataFrame): The DataFrame to filter.
        db_path (str): Path to the SQLite database file.
        id_column (str): The name of the column in the DataFrame that contains
                         the IDs. Default is 'id'.

    Returns:
        pd.DataFrame: A filtered DataFrame containing only rows whose IDs are
                      not in the database.
    """
    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get the list of IDs from the DataFrame
    ids = df[id_column].tolist()

    # Query the database to find which IDs already exist
    placeholders = ",".join(["?"] * len(ids))  # Create placeholders for query
    query = f"SELECT id FROM articles WHERE id IN ({placeholders})"
    cursor.execute(query, ids)

    # Fetch the existing IDs from the database
    existing_ids = [row[0] for row in cursor.fetchall()]

    # Close the database connection
    conn.close()

    # Filter the DataFrame to include only rows whose IDs are not in database
    filtered_df = df[~df[id_column].isin(existing_ids)]

    return filtered_df
