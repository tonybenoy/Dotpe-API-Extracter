import requests
import json
import time
import os
import sqlite3

# Base URL of the API
base_url = "https://api.dotpe.in/api/merchant/external/store"

# File to save results and store_id checkpoint
db_file = "dotpe_api_results.db"
checkpoint_file = "checkpoint.txt"

# Headers to avoid rate limit as much as possible
headers = {
    "Host": "api.dotpe.in",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:130.0) Gecko/20100101 Firefox/130.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/png,image/svg+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "cross-site",
    "DNT": "1",
    "Sec-GPC": "1",
    "Priority": "u=0, i",
    "TE": "trailers"
}

# Function to make the API request
def make_api_request(store_id):
    url = f"{base_url}/{store_id}?serviceSubtype=fine"
    response = requests.get(url, headers=headers)

    # Log request details
    print(f"Requesting storeID: {store_id}, Status Code: {response.status_code}")

    if response.status_code == 200:
        return response.json()
    elif response.status_code == 403:
        # 403 Forbidden, could indicate rate limit (based on your API behavior)
        print(f"Rate limit hit. Waiting to retry storeID: {store_id}")
        return "rate_limit"
    else:
        return None

# Function to initialize SQLite DB and table
def init_db():
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS store_data (
            store_id INTEGER PRIMARY KEY,
            data TEXT
        )
    ''')
    conn.commit()
    conn.close()

# Function to save the results to SQLite DB
def save_results(store_id, result):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Insert store_id and the JSON result (stored as text)
    cursor.execute('''
        INSERT INTO store_data (store_id, data)
        VALUES (?, ?)
    ''', (store_id, json.dumps(result)))

    conn.commit()
    conn.close()

# Function to load the last checkpoint from the DB or checkpoint file
def load_checkpoint():
    # First check if the SQLite database exists and contains data
    if os.path.exists(db_file):
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # Query for the maximum store_id from the database
        cursor.execute('SELECT MAX(store_id) FROM store_data')
        result = cursor.fetchone()
        conn.close()

        # If the database has data, return the next store_id
        if result and result[0] is not None:
            return result[0] + 1

    # If no data in DB, or DB doesn't exist, check the checkpoint file
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, "r") as f:
            try:
                return int(f.read().strip())
            except ValueError:
                print("Invalid checkpoint file. Starting from store_id = 1.")

    # Default to store_id = 1 if no data is found in DB or checkpoint file
    return 1

# Function to save the checkpoint in case DB isn't available
def save_checkpoint(store_id):
    with open(checkpoint_file, "w") as f:
        f.write(str(store_id))

# Load the last store_id from the checkpoint (DB or file)
store_id = load_checkpoint()

# Initialize the database and table
init_db()


# Loop through store IDs and handle rate limiting with backoff
backoff_time = 5  # Initial backoff time in seconds

while True:
    result = make_api_request(store_id)

    if result == "rate_limit":
        # If rate limit hit, use exponential backoff and retry
        time.sleep(backoff_time)
        backoff_time *= 2  # Exponential increase of wait time
        continue
    elif result is None:
        # Stop if we get a non-200 response
        print("Non-200 status code received. Stopping the loop.")
        break

    # Reset backoff time after a successful request
    backoff_time = 5

    # Save the results to the SQLite DB or save a checkpoint if DB not available
    try:
        save_results(store_id, result)
    except Exception as e:
        print(f"Failed to save results to DB: {e}. Saving checkpoint instead.")

    # Continuously update the checkpoint after each request
    save_checkpoint(store_id)

    # Increment the store_id for the next request
    store_id += 1

    # Add a small delay to avoid hitting rate limits
    time.sleep(1)

# Save final checkpoint before exiting
save_checkpoint(store_id)
print(f"Results saved to SQLite DB: {db_file} and checkpoint saved to {checkpoint_file}")
