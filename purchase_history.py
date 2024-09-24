import requests
import json
import time
import os

# File from which to read the results
input_file = "dotpe_api_results.json"

# File to save processed results
output_file = "purchase_history_results.json"

# Checkpoint file to track progress
checkpoint_file = "purchase_history_checkpoint.txt"

# Headers for API requests
headers = {
    "Host": "api.dotpe.in",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:130.0) Gecko/20100101 Firefox/130.0",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive"
}

# Base URL for the purchase history API
purchase_history_base_url = "https://api.dotpe.in/api/morder/suggestion/items/purchase/history"

# Function to call the purchase history API with rate limit handling
def get_purchase_history(merchantID, storeID):
    url = f"{purchase_history_base_url}?merchantID={merchantID}&storeID={storeID}"
    response = requests.get(url, headers=headers)

    print(f"Requesting purchase history for storeID: {storeID}, merchantID: {merchantID}, Status Code: {response.status_code}")

    if response.status_code == 200:
        return response.json()
    elif response.status_code == 403:  # Assume 403 means rate limit hit
        print(f"Rate limit hit for storeID: {storeID}, merchantID: {merchantID}")
        return "rate_limit"
    else:
        print(f"Non-success status code {response.status_code} for storeID: {storeID}")
        return None

# Function to load existing purchase history data
def load_existing_results():
    if os.path.exists(output_file):
        with open(output_file, "r") as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return []
    return []

# Function to append new results to the purchase history JSON file
def append_results(results):
    # Load existing data
    existing_results = load_existing_results()

    # Append new results to the existing data
    existing_results.extend(results)

    # Save the combined results to the output file
    with open(output_file, "w") as f:
        json.dump(existing_results, f, indent=4)

# Function to load the store data from the input JSON file
def load_store_data():
    if os.path.exists(input_file):
        with open(input_file, "r") as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return []
    return []

# Function to save the current checkpoint (store_id)
def save_checkpoint(store_id):
    with open(checkpoint_file, "w") as f:
        f.write(str(store_id))

# Function to load the checkpoint
def load_checkpoint():
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, "r") as f:
            return int(f.read().strip())
    return 0  # Start from the beginning if no checkpoint exists

# Load store data from the input file
store_data = load_store_data()

# Initialize results list for the purchase history API responses
results = []

# Initialize backoff time for rate limiting
backoff_time = 5  # Start with 5 seconds

# Load the checkpoint (last processed store_id)
checkpoint = load_checkpoint()

# Start processing stores from the checkpoint onward
for store_entry in store_data[checkpoint:]:
    try:
        store_id = store_entry["store_id"]
        merchant_id = store_entry["data"]["store"]["merchantID"]

        # Call the purchase history API for the current store
        while True:
            purchase_history = get_purchase_history(merchant_id, store_id)

            if purchase_history == "rate_limit":
                # If rate limit is hit, apply exponential backoff
                print(f"Rate limit reached. Retrying after {backoff_time} seconds.")
                time.sleep(backoff_time)
                backoff_time *= 2  # Exponential backoff
            elif purchase_history is not None:
                # Successfully retrieved data
                results.append({
                    "store_id": store_id,
                    "merchant_id": merchant_id,
                    "purchase_history": purchase_history
                })
                break  # Exit the loop once successful
            else:
                # For non-200 and non-rate-limit responses, log and skip
                print(f"Skipping storeID: {store_id} due to non-successful response.")
                break

        # Reset backoff time after successful request
        backoff_time = 5

        # Save the results after each API call to avoid data loss
        append_results(results)

        # Clear the results list after saving to avoid storing the same results in memory
        results.clear()

        # Save the current store_id as a checkpoint
        save_checkpoint(checkpoint + store_data.index(store_entry) + 1)

        # Add a small delay between requests to avoid hitting rate limits
        time.sleep(1)

    except KeyError as e:
        # Skip the current entry if the required keys are missing
        print(f"Skipping store entry due to missing key: {e}")
        continue

print(f"Purchase history results saved to {output_file}")
