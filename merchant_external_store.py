import requests
import json
import time
import os

# Base URL of the API
base_url = "https://api.dotpe.in/api/merchant/external/store"

# File to save results and store_id checkpoint
output_file = "dotpe_api_results.json"
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
        # 429 Too Many Requests (Rate Limit Exceeded)
        print(f"Rate limit hit. Waiting to retry storeID: {store_id}")
        return "rate_limit"
    else:
        return None

# Function to save the results and checkpoint
def save_results(results, current_store_id):
    # Save the results to the output JSON file
    with open(output_file, "w") as f:
        json.dump(results, f, indent=4)

    # Save the current store_id to the checkpoint file
    with open(checkpoint_file, "w") as f:
        f.write(str(current_store_id))

# Function to load the last checkpoint
def load_checkpoint():
    # Check if checkpoint file exists
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, "r") as f:
            return int(f.read().strip())
    else:
        return 1  # Default to store_id = 1 if no checkpoint

# Load the last store_id from the checkpoint
store_id = load_checkpoint()

# Initialize results list
results = []

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

    # Append the result to the list
    results.append({
        "store_id": store_id,
        "data": result
    })

    # Save the results and the current store_id
    save_results(results, store_id)

    # Increment the store_id for the next request
    store_id += 1

    # Add a small delay to avoid hitting rate limits
    time.sleep(1)

# Save final results before exiting
save_results(results, store_id)
print(f"Results saved to {output_file} and checkpoint saved to {checkpoint_file}")
