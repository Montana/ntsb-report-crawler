import os
import time
import requests
import argparse
import csv
from slugify import slugify
from pyquery import PyQuery as pq
from collections import OrderedDict
from datetime import datetime

BASE_URL = "https://data.ntsb.gov/"
DOCKET_URL_TEMPLATE = BASE_URL + "Docket/TOCPrintable?data={}"
SLEEP_TIME = 1

def create_output_directory(docket_id):
    if not os.path.exists(docket_id):
        os.mkdir(docket_id)
    return docket_id

def fetch_html(url):
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        response = requests.get(url, headers=headers)
        print(f"Fetching URL: {url}, Status Code: {response.status_code}")
        response.raise_for_status()
        return pq(response.content)
    except requests.RequestException as e:
        print(f"Error fetching URL: {url}\n{e}")
        return None

def parse_docket_page(html, docket_id):
    rows = html("tr")  # Assuming each row in the table is a <tr> element
    print(f"Found {len(rows)} records to process.")
    master_dict = {}
    
    for row in rows:
        columns = pq(row)("td")
        if len(columns) < 4:  # Adjust based on actual column count
            continue
        
        doc_no = pq(columns[0]).text().strip()
        doc_date = pq(columns[1]).text().strip()
        doc_title = pq(columns[2]).text().strip()
        doc_url = pq(columns[2])("a").attr("href")  # Get the document link
        doc_pages = pq(columns[3]).text().strip() or 0
        
        if doc_url:
            doc_url = BASE_URL + doc_url if not doc_url.startswith("http") else doc_url

        master_dict[doc_no] = {
            "doc_date": doc_date,
            "doc_title": doc_title,
            "doc_url": doc_url,
            "doc_pages": int(doc_pages),
        }
    return master_dict

def download_file(url, filename):
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(filename, "wb") as file:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    file.write(chunk)
        return True
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return False

def save_to_csv(master_dict, docket_id):
    csv_file = f"{docket_id}.csv"
    print(f"Saving data to {csv_file}")
    
    with open(csv_file, mode="w", newline="") as file:
        writer = csv.writer(file)
        headers = ["doc_no", "doc_date", "doc_title", "doc_url", "doc_pages"]
        writer.writerow(headers)
        for doc_no, data in master_dict.items():
            writer.writerow([doc_no, data["doc_date"], data["doc_title"], data["doc_url"], data["doc_pages"]])
    print(f"Data saved to {csv_file}")

def main():
    parser = argparse.ArgumentParser(description="Fetch National Transportation Safety Board (NTSB) docket files.")
    parser.add_argument("docket_id", help="Docket ID to fetch data for.")
    args = parser.parse_args()
    
    docket_id = args.docket_id
    output_dir = create_output_directory(docket_id)
    docket_url = DOCKET_URL_TEMPLATE.format(docket_id)
    
    print(f"Fetching docket data from {docket_url}")
    html = fetch_html(docket_url)
    if not html:
        print("Failed to fetch docket page.")
        return
    
    master_dict = parse_docket_page(html, docket_id)
    print(f"Found {len(master_dict)} files to process.")
    
    for doc_no, data in master_dict.items():
        filename = os.path.join(output_dir, f"{data['doc_date']}-{slugify(data['doc_title'])}.pdf")
        if os.path.exists(filename):
            print(f"{filename} already exists. Skipping download.")
            continue
        
        if data["doc_url"]:
            print(f"Downloading {data['doc_title']} to {filename}")
            success = download_file(data["doc_url"], filename)
            if not success:
                print(f"Failed to download {data['doc_title']}. Skipping.")
                continue
        else:
            print(f"No URL found for {data['doc_title']}. Skipping.")
        
        time.sleep(SLEEP_TIME)
    
    save_to_csv(master_dict, docket_id)

if __name__ == "__main__":
    main()
