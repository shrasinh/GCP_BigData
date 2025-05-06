import os
import requests
from bs4 import BeautifulSoup

# GitHub repository URL
repo_url = "https://github.com/ShabbirHasan1/NSE-Data/tree/main/NSE%20Minute%20Data/NSE_Stocks_Data"

# Fetch the page content
response = requests.get(repo_url)
soup = BeautifulSoup(response.content, "html.parser")

# Find all the file links
file_links = []
for link in soup.find_all("a"):
    href = link.get("href")
    if href and href.endswith(".csv"):
        file_links.append(
            f"https://raw.githubusercontent.com{href.replace('/blob', '')}"
        )

for file_link in file_links:
    try:
        # Download the CSV file
        filename = os.path.basename(file_link)
        response = requests.get(file_link)
        with open(filename, "wb") as f:
            f.write(response.content)

    except Exception as e:
        print(f"Error processing file {filename}: {e}")
