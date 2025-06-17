import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urldefrag
import json
import time

# Base settings
BASE_URL = "https://developers.facebook.com"
START_PATH = "/docs/whatsapp/cloud-api"

# Data structures
to_visit = [urljoin(BASE_URL, START_PATH)]
visited = set()
pages = []

# HTTP headers
def default_headers():
    return {
        'User-Agent': 'Mozilla/5.0 (compatible; scraper/1.0)'
    }

while to_visit:
    url = to_visit.pop(0)
    if url in visited:
        continue
    visited.add(url)
    print(f"Fetching {url}...")
    try:
        resp = requests.get(url, headers=default_headers())
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        continue
    if resp.status_code != 200:
        print(f"Non-OK status {resp.status_code} at {url}")
        continue

    # Parse HTML
    soup = BeautifulSoup(resp.text, 'html.parser')

    # Extract title
    title_tag = soup.find(['h1', 'h2'])
    title = title_tag.get_text(strip=True) if title_tag else ''

    # Extract main content from the documentation pagelet or fallback containers
    content_container = (
        soup.find('div', id='documentation_body_pagelet') or
        soup.find('div', class_='docs-article-content') or
        soup.find('article')
    )
    content = content_container.get_text(separator='\n', strip=True) if content_container else ''

    pages.append({
        'url': url,
        'title': title,
        'content': content
    })

    # Discover additional doc links
    for a in soup.find_all('a', href=True):
        href = a['href']
        full = urljoin(BASE_URL, href)
        if full.startswith(urljoin(BASE_URL, START_PATH)):
            clean = urldefrag(full)[0]
            if clean not in visited and clean not in to_visit:
                to_visit.append(clean)

    # Be polite
    time.sleep(1)

# Serialize to JSON
output_file = '../../../RedBox Storage/RedBox Storage Team Site - Documents/Revenue/Data/DB/Api Docs/whatsapp_cloud_api_docs.json'
with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(pages, f, ensure_ascii=False, indent=2)

print(f"Scraped {len(pages)} pages. Saved to {output_file}.")