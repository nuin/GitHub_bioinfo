import requests
from xml.etree import ElementTree
import urllib.parse
import time
import logging
import sys
import re
import duckdb
import os

# Set up logging
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler("pubmed_search.log"),
                        logging.StreamHandler(sys.stdout)
                    ])

def search_pubmed(query, retstart=0, retmax=100):
    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
    encoded_query = urllib.parse.quote(query)
    search_url = f"{base_url}esearch.fcgi?db=pubmed&term={encoded_query}&retstart={retstart}&retmax={retmax}&retmode=xml"

    logging.info(f"Searching PubMed with URL: {search_url}")
    response = requests.get(search_url)
    logging.debug(f"Response status code: {response.status_code}")
    logging.debug(f"Response content: {response.text[:500]}...")  # Print first 500 characters of response

    if response.status_code == 200:
        root = ElementTree.fromstring(response.content)
        id_list = [id_element.text for id_element in root.findall("./IdList/Id")]
        count = root.find("Count")
        total_count = int(count.text) if count is not None else None
        logging.info(f"Found {len(id_list)} PubMed IDs in this batch")
        return id_list, total_count
    else:
        logging.error(f"Search failed with status code {response.status_code}")
        raise Exception(f"Search failed with status code {response.status_code}")

def fetch_pubmed_details(pubmed_ids):
    if not pubmed_ids:
        return []
    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
    fetch_url = f"{base_url}efetch.fcgi?db=pubmed&id={','.join(pubmed_ids)}&retmode=xml"

    logging.info(f"Fetching details for {len(pubmed_ids)} PubMed IDs")
    response = requests.get(fetch_url)
    if response.status_code == 200:
        root = ElementTree.fromstring(response.content)
        articles = []
        for article in root.findall("./PubmedArticle"):
            pmid = article.find(".//PMID").text
            title = article.find(".//ArticleTitle").text
            abstract_element = article.find(".//Abstract/AbstractText")
            abstract = abstract_element.text if abstract_element is not None else None
            github_link = find_github_link(article)
            articles.append({
                "pmid": pmid,
                "title": title,
                "abstract": abstract,
                "github_link": github_link,
                "has_github_link": github_link is not None
            })
        logging.info(f"Fetched details for {len(articles)} articles")
        return articles
    else:
        logging.error(f"Fetch failed with status code {response.status_code}")
        raise Exception(f"Fetch failed with status code {response.status_code}")

def find_github_link(article):
    github_regex = r'https?://(?:www\.)?github\.com/[^\s/]+/[^\s/]+'
    for element in article.iter():
        if element.text:
            match = re.search(github_regex, element.text)
            if match:
                return match.group(0)
    return None

def get_all_publications(query, batch_size=100, fetch_size=100):
    all_pubmed_ids = []
    start_index = 0
    total_count = None

    while True:
        pubmed_ids, batch_total_count = search_pubmed(query, retstart=start_index, retmax=batch_size)

        if total_count is None:
            total_count = batch_total_count
            logging.info(f"Total count of publications: {total_count}")

        all_pubmed_ids.extend(pubmed_ids)

        if len(all_pubmed_ids) >= total_count or len(pubmed_ids) < batch_size:
            break

        start_index += batch_size
        logging.info(f"Retrieved {len(all_pubmed_ids)} out of {total_count} PubMed IDs")
        logging.info(f"Sleeping for 1 second to respect rate limits")
        time.sleep(1)  # Add delay to respect rate limits

    logging.info(f"Total PubMed IDs found: {len(all_pubmed_ids)}")

    all_publications = []
    for i in range(0, len(all_pubmed_ids), fetch_size):
        batch_pubmed_ids = all_pubmed_ids[i:i+fetch_size]
        publications = fetch_pubmed_details(batch_pubmed_ids)
        all_publications.extend(publications)
        logging.info(f"Retrieved details for {len(all_publications)} out of {len(all_pubmed_ids)} publications")
        logging.info(f"Sleeping for 1 second to respect rate limits")
        time.sleep(1)  # Add delay to respect rate limits

    return all_publications

# Search query
query = "github"

logging.info("Starting PubMed search and data retrieval")

# Get all publications with GitHub mention
publications = get_all_publications(query)

logging.info(f"Total publications retrieved: {len(publications)}")

# Clear existing database if it exists
db_name = 'pubmed_results.db'
if os.path.exists(db_name):
    os.remove(db_name)
    logging.info(f"Removed existing database: {db_name}")

# Initialize DuckDB connection
con = duckdb.connect(db_name)

# Create table with the new has_github_link column
con.execute("""
    CREATE TABLE IF NOT EXISTS publications (
        pmid VARCHAR,
        title VARCHAR,
        abstract VARCHAR,
        github_link VARCHAR,
        has_github_link BOOLEAN
    )
""")

# Insert data into DuckDB if there are publications
if publications:
    con.executemany("""
        INSERT INTO publications (pmid, title, abstract, github_link, has_github_link)
        VALUES (?, ?, ?, ?, ?)
    """, [(p['pmid'], p['title'], p['abstract'], p['github_link'], p['has_github_link']) for p in publications])
    logging.info(f"Results saved to DuckDB database: {db_name}")
else:
    logging.warning("No publications found to insert into the database.")

# Print summary
result = con.execute("""
    SELECT 
        COUNT(*) as total,
        SUM(CASE WHEN has_github_link THEN 1 ELSE 0 END) as with_github,
        SUM(CASE WHEN NOT has_github_link THEN 1 ELSE 0 END) as without_github
    FROM publications
""")
total, with_github, without_github = result.fetchone()
logging.info(f"Total publications in database: {total}")
logging.info(f"Publications with GitHub links: {with_github}")
logging.info(f"Publications without GitHub links: {without_github}")

# Close the connection
con.close()

logging.info("Search and retrieval process completed")