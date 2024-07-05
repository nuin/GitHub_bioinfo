import requests
from xml.etree import ElementTree
import urllib.parse
import time
import logging
import sys
import re
import duckdb

# Set up logging
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler("pubmed_search.log"),
                        logging.StreamHandler(sys.stdout)
                    ])

def search_pubmed(query, retstart=0, retmax=10000):
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
        if count is not None:
            logging.info(f"Total count reported by PubMed: {count.text}")
        logging.info(f"Found {len(id_list)} PubMed IDs")
        return id_list
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
            articles.append({"pmid": pmid, "title": title, "abstract": abstract, "github_link": github_link})
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

def get_all_publications(query, batch_size=10000, fetch_size=100):
    all_pubmed_ids = []
    start_index = 0
    
    while True:
        pubmed_ids = search_pubmed(query, retstart=start_index, retmax=batch_size)
        all_pubmed_ids.extend(pubmed_ids)
        
        if len(pubmed_ids) < batch_size:
            break
        
        start_index += batch_size
        logging.info(f"Sleeping for 1 second to respect rate limits")
        time.sleep(1)  # Add delay to respect rate limits
    
    logging.info(f"Total PubMed IDs found: {len(all_pubmed_ids)}")
    
    all_publications = []
    for i in range(0, len(all_pubmed_ids), fetch_size):
        batch_pubmed_ids = all_pubmed_ids[i:i+fetch_size]
        publications = fetch_pubmed_details(batch_pubmed_ids)
        all_publications.extend(publications)
        logging.info(f"Sleeping for 1 second to respect rate limits")
        time.sleep(1)  # Add delay to respect rate limits
    
    return all_publications

# Search query
query = "github"

logging.info("Starting PubMed search and data retrieval")

# Get all publications with GitHub mention
publications = get_all_publications(query)

logging.info(f"Total publications retrieved: {len(publications)}")

# Initialize DuckDB connection
con = duckdb.connect('pubmed_results.db')

# Create table
con.execute("""
    CREATE TABLE IF NOT EXISTS publications (
        pmid VARCHAR,
        title VARCHAR,
        abstract VARCHAR,
        github_link VARCHAR
    )
""")

# Insert data into DuckDB if there are publications
if publications:
    con.executemany("""
        INSERT INTO publications (pmid, title, abstract, github_link)
        VALUES (?, ?, ?, ?)
    """, [(p['pmid'], p['title'], p['abstract'], p['github_link']) for p in publications])
    logging.info("Results saved to DuckDB database: pubmed_results.db")
else:
    logging.warning("No publications found to insert into the database.")

# Print summary
result = con.execute("SELECT COUNT(*) as total, COUNT(github_link) as with_github FROM publications")
total, with_github = result.fetchone()
logging.info(f"Total publications in database: {total}")
logging.info(f"Publications with GitHub links: {with_github}")

# Close the connection
con.close()

logging.info("Search and retrieval process completed")