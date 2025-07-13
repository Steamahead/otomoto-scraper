import os
import csv
import time
import re
import hashlib
import logging
import requests
from dataclasses import dataclass, asdict
from datetime import datetime
from difflib import SequenceMatcher
from typing import List, Tuple, Dict

from bs4 import BeautifulSoup
import tempfile

# ---------------------------
# Constants & Data Structure Definition
# ---------------------------
BASE_URL = ("https://www.otomoto.pl/osobowe/ds-automobiles/ds-7-crossback?"
            "search[advanced_search_expanded]=true")
EXPECTED_PER_PAGE = 32
MAX_PAGES_TO_CHECK = 20
DEBUG_MODE = True
REQUIRED_PREFIX = "https://www.otomoto.pl/osobowe/oferta/ds-automobiles-ds-7-crossback"

CANDIDATE_VERSIONS = [
    "Elegance", "Performance Line", "Prestige", "Ultra Prestige", "Louvre",
    "Opera", "Rivoli", "Grand Chic", "Bastille", "Pallas", "Etoile", "La Premiere",
    "Esprit de Voyage", "So Chic", "Be Chic", "Edition France"
]

@dataclass
class Car:
    auction_id: str
    link: str
    full_name: str
    description: str
    year: int
    mileage_km: int
    engine_capacity: int
    engine_power: str
    fuel_type: str
    price_pln: int
    seller_type: str
    city: str
    voivodship: str
    scrape_date: str
    scrape_time: str
    listing_status: str
    version: str

# ---------------------------
# Database Functions (No changes here)
# ---------------------------
def get_sql_connection():
    try:
        import pymssql
        server = os.environ.get('DB_SERVER')
        database = os.environ.get('DB_NAME')
        username = os.environ.get('DB_UID')
        password = os.environ.get('DB_PWD')
        logging.info(f"Connecting to SQL server with SQL auth: {server}/{database} as {username}")
        connection = pymssql.connect(server=server, user=username, password=password, database=database, timeout=30, appname="AzureFunctionsApp")
        logging.info("SQL connection successful")
        return connection
    except Exception as e:
        logging.error(f"SQL connection error: {str(e)}")
        logging.error(traceback.format_exc())
        return None

def compute_auction_key(url: str) -> str:
    return hashlib.md5(url.encode('utf-8')).hexdigest()

def get_auction_number(auction_key: str) -> int:
    connection = None
    try:
        connection = get_sql_connection()
        if not connection: return 1000000
        cursor = connection.cursor()
        query = "SELECT TOP 1 AuctionNumber FROM Listings WHERE AuctionKey = %s ORDER BY CreatedDate DESC"
        cursor.execute(query, (auction_key,))
        row = cursor.fetchone()
        if row:
            return row[0]
        else:
            max_query = "SELECT ISNULL(MAX(AuctionNumber), 0) FROM Listings"
            cursor.execute(max_query)
            max_val = cursor.fetchone()[0]
            return int(max_val) + 1
    except Exception as e:
        logging.error(f"Error in get_auction_number: {str(e)}")
        return 1000000
    finally:
        if connection: connection.close()

def insert_into_db(car: Car) -> int:
    connection = None
    cursor = None
    try:
        connection = get_sql_connection()
        if not connection: return None
        cursor = connection.cursor()
        auction_key = compute_auction_key(car.link)
        auction_number = get_auction_number(auction_key)
        insert_query = """
           INSERT INTO Listings (
               ListingURL, AuctionKey, AuctionNumber, FullName, Description, Year, Mileage, EngineCapacity,
               FuelType, City, Voivodship, SellerType, ScrapeDate, ScrapeDateTime, ListingStatus, Version, Price
           ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
           SELECT SCOPE_IDENTITY();
           """
        params = (
            car.link, auction_key, auction_number, car.full_name, car.description, car.year,
            car.mileage_km, car.engine_capacity, car.fuel_type, car.city, car.voivodship,
            car.seller_type, car.scrape_date, car.scrape_time, car.listing_status, car.version, car.price_pln
        )
        cursor.execute(insert_query, params)
        listing_id = cursor.fetchone()[0]
        connection.commit()
        return listing_id
    except Exception as e:
        if connection: connection.rollback()
        logging.error(f"Error inserting car {car.full_name}: {str(e)}")
        return None
    finally:
        if cursor: cursor.close()
        if connection: connection.close()

# ---------------------------
# Utility Functions
# ---------------------------
def basic_url_cleanup(url: str) -> str:
    url = url.strip()
    return 'https://www.otomoto.pl' + url if url.startswith('/') else url

def extract_version(full_name: str, description: str) -> str:
    # This logic remains the same
    for cand in CANDIDATE_VERSIONS:
        if cand.lower() in full_name.lower() or cand.lower() in description.lower():
            return cand
    return ""

def parse_location(location_str: str) -> Tuple[str, str]:
    # Regex to handle formats like "Street - 73-108 Morzyczyn, stargardzki, Zachodniopomorskie (Polska)"
    postal_match = re.search(r'\d{2}-\d{3}\s+([^,]+),.*,\s*([^,]+?)\s*\((Polska|)', location_str)
    if postal_match:
        city = postal_match.group(1).strip()
        voivodship = postal_match.group(2).strip().split(',')[-1].strip()
        return city, voivodship

    # Fallback for formats like "Zator, oświęcimski, Małopolskie" or "Wrocław, Krzyki"
    parts = [part.strip() for part in location_str.replace("(Polska)", "").split(',')]
    if len(parts) >= 2:
        return parts[0], parts[-1]
    
    return parts[0] if parts else "", ""

# ---------------------------
# Web Scraping Functions
# ---------------------------
def get_page_html(url: str) -> str:
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(url, headers=headers, timeout=20)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        logging.error(f"Error fetching URL {url}: {e}")
        return ""

def get_total_auction_count_and_pages(html: str) -> Tuple[int, int]:
    try:
        soup = BeautifulSoup(html, "html.parser")
        h1_tag = soup.find("h1")
        if h1_tag:
            match = re.search(r'(\d+)\s+ogłoszeń', h1_tag.get_text())
            if match:
                total_auctions = int(match.group(1))
                total_pages = (total_auctions + EXPECTED_PER_PAGE - 1) // EXPECTED_PER_PAGE
                return total_auctions, total_pages
    except Exception as e:
        logging.error(f"Error getting auction counts: {e}")
    return 320, 10

def extract_cars_from_html(html: str) -> List[Car]:
    cars: List[Car] = []
    soup = BeautifulSoup(html, "html.parser")
    container = soup.find("div", {"data-testid": "search-results"})
    if not container:
        logging.error("Search results container not found!")
        return cars

    listings = container.find_all("article", attrs={"data-id": True})
    
    for listing in listings:
        try:
            h2_tag = listing.find("h2")
            if not h2_tag: continue
            a_tag = h2_tag.find("a", href=True)
            if not a_tag: continue

            cleaned_link = basic_url_cleanup(a_tag["href"])
            if not cleaned_link.startswith(REQUIRED_PREFIX): continue

            full_name = a_tag.get_text(strip=True)
            
            # --- Extract data from the LISTING item first ---
            params = {li.find('p').text: li.find_all('p')[1].text for li in listing.find_all('li') if li.find('p')}
            
            year = int(params.get("Rok produkcji", 0))
            mileage_km = int(re.sub(r'\D', '', params.get("Przebieg", "0")))
            engine_capacity = int(re.sub(r'\D', '', params.get("Poj. skokowa", "0")))
            fuel_type = params.get("Rodzaj paliwa", "")

            price_pln = 0
            price_elem = listing.find("h3")
            if price_elem:
                price_pln = int(re.sub(r'\D', '', price_elem.get_text(strip=True)))

            # --- Fetch detail page ONLY for missing info ---
            detail_soup = None
            def get_detail_soup():
                nonlocal detail_soup
                if detail_soup is None:
                    detail_html = get_page_html(cleaned_link)
                    detail_soup = BeautifulSoup(detail_html, 'html.parser') if detail_html else BeautifulSoup("", 'html.parser')
                return detail_soup

            # Description
            desc_soup = get_detail_soup()
            description = ""
            desc_tag = desc_soup.find('p', class_='e1afgq2j0 ooa-w3crlp')
            if desc_tag:
                parts = desc_tag.get_text(strip=True).split('•')
                if len(parts) > 1:
                    description = parts[-1].strip()

            # Location
            loc_soup = get_detail_soup()
            city, voivodship = "", ""
            location_tag = loc_soup.find('p', class_='ef0vquw1 ooa-1frho3b')
            if location_tag:
                city, voivodship = parse_location(location_tag.get_text(strip=True))

            # Seller Type
            seller_soup = get_detail_soup()
            seller_type = ""
            seller_tags = seller_soup.find_all('p', class_='ooa-1hl3hwd')
            for tag in seller_tags:
                if tag.text in ["Firma", "Osoba prywatna", "Autoryzowany Dealer"]:
                    seller_type = tag.text
                    break

            now = datetime.now()
            car = Car(
                auction_id="", link=cleaned_link, full_name=full_name, description=description,
                year=year, mileage_km=mileage_km, engine_capacity=engine_capacity, engine_power="",
                fuel_type=fuel_type, price_pln=price_pln, seller_type=seller_type, city=city,
                voivodship=voivodship, scrape_date=now.strftime("%Y-%m-%d"),
                scrape_time=now.strftime("%H:%M:%S"), listing_status="Active",
                version=extract_version(full_name, description)
            )
            cars.append(car)
        except Exception as e:
            logging.error(f"Error parsing listing: {e}", exc_info=True)

    return cars

# ---------------------------
# Main Scraper Function
# ---------------------------
def run_scraper():
    logging.info(f"Scraper starting at {datetime.now()}")
    all_cars = []
    
    try:
        html = get_page_html(BASE_URL)
        if not html:
            logging.error("Failed to fetch the main page, aborting.")
            return

        total_auctions, total_pages = get_total_auction_count_and_pages(html)
        pages_to_check = min(total_pages, MAX_PAGES_TO_CHECK)
        logging.info(f"Found {total_auctions} auctions across {total_pages} pages. Will check {pages_to_check} pages.")

        for current_page in range(1, pages_to_check + 1):
            logging.info(f"Fetching page {current_page}/{pages_to_check}")
            page_url = f"{BASE_URL}&page={current_page}"
            
            # Use the already fetched HTML for the first page
            page_html = html if current_page == 1 else get_page_html(page_url)
            
            if not page_html:
                logging.warning(f"Failed to fetch page {current_page}, skipping.")
                continue

            cars_on_page = extract_cars_from_html(page_html)
            if not cars_on_page:
                logging.info(f"No cars found on page {current_page}. Stopping.")
                break
            
            all_cars.extend(cars_on_page)
            logging.info(f"Found {len(cars_on_page)} cars on page {current_page}. Total collected: {len(all_cars)}")
            time.sleep(0.5) # A small delay to be polite to the server

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)

    # Final processing (assigning IDs and saving to DB)
    for i, car in enumerate(all_cars):
        car.auction_id = f"{i+1}_{car.mileage_km}_{car.price_pln}"
        insert_into_db(car)

    logging.info(f"Scraper finished at {datetime.now()}. Processed {len(all_cars)} listings.")
    # write_to_csv(all_cars) # Optional: uncomment for local CSV backup

if __name__ == "__main__":
    run_scraper()
