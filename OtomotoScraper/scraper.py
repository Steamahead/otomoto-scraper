import os
import csv
import time
import re
import hashlib
from dataclasses import dataclass, asdict
from datetime import datetime
from difflib import SequenceMatcher
from typing import List, Tuple, Set, Dict

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import pyodbc
import tempfile

# ---------------------------
# Constants & Data Structure Definition
# ---------------------------
BASE_URL = ("https://www.otomoto.pl/osobowe/ds-automobiles/ds-7-crossback?"
            "search[advanced_search_expanded]=true")
EXPECTED_PER_PAGE = 32
MAX_PAGES_TO_CHECK = 20
DEBUG_MODE = True

# Process only auctions whose normalized URL begins with this prefix.
REQUIRED_PREFIX = "https://www.otomoto.pl/osobowe/oferta/ds-automobiles-ds-7-crossback"

# Updated candidate DS version names.
CANDIDATE_VERSIONS = [
    "Elegance",
    "Performance Line",
    "Prestige",
    "Ultra Prestige",
    "Louvre",
    "Opera",
    "Rivoli",
    "Grand Chic",
    "Bastille",
    "Pallas",
    "Etoile",
    "La Premiere"
]


@dataclass
class Car:
    auction_id: str  # External ID
    link: str  # Clickable link (the normalized URL)
    full_name: str
    description: str
    year: int
    mileage_km: int  # Changed from str to int
    engine_capacity: int  # Changed from str to int
    engine_power: str
    fuel_type: str
    price_pln: int  # Internal only, not output in CSV
    seller_type: str  # "Prywatny sprzedawca" or "Firma"
    city: str
    voivodship: str
    scrape_date: str  # The date/time the data was scraped
    listing_status: str  # Default "Active"
    version: str  # DS version/inspiration (from fuzzy lookup)
    data_id: str  # Original data-id from HTML


# ---------------------------
# Utility Functions
# ---------------------------
def debug_print(message):
    if DEBUG_MODE:
        print(f"[DEBUG] {message}")


def basic_url_cleanup(url: str) -> str:
    """Very basic URL cleanup - just handle relative URLs"""
    url = url.strip()

    # Convert relative URL to absolute
    if url.startswith('/'):
        url = 'https://www.otomoto.pl' + url

    return url


def compute_auction_key(url: str) -> str:
    """Compute a stable unique key (MD5 hash) from the auction URL."""
    return hashlib.md5(url.encode('utf-8')).hexdigest()

def get_auction_number(auction_key: str) -> int:
    """
    Checks if an AuctionNumber already exists for the given AuctionKey.
    If it does, returns that number; if not, returns the next sequential number.
    """
    import logging
    import pymssql  # Use pymssql instead of pyodbc
    
    logging.info(f"Getting auction number for key: {auction_key}")
    try:
        connection = pymssql.connect(
            server=os.environ.get('DB_SERVER'),
            database=os.environ.get('DB_NAME'),
            user=os.environ.get('DB_UID'),
            password=os.environ.get('DB_PWD')
        )
        logging.info("Database connection established")
        cursor = connection.cursor()

        query = "SELECT TOP 1 AuctionNumber FROM Listings WHERE AuctionKey = %s ORDER BY CreatedDate DESC"
        cursor.execute(query, (auction_key,))
        row = cursor.fetchone()
        if row:
            auction_number = row[0]
            logging.info(f"Found existing auction number: {auction_number}")
        else:
            cursor.execute("SELECT ISNULL(MAX(AuctionNumber), 0) FROM Listings")
            max_val = cursor.fetchone()[0]
            auction_number = max_val + 1
            logging.info(f"Created new auction number: {auction_number}")

        cursor.close()
        connection.close()
        return auction_number
    except Exception as e:
        logging.error(f"Error in get_auction_number: {str(e)}")
        # Since we can't get a valid auction number, return a default
        return 1000000  # Return a large default number to avoid conflicts

def insert_into_db(car: Car) -> int:
    """Insert a car record into the database and return the ListingID."""
    import logging
    import pymssql  # Use pymssql instead of pyodbc
    
    logging.info(f"Inserting into database: {car.full_name[:30]}")
    try:
        connection = pymssql.connect(
            server=os.environ.get('DB_SERVER'),
            database=os.environ.get('DB_NAME'),
            user=os.environ.get('DB_UID'),
            password=os.environ.get('DB_PWD')
        )
        cursor = connection.cursor()

        try:
            auction_key = compute_auction_key(car.link)
            auction_number = get_auction_number(auction_key)

            insert_query = """
               INSERT INTO Listings (
                   ListingURL, AuctionKey, AuctionNumber, FullName, Description, Year, Mileage, EngineCapacity,
                   FuelType, City, Voivodship, SellerType, ScrapeDate, ListingStatus, Version, Price
               ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
               SELECT SCOPE_IDENTITY();
               """
            params = (
                car.link,
                auction_key,
                auction_number,
                car.full_name,
                car.description,
                car.year,
                car.mileage_km,
                car.engine_capacity,
                car.fuel_type,
                car.city,
                car.voivodship,
                car.seller_type,
                car.scrape_date,
                car.listing_status,
                car.version,
                car.price_pln
            )

            cursor.execute(insert_query, params)
            listing_id = cursor.fetchone()[0]
            connection.commit()
            logging.info(f"Successfully inserted car: {car.full_name} with ID: {listing_id}")
            return listing_id
        except Exception as e:
            connection.rollback()
            logging.error(f"Error inserting car {car.full_name}: {str(e)}")
            return None
        finally:
            cursor.close()
            connection.close()
    except Exception as e:
        logging.error(f"Database connection error: {str(e)}")
        return None

def fuzzy_contains(candidate: str, text: str, cutoff: float = 0.9) -> bool:
    candidate = candidate.lower()
    text = text.lower()
    candidate_len = len(candidate)
    for i in range(len(text) - candidate_len + 1):
        substring = text[i:i + candidate_len]
        if SequenceMatcher(None, candidate, substring).ratio() >= cutoff:
            return True
    return False


def extract_version(full_name: str, description: str) -> str:
    for cand in CANDIDATE_VERSIONS:
        if fuzzy_contains(cand, full_name, 0.9) or fuzzy_contains(cand, description, 0.9):
            return cand
    return ""


def parse_location(location_str: str) -> Tuple[str, str]:
    if "(" in location_str and location_str.endswith(")"):
        city, voivodship = location_str.split("(", 1)
        return city.strip(), voivodship.rstrip(")").strip()
    return location_str.strip(), ""


def setup_driver(headless: bool = False) -> webdriver.Chrome:
    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36")

    # Create a unique temporary directory for Chrome's user data
    unique_user_data_dir = tempfile.mkdtemp(prefix=f"chrome_user_data_{int(time.time() * 1000)}_")
    print(f"[DEBUG] Using unique user data directory: {unique_user_data_dir}")
    chrome_options.add_argument(f"--user-data-dir={unique_user_data_dir}")

    driver = webdriver.Chrome(options=chrome_options)
    driver.implicitly_wait(10)
    return driver


def scroll_page(driver, max_scrolls: int = 10, wait: float = 2.0) -> None:
    last_height = driver.execute_script("return document.body.scrollHeight")
    for i in range(max_scrolls):
        driver.execute_script("window.scrollBy(0, 500);")
        time.sleep(wait)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            print(f"Scrolling stabilized after {i + 1} scrolls.")
            break
        last_height = new_height


def get_total_auction_count_and_pages(driver) -> Tuple[int, int]:
    try:
        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")
        total_auctions = 0
        total_pages = 1
        h1_tag = soup.find("h1")
        if h1_tag:
            h1_text = h1_tag.get_text()
            match = re.search(r'(\d+)\s+ogłosz', h1_text)
            if match:
                total_auctions = int(match.group(1))
                debug_print(f"Found total auctions from h1: {total_auctions}")
        if total_auctions == 0:
            texts_with_counts = soup.find_all(string=re.compile(r'\d+\s+ogłosz'))
            for text in texts_with_counts:
                match = re.search(r'(\d+)\s+ogłosz', text)
                if match:
                    total_auctions = int(match.group(1))
                    debug_print(f"Found total auctions from text: {total_auctions}")
                    break
        pagination = soup.find("ul", class_=lambda x: x and "pagination" in x)
        if pagination:
            page_numbers = [int(li.get_text(strip=True)) for li in pagination.find_all("li")
                            if li.get_text(strip=True).isdigit()]
            if page_numbers:
                total_pages = max(page_numbers)
                debug_print(f"Found total pages from pagination: {total_pages}")
        if total_pages == 1 and total_auctions > EXPECTED_PER_PAGE:
            total_pages = (total_auctions + EXPECTED_PER_PAGE - 1) // EXPECTED_PER_PAGE
            debug_print(f"Estimated total pages from auction count: {total_pages}")
        if total_auctions == 0 and total_pages > 1:
            total_auctions = total_pages * EXPECTED_PER_PAGE
            debug_print(f"Estimated total auctions from page count: {total_auctions}")
        if total_auctions == 0:
            total_auctions = 320
            debug_print(f"Using default auction count: {total_auctions}")
        if total_pages == 1 and total_auctions > EXPECTED_PER_PAGE:
            total_pages = (total_auctions + EXPECTED_PER_PAGE - 1) // EXPECTED_PER_PAGE
            debug_print(f"Using calculated page count: {total_pages}")
        return total_auctions, total_pages
    except Exception as e:
        print(f"Error getting auction counts: {e}")
        return 320, 10


def save_page_html(driver, page_num):
    if DEBUG_MODE:
        html = driver.page_source
        # Don't try to save to files in Azure Functions
        print(f"HTML snippet for page {page_num}: {html[:500]}...")

def extract_cars_from_html(html: str) -> List[Car]:
    cars: List[Car] = []
    soup = BeautifulSoup(html, "html.parser")
    container = soup.find("div", {"data-testid": "search-results"})
    if not container:
        print("Search results container not found in HTML!")
        return cars

    listings = container.find_all("article", attrs={"data-id": True})
    for listing in listings:
        try:
            # Get the data-id attribute
            data_id = listing.get("data-id", "")

            h2_tag = listing.find("h2", class_=lambda c: c and "ooa-1jjzghu" in c)
            if not h2_tag:
                continue

            a_tag = h2_tag.find("a", href=True)
            raw_link = a_tag["href"] if a_tag else ""
            cleaned_link = basic_url_cleanup(raw_link)

            # Skip if the URL doesn't match our required prefix
            if not cleaned_link.startswith(REQUIRED_PREFIX):
                continue

            full_name = a_tag.get_text(strip=True) if a_tag else ""

            # Use the updated selector for description
            desc_tag = listing.find("p", attrs={"data-sentry-element": "SubTitle"})
            full_desc = desc_tag.get_text(strip=True) if desc_tag else ""
            parts = [part.strip() for part in full_desc.split("•") if part.strip()]

            # For engine capacity, assume it's the first part (like "1 997 cm3")
            engine_capacity_text = parts[0] if len(parts) >= 1 else ""
            # Remove non-digit characters and convert to int:
            engine_capacity_clean = int(re.sub(r'\D', '', engine_capacity_text)) if engine_capacity_text and re.search(
                r'\d', engine_capacity_text) else 0

            engine_power = parts[1] if len(parts) >= 2 else ""
            # The rest becomes the description:
            description = " • ".join(parts[2:]) if len(parts) >= 3 else ""

            year_tag = listing.find("dd", {"data-parameter": "year"})
            year_str = year_tag.get_text(strip=True) if year_tag else "0"
            try:
                year = int(year_str)
            except ValueError:
                year = 0

            mileage_tag = listing.find("dd", {"data-parameter": "mileage"})
            mileage_text = mileage_tag.get_text(strip=True) if mileage_tag else ""
            # Remove non-digits (e.g., "km") and convert:
            mileage_clean = int(re.sub(r'\D', '', mileage_text)) if mileage_text and re.search(r'\d',
                                                                                               mileage_text) else 0

            fuel_tag = listing.find("dd", {"data-parameter": "fuel_type"})
            fuel_type = fuel_tag.get_text(strip=True) if fuel_tag else ""
            # Replace "Hybryda" with "Hybryda Plug-in" (case-insensitive)
            if fuel_type.strip().lower() == "hybryda":
                fuel_type = "Hybryda Plug-in"

            price_tag = listing.find("h3", attrs={"data-sentry-element": "Price"})
            if price_tag:
                raw_price = price_tag.get_text(strip=True)
                try:
                    price_pln = int(raw_price.replace(" ", "").replace("PLN", "").replace("zł", ""))
                except ValueError:
                    price_pln = 0
            else:
                price_pln = 0

            location_tag = listing.find("p", class_="ooa-oj1jk2")
            location_str = location_tag.get_text(strip=True) if location_tag else ""
            city, voivodship = parse_location(location_str)

            seller_elem = listing.find("article", class_=lambda c: c and "ooa-12g3tpj" in c)
            seller_text = seller_elem.get_text(strip=True) if seller_elem else "Unknown"
            seller_type = "Prywatny sprzedawca" if seller_text.lower() == "prywatny sprzedawca" else "Firma"

            scrape_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            found_version = extract_version(full_name, full_desc)

            car = Car(
                auction_id="",  # Will be set later
                link=cleaned_link,
                full_name=full_name,
                description=description,
                year=year,
                mileage_km=mileage_clean,
                engine_capacity=engine_capacity_clean,
                engine_power=engine_power,
                fuel_type=fuel_type,
                price_pln=price_pln,
                seller_type=seller_type,
                city=city,
                voivodship=voivodship,
                listing_status="Active",
                version=found_version,
                scrape_date=scrape_date,
                data_id=data_id
            )
            cars.append(car)
        except Exception as e:
            print(f"Error parsing listing: {e}")
    return cars


def write_to_csv(cars: List[Car]) -> None:
    import tempfile
    import os

    # Get the system temporary directory
    temp_dir = tempfile.gettempdir()
    # Define a path for your CSV file in that directory
    csv_path = os.path.join(temp_dir, "cars.csv")

    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        fieldnames = [
            "auction_id", "link", "full_name", "description", "year",
            "mileage_km", "engine_capacity", "engine_power", "fuel_type",
            "seller_type", "city", "voivodship", "scrape_date", "listing_status", "version"
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for car in cars:
            car_dict = asdict(car)
            car_dict.pop("price_pln", None)
            car_dict.pop("data_id", None)  # Don't include this in the CSV
            writer.writerow(car_dict)
    print(f"Data saved to file {csv_path} with {len(cars)} unique listings.")


# ---------------------------
# Main Scraper Function
# ---------------------------
def run_scraper():
    print(f"[DEBUG] run_scraper starting at {datetime.now()}")
    driver = None
    all_cars: List[Car] = []

    # NO DUPLICATE DETECTION AT ALL
    # We're going to process every single listing on every page

    auction_counter = 0
    processed_counter = 0

    try:
        driver = setup_driver(headless=True)
        driver.get(BASE_URL)
        time.sleep(5)
        total_auctions, total_pages = get_total_auction_count_and_pages(driver)
        print(f"Total auctions found on the site: {total_auctions}")
        print(f"Estimated total pages: {total_pages}")
        save_page_html(driver, 1)
        pages_to_check = min(total_pages, MAX_PAGES_TO_CHECK)

        for current_page in range(1, pages_to_check + 1):
            page_url = BASE_URL if current_page == 1 else f"{BASE_URL}&page={current_page}"
            print(f"\nFetching page {current_page} of {pages_to_check}: {page_url}")
            driver.get(page_url)
            time.sleep(5)
            scroll_page(driver, max_scrolls=10, wait=2)
            save_page_html(driver, current_page)
            html = driver.page_source
            cars_on_page = extract_cars_from_html(html)

            if not cars_on_page:
                print(f"No auctions found on page {current_page}. Stopping.")
                break

            print(f"Found {len(cars_on_page)} cars on page {current_page}")

            for car in cars_on_page:
                processed_counter += 1

                # NO DUPLICATE DETECTION AT ALL
                # Process every car we find

                # Generate auction ID
                auction_counter += 1
                mileage_digits = str(car.mileage_km)
                car.auction_id = f"{auction_counter}_{mileage_digits}_{car.price_pln}"

                # Insert the car into the database
                try:
                    db_id = insert_into_db(car)
                    if db_id:
                        print(f"Database insertion successful, ID: {db_id}")
                    else:
                        print("Database insertion failed")
                except Exception as e:
                    print(f"Error during database insertion: {e}")

                # Add to the list of all cars (for CSV backup)
                all_cars.append(car)

            print(f"After page {current_page}:")
            print(f"- Total processed and collected: {processed_counter}")

    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()

    finally:
        if driver is not None:
            driver.quit()
            print("ChromeDriver closed successfully.")

    print(f"[DEBUG] run_scraper ended at {datetime.now()}")
    print("\n=== FINAL RESULTS ===")
    print(f"Total auctions processed and collected: {processed_counter}")
    write_to_csv(all_cars)


if __name__ == "__main__":
    run_scraper()
