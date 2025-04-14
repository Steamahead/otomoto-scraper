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
from typing import List, Tuple, Set, Dict

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
# Database Functions
# ---------------------------
def get_sql_connection():
    """Get SQL connection using SQL authentication only"""
    import logging
    import os

    try:
        # Import pymssql directly
        import pymssql

        # Get connection details from environment variables
        server = os.environ.get('DB_SERVER')
        database = os.environ.get('DB_NAME')
        username = os.environ.get('DB_UID')
        password = os.environ.get('DB_PWD')

        logging.info(f"Connecting to SQL server with SQL auth: {server}/{database} as {username}")

        # Connect using pymssql with SQL authentication
        connection = pymssql.connect(
            server=server,
            user=username,
            password=password,
            database=database,
            timeout=30,
            appname="AzureFunctionsApp"
        )

        logging.info("SQL connection successful with SQL auth")
        return connection
    except Exception as e:
        logging.error(f"SQL connection error: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        return None


def compute_auction_key(url: str) -> str:
    """Compute a stable unique key (MD5 hash) from the auction URL."""
    return hashlib.md5(url.encode('utf-8')).hexdigest()


def get_auction_number(auction_key: str, data_id: str) -> int:
    """Checks if an AuctionNumber already exists for the given AuctionKey or DataID."""
    logging.info(f"Getting auction number for key: {auction_key}, data_id: {data_id}")
    connection = None

    try:
        connection = get_sql_connection()
        if not connection:
            logging.error("Failed to establish database connection")
            return 1000000  # Default value

        cursor = connection.cursor()

        # First check for existing auction by auction_key (URL-based)
        query = "SELECT TOP 1 AuctionNumber FROM Listings WHERE AuctionKey = %s ORDER BY CreatedDate DESC"
        cursor.execute(query, (auction_key,))
        row = cursor.fetchone()

        if row:
            auction_number = row[0]
            logging.info(f"Found existing auction number by URL: {auction_number}")
            return auction_number

        # If data_id is not empty, check if an auction with this data_id already exists
        if data_id:
            # Check if the column exists first
            try:
                data_id_query = "SELECT TOP 1 AuctionNumber FROM Listings WHERE DataID = %s ORDER BY CreatedDate DESC"
                cursor.execute(data_id_query, (data_id,))
                data_id_row = cursor.fetchone()
                
                if data_id_row:
                    auction_number = data_id_row[0]
                    logging.info(f"Found existing auction number by DataID: {auction_number}")
                    return auction_number
            except Exception as e:
                # DataID column might not exist yet
                logging.warning(f"Error checking DataID (column might not exist): {e}")

        # Get a new auction number if no match found
        max_query = "SELECT ISNULL(MAX(AuctionNumber), 0) FROM Listings"
        cursor.execute(max_query)
        max_val = cursor.fetchone()[0]
        auction_number = int(max_val) + 1
        logging.info(f"Created new auction number: {auction_number}")

        return auction_number
    except Exception as e:
        logging.error(f"Error in get_auction_number: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        return 1000000  # Default value
    finally:
        if connection:
            try:
                connection.close()
                logging.debug("Connection closed in get_auction_number")
            except Exception as close_error:
                logging.warning(f"Error closing connection: {str(close_error)}")


def ensure_data_id_column():
    """Ensure the DataID column exists in the Listings table."""
    connection = None
    cursor = None
    
    try:
        connection = get_sql_connection()
        if not connection:
            logging.error("Failed to establish database connection")
            return
            
        cursor = connection.cursor()
        
        # Check if DataID column exists
        check_query = """
        IF NOT EXISTS (
            SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'Listings' AND COLUMN_NAME = 'DataID'
        )
        BEGIN
            ALTER TABLE Listings ADD DataID NVARCHAR(100) NULL;
            PRINT 'DataID column added successfully.';
        END
        ELSE
        BEGIN
            PRINT 'DataID column already exists.';
        END
        """
        
        cursor.execute(check_query)
        connection.commit()
        logging.info("Ensured DataID column exists")
    except Exception as e:
        logging.error(f"Error ensuring DataID column: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            try:
                connection.close()
            except Exception as close_error:
                logging.warning(f"Error closing connection: {str(close_error)}")


def insert_into_db(car: Car) -> int:
    """Insert a car record into the database and return the ListingID."""
    logging.info(f"Inserting into database: {car.full_name[:30]}")
    connection = None
    cursor = None

    try:
        connection = get_sql_connection()
        if not connection:
            logging.error(f"Failed to establish database connection for car: {car.full_name}")
            return None

        cursor = connection.cursor()

        try:
            auction_key = compute_auction_key(car.link)
            auction_number = get_auction_number(auction_key, car.data_id)
            
            # ADDED: Check if this auction was already scraped today (by auction number)
            # This prevents duplicate insertions of the same auction on the same day
            today = datetime.now().strftime("%Y-%m-%d")
            check_query = """
            SELECT TOP 1 ListingID 
            FROM Listings 
            WHERE AuctionNumber = %s 
            AND CONVERT(date, ScrapeDate) = %s
            ORDER BY ScrapeDate DESC
            """
            
            cursor.execute(check_query, (auction_number, today))
            existing_record = cursor.fetchone()
            
            if existing_record:
                # Auction already exists today - return existing ID and skip insertion
                listing_id = existing_record[0]
                logging.info(f"Auction with number {auction_number} already scraped today. Reusing ID: {listing_id}")
                return listing_id

            # Check if DataID column exists before inserting
            has_data_id_column = True
            try:
                check_query = """
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'Listings' AND COLUMN_NAME = 'DataID'
                """
                cursor.execute(check_query)
                column_count = cursor.fetchone()[0]
                has_data_id_column = column_count > 0
            except Exception as e:
                logging.warning(f"Error checking for DataID column: {e}")
                has_data_id_column = False

            # Insert query with or without DataID based on column existence
            if has_data_id_column:
                insert_query = """
                   INSERT INTO Listings (
                       ListingURL, AuctionKey, AuctionNumber, FullName, Description, Year, Mileage, EngineCapacity,
                       FuelType, City, Voivodship, SellerType, ScrapeDate, ListingStatus, Version, Price, DataID
                   ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
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
                    car.price_pln,
                    car.data_id  # Include data_id in the parameters
                )
            else:
                # Original query without DataID
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
            if connection:
                connection.rollback()
            logging.error(f"Error inserting car {car.full_name}: {str(e)}")
            import traceback
            logging.error(traceback.format_exc())
            return None
    except Exception as e:
        logging.error(f"Database connection error: {str(e)}")
        return None
    finally:
        if cursor:
            cursor.close()
        if connection:
            try:
                connection.close()
                logging.debug("Connection closed in insert_into_db")
            except Exception as close_error:
                logging.warning(f"Error closing connection: {str(close_error)}")

# ---------------------------
# Utility Functions
# ---------------------------
def debug_print(message):
    if DEBUG_MODE:
        logging.info(f"[DEBUG] {message}")


def basic_url_cleanup(url: str) -> str:
    """Very basic URL cleanup - just handle relative URLs"""
    url = url.strip()

    # Convert relative URL to absolute
    if url.startswith('/'):
        url = 'https://www.otomoto.pl' + url

    return url


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


# ---------------------------
# Web Scraping Functions
# ---------------------------
def get_page_html(url: str) -> str:
    """Get page HTML using requests instead of Selenium"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',
        'Accept-Language': 'pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Referer': 'https://www.otomoto.pl/'
    }

    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.text
    except Exception as e:
        logging.error(f"Error fetching URL {url}: {str(e)}")
        return ""


def get_total_auction_count_and_pages(html: str) -> Tuple[int, int]:
    try:
        soup = BeautifulSoup(html, "html.parser")
        total_auctions = 0
        total_pages = 1

        # Find total auctions from h1 text
        h1_tag = soup.find("h1")
        if h1_tag:
            h1_text = h1_tag.get_text()
            match = re.search(r'(\d+)\s+ogłosz', h1_text)
            if match:
                total_auctions = int(match.group(1))
                debug_print(f"Found total auctions from h1: {total_auctions}")

        # Find total auctions from text if not found in h1
        if total_auctions == 0:
            texts_with_counts = soup.find_all(string=re.compile(r'\d+\s+ogłosz'))
            for text in texts_with_counts:
                match = re.search(r'(\d+)\s+ogłosz', text)
                if match:
                    total_auctions = int(match.group(1))
                    debug_print(f"Found total auctions from text: {total_auctions}")
                    break

        # Find pagination
        pagination = soup.find("ul", class_=lambda x: x and "pagination" in x)
        if pagination:
            page_numbers = [int(li.get_text(strip=True)) for li in pagination.find_all("li")
                            if li.get_text(strip=True).isdigit()]
            if page_numbers:
                total_pages = max(page_numbers)
                debug_print(f"Found total pages from pagination: {total_pages}")

        # Calculate total pages if not found
        if total_pages == 1 and total_auctions > EXPECTED_PER_PAGE:
            total_pages = (total_auctions + EXPECTED_PER_PAGE - 1) // EXPECTED_PER_PAGE
            debug_print(f"Estimated total pages from auction count: {total_pages}")

        # Calculate total auctions if not found
        if total_auctions == 0 and total_pages > 1:
            total_auctions = total_pages * EXPECTED_PER_PAGE
            debug_print(f"Estimated total auctions from page count: {total_auctions}")

        # Use defaults if nothing found
        if total_auctions == 0:
            total_auctions = 320
            debug_print(f"Using default auction count: {total_auctions}")
        if total_pages == 1 and total_auctions > EXPECTED_PER_PAGE:
            total_pages = (total_auctions + EXPECTED_PER_PAGE - 1) // EXPECTED_PER_PAGE
            debug_print(f"Using calculated page count: {total_pages}")

        return total_auctions, total_pages
    except Exception as e:
        logging.error(f"Error getting auction counts: {e}")
        return 320, 10


def extract_cars_from_html(html: str) -> List[Car]:
    cars: List[Car] = []
    soup = BeautifulSoup(html, "html.parser")

    # Try to find the container with multiple possible selectors
    container = soup.find("div", {"data-testid": "search-results"})
    if not container:
        # Try alternative selectors if the primary one fails
        container = soup.find("div", class_=lambda c: c and "ooa-1e1uucc" in c)

    if not container:
        logging.error("Search results container not found in HTML!")
        return cars

    # Find all listings
    listings = container.find_all("article", attrs={"data-id": True})

    for listing in listings:
        try:
            # Get the data-id attribute
            data_id = listing.get("data-id", "")

            # Get title and link
            h2_tag = listing.find("h2", class_=lambda c: c and "ooa-1jjzghu" in c)
            if not h2_tag:
                # Try alternative selector
                h2_tag = listing.find("h2", {"data-testid": "ad-title"})
                if not h2_tag:
                    continue

            a_tag = h2_tag.find("a", href=True)
            raw_link = a_tag["href"] if a_tag else ""
            cleaned_link = basic_url_cleanup(raw_link)

            # Skip if the URL doesn't match our required prefix
            if not cleaned_link.startswith(REQUIRED_PREFIX):
                continue

            full_name = a_tag.get_text(strip=True) if a_tag else ""

            # Extract description
            desc_tag = listing.find("p", attrs={"data-sentry-element": "SubTitle"})
            if not desc_tag:
                # Try alternative selector
                desc_tag = listing.find("p", class_=lambda c: c and "ooa-1e4spvk" in c)

            full_desc = desc_tag.get_text(strip=True) if desc_tag else ""
            parts = [part.strip() for part in full_desc.split("•") if part.strip()]

            # Extract engine capacity
            engine_capacity_text = parts[0] if len(parts) >= 1 else ""
            engine_capacity_clean = int(re.sub(r'\D', '', engine_capacity_text)) if engine_capacity_text and re.search(
                r'\d', engine_capacity_text) else 0

            # Extract engine power
            engine_power = parts[1] if len(parts) >= 2 else ""

            # Build description from remaining parts
            description = " • ".join(parts[2:]) if len(parts) >= 3 else ""

            # Extract year
            year_tag = listing.find("dd", {"data-parameter": "year"})
            year_str = year_tag.get_text(strip=True) if year_tag else "0"
            try:
                year = int(year_str)
            except ValueError:
                year = 0

            # Extract mileage
            mileage_tag = listing.find("dd", {"data-parameter": "mileage"})
            mileage_text = mileage_tag.get_text(strip=True) if mileage_tag else ""
            mileage_clean = int(re.sub(r'\D', '', mileage_text)) if mileage_text and re.search(r'\d',
                                                                                               mileage_text) else 0

            # Extract fuel type
            fuel_tag = listing.find("dd", {"data-parameter": "fuel_type"})
            fuel_type = fuel_tag.get_text(strip=True) if fuel_tag else ""
            if fuel_type.strip().lower() == "hybryda":
                fuel_type = "Hybryda Plug-in"

            # Extract price
            price_tag = listing.find("h3", attrs={"data-sentry-element": "Price"})
            if not price_tag:
                # Try alternative selector
                price_tag = listing.find("p", attrs={"data-testid": "ad-price"})

            if price_tag:
                raw_price = price_tag.get_text(strip=True)
                try:
                    price_pln = int(raw_price.replace(" ", "").replace("PLN", "").replace("zł", ""))
                except ValueError:
                    price_pln = 0
            else:
                price_pln = 0

            # Extract location
            location_tag = listing.find("p", class_="ooa-oj1jk2")
            if not location_tag:
                location_tag = listing.find("p", attrs={"data-testid": "location-date"})

            location_str = location_tag.get_text(strip=True) if location_tag else ""
            city, voivodship = parse_location(location_str)

            # Extract seller type
            seller_elem = listing.find("article", class_=lambda c: c and "ooa-12g3tpj" in c)
            seller_text = seller_elem.get_text(strip=True) if seller_elem else "Unknown"
            seller_type = "Prywatny sprzedawca" if "prywatny" in seller_text.lower() else "Firma"

            # Generate other values
            scrape_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            found_version = extract_version(full_name, full_desc)

            # Create Car object
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
            logging.error(f"Error parsing listing: {e}")

    return cars


def write_to_csv(cars: List[Car]) -> None:
    try:
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
        logging.info(f"Data saved to file {csv_path} with {len(cars)} unique listings.")
    except Exception as e:
        logging.error(f"Error writing CSV: {e}")


# ---------------------------
# Main Scraper Function
# ---------------------------
def run_scraper():
    logging.info(f"[DEBUG] run_scraper starting at {datetime.now()}")
    all_cars: List[Car] = []
    auction_counter = 0
    processed_counter = 0
    new_inserts_counter = 0     # Nowo dodane aukcje
    duplicates_counter = 0      # Pominięte duplikaty
    failed_inserts_counter = 0  # Nieudane wstawienia

    try:
        # First, ensure DataID column exists in database
        ensure_data_id_column()
        
        # Get the main page
        html = get_page_html(BASE_URL)
        if not html:
            logging.error("Failed to fetch the main page")
            return

        # Get total auctions and pages
        total_auctions, total_pages = get_total_auction_count_and_pages(html)
        logging.info(f"Total auctions found on the site: {total_auctions}")
        logging.info(f"Estimated total pages: {total_pages}")

        # Process the first page HTML we already have
        cars_on_page = extract_cars_from_html(html)
        if cars_on_page:
            for car in cars_on_page:
                processed_counter += 1
                auction_counter += 1
                mileage_digits = str(car.mileage_km)
                car.auction_id = f"{auction_counter}_{mileage_digits}_{car.price_pln}"

                # Insert into DB
                try:
                    before_id_count = get_today_listing_count()
                    db_id = insert_into_db(car)
                    after_id_count = get_today_listing_count()
                    
                    if db_id:
                        if after_id_count > before_id_count:
                            logging.info(f"Database insertion successful (new entry), ID: {db_id}")
                            new_inserts_counter += 1
                        else:
                            logging.info(f"Database found existing entry, reused ID: {db_id}")
                            duplicates_counter += 1
                    else:
                        logging.error("Database insertion failed")
                        failed_inserts_counter += 1
                except Exception as e:
                    logging.error(f"Error during database insertion: {e}")
                    failed_inserts_counter += 1

                all_cars.append(car)

        # Determine how many pages to check
        pages_to_check = min(total_pages, MAX_PAGES_TO_CHECK)

        # Now process remaining pages
        for current_page in range(2, pages_to_check + 1):
            page_url = f"{BASE_URL}&page={current_page}"
            logging.info(f"\nFetching page {current_page} of {pages_to_check}: {page_url}")

            html = get_page_html(page_url)
            if not html:
                logging.error(f"Failed to fetch page {current_page}")
                continue

            cars_on_page = extract_cars_from_html(html)

            if not cars_on_page:
                logging.info(f"No auctions found on page {current_page}. Stopping.")
                break

            logging.info(f"Found {len(cars_on_page)} cars on page {current_page}")

            for car in cars_on_page:
                processed_counter += 1

                # Generate auction ID
                auction_counter += 1
                mileage_digits = str(car.mileage_km)
                car.auction_id = f"{auction_counter}_{mileage_digits}_{car.price_pln}"

                # Insert the car into the database
                try:
                    before_id_count = get_today_listing_count()
                    db_id = insert_into_db(car)
                    after_id_count = get_today_listing_count()
                    
                    if db_id:
                        if after_id_count > before_id_count:
                            logging.info(f"Database insertion successful (new entry), ID: {db_id}")
                            new_inserts_counter += 1
                        else:
                            logging.info(f"Database found existing entry, reused ID: {db_id}")
                            duplicates_counter += 1
                    else:
                        logging.info("Database insertion failed")
                        failed_inserts_counter += 1
                except Exception as e:
                    logging.error(f"Error during database insertion: {e}")
                    failed_inserts_counter += 1

                # Add to the list of all cars (for CSV backup)
                all_cars.append(car)

            logging.info(f"After page {current_page}:")
            logging.info(f"- Total processed: {processed_counter}")
            logging.info(f"- New entries: {new_inserts_counter}")
            logging.info(f"- Duplicates skipped: {duplicates_counter}")
            logging.info(f"- Failed insertions: {failed_inserts_counter}")

            # Small delay to avoid overloading the server
            time.sleep(2)

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        import traceback
        logging.error(traceback.format_exc())

    logging.info(f"[DEBUG] run_scraper ended at {datetime.now()}")
    logging.info("\n=== FINAL RESULTS ===")
    logging.info(f"Total auctions processed: {processed_counter}")
    logging.info(f"New entries added to database: {new_inserts_counter}")
    logging.info(f"Duplicate auctions skipped: {duplicates_counter}")
    logging.info(f"Failed insertions: {failed_inserts_counter}")
    logging.info(f"Total auctions in CSV: {len(all_cars)}")
    write_to_csv(all_cars)


def get_today_listing_count():
    """Get the count of listings added today."""
    connection = None
    cursor = None
    
    try:
        connection = get_sql_connection()
        if not connection:
            return 0
            
        cursor = connection.cursor()
        today = datetime.now().strftime("%Y-%m-%d")
        
        query = "SELECT COUNT(*) FROM Listings WHERE CONVERT(date, ScrapeDate) = %s"
        cursor.execute(query, (today,))
        count = cursor.fetchone()[0]
        return count
    except Exception as e:
        logging.warning(f"Error getting today's listing count: {e}")
        return 0
    finally:
        if cursor:
            cursor.close()
        if connection:
            try:
                connection.close()
            except Exception:
                pass

if __name__ == "__main__":
    run_scraper()
