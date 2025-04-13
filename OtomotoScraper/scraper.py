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
MAX_PAGES_TO_CHECK = 30  # Increased to ensure we get all pages
DEBUG_MODE = True

# Process only auctions whose normalized URL begins with this prefix.
# Made more flexible by using shorter prefix
REQUIRED_PREFIX = "https://www.otomoto.pl/osobowe/oferta/ds-automobiles-ds-7"

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

def get_auction_number(auction_key: str) -> int:
    """Checks if an AuctionNumber already exists for the given AuctionKey."""
    logging.info(f"Getting auction number for key: {auction_key}")
    connection = None
    
    try:
        connection = get_sql_connection()
        if not connection:
            logging.error("Failed to establish database connection")
            return 1000000  # Default value
            
        cursor = connection.cursor()
        
        # Check for existing auction number
        query = "SELECT TOP 1 AuctionNumber FROM Listings WHERE AuctionKey = %s ORDER BY CreatedDate DESC"
        cursor.execute(query, (auction_key,))
        row = cursor.fetchone()
        
        if row:
            auction_number = row[0]
            logging.info(f"Found existing auction number: {auction_number}")
        else:
            # Get max auction number
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
            auction_number = get_auction_number(auction_key)

            # For pymssql, use %s placeholders
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
            
def determine_engine_capacity(full_name, full_desc, fuel_type):
    """
    Determine engine capacity based on DS7 Crossback specific patterns.
    Only 3 possible values: 1598cc (1.6L), 1997cc (2.0L), or 1499cc (1.5L)
    """
    combined_text = (full_name + " " + full_desc).lower()
    
    # Method 1: Check for specific engine designations
    if any(term in combined_text for term in ["1.6", "1,6", "e-tense", "etense", "phev", "hybrid", "plug-in", "hybryda"]):
        return 1598  # 1.6L for hybrids and PureTech
    
    if any(term in combined_text for term in ["2.0", "2,0", "180", "hdi 180", "bluehdi 180"]):
        return 1997  # 2.0L BlueHDi
    
    if any(term in combined_text for term in ["1.5", "1,5", "130", "hdi 130", "bluehdi 130"]):
        return 1499  # 1.5L BlueHDi
    
    # Method 2: Check for power ratings that indicate specific engines
    power_matches = re.findall(r'(\d+)\s*(?:km|hp|ps|cv|ch)', combined_text, re.IGNORECASE)
    if power_matches:
        power = int(power_matches[0])
        if power >= 200:  # High power is always the hybrid
            return 1598  # 1.6L Hybrid
        elif power >= 150:  # Mid power is usually 2.0 diesel
            return 1997  # 2.0L Diesel
        else:  # Lower power is usually 1.5 diesel
            return 1499  # 1.5L Diesel
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
    
    # Add retry capability
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            return response.text
        except Exception as e:
            logging.error(f"Error fetching URL {url} (attempt {attempt+1}/{max_retries}): {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(2)  # Wait a bit before retrying
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

def find_container(soup):
    """Find the search results container with multiple fallback strategies"""
    # Strategy 1: Look for data-testid attribute
    container = soup.find("div", {"data-testid": "search-results"})
    if container:
        return container
    
    # Strategy 2: Look for specific class
    container = soup.find("div", class_=lambda c: c and "ooa-1e1uucc" in c)
    if container:
        return container

    # Strategy 3: Look for special classes
    for class_pattern in ["listings", "results", "search-results", "offers"]:
        container = soup.find("div", class_=lambda c: c and class_pattern.lower() in c.lower())
        if container:
            return container
    
    # Strategy 4: Look for divs that contain article elements
    for div in soup.find_all("div"):
        if div.find_all("article") or div.find_all("div", class_=lambda c: c and "offer" in c.lower()):
            return div
    
    return None

def find_listings(container):
    """Find all listings with multiple fallback strategies"""
    if not container:
        return []
    
    # Strategy 1: Articles with data-id
    listings = container.find_all("article", attrs={"data-id": True})
    if listings:
        return listings
    
    # Strategy 2: Articles with any class
    listings = container.find_all("article")
    if listings:
        return listings
    
    # Strategy 3: Divs with offer-related classes
    offer_patterns = ["offer", "listing", "advert", "item", "result"]
    for pattern in offer_patterns:
        listings = container.find_all("div", class_=lambda c: c and pattern.lower() in c.lower())
        if listings:
            return listings
    
    # Strategy 4: Divs with links containing "oferta"
    divs_with_links = []
    for div in container.find_all("div"):
        if div.find("a", href=lambda h: h and "oferta" in h):
            divs_with_links.append(div)
    
    if divs_with_links:
        return divs_with_links
    
    return []

def extract_cars_from_html(html: str) -> List[Car]:
    cars: List[Car] = []
    soup = BeautifulSoup(html, "html.parser")
    
    # Try to find the container with enhanced fallback strategies
    container = find_container(soup)
    
    if not container:
        logging.error("Search results container not found in HTML!")
        return cars

    # Find all listings with enhanced fallback strategies
    listings = find_listings(container)
    
    if not listings:
        logging.error("No listings found in the container!")
        return cars
    
    logging.info(f"Found {len(listings)} potential listings to process")
    
    for listing in listings:
        try:
            # Get the data-id attribute with fallback
            data_id = listing.get("data-id", "")
            if not data_id:
                # Try to get any id or data attribute
                for attr in listing.attrs:
                    if "id" in attr.lower() or attr.startswith("data-"):
                        data_id = listing.get(attr, "")
                        break
            
            # Find a title element - try multiple approaches
            h2_tag = None
            for tag_type in ["h2", "h3", "div", "p"]:
                # Try class-based selectors
                for class_pattern in ["title", "name", "header"]:
                    tags = listing.find_all(tag_type, class_=lambda c: c and class_pattern.lower() in c.lower())
                    if tags:
                        h2_tag = tags[0]
                        break
                
                # Try data attribute selectors
                if not h2_tag:
                    h2_tag = listing.find(tag_type, attrs={"data-testid": "ad-title"})
                
                # Try any h2/h3
                if not h2_tag and tag_type in ["h2", "h3"]:
                    h2_tag = listing.find(tag_type)
                
                if h2_tag:
                    break
            
            # If still no title element, look for any prominent text
            if not h2_tag:
                large_text_elements = listing.find_all(class_=lambda c: c and any(size in c.lower() for size in ["large", "big", "title", "head"]))
                if large_text_elements:
                    h2_tag = large_text_elements[0]
            
            # Find the link - either in the title or elsewhere
            a_tag = None
            if h2_tag:
                a_tag = h2_tag.find("a", href=True)
            
            # If no link in title, look for any link in the listing
            if not a_tag:
                a_tags = listing.find_all("a", href=True)
                for tag in a_tags:
                    href = tag.get("href", "")
                    if "oferta" in href or "ds-7" in href or "ds7" in href:
                        a_tag = tag
                        break
                # If still no match, just take the first link
                if not a_tag and a_tags:
                    a_tag = a_tags[0]
            
            if not a_tag:
                logging.warning(f"No link found in listing, skipping")
                continue
                
            raw_link = a_tag["href"]
            cleaned_link = basic_url_cleanup(raw_link)

            # More flexible URL filtering
            if not cleaned_link.startswith(REQUIRED_PREFIX):
                # Allow some variations
                if "ds-7" in cleaned_link or "ds7" in cleaned_link:
                    pass  # Accept these variations
                else:
                    logging.info(f"Skipping non-matching URL: {cleaned_link}")
                    continue

            # Extract title text with fallbacks
            if a_tag and a_tag.get_text(strip=True):
                full_name = a_tag.get_text(strip=True)
            elif h2_tag:
                full_name = h2_tag.get_text(strip=True)
            else:
                # Try to find any prominent text
                for tag in ["h2", "h3", "h4", "strong", "b"]:
                    element = listing.find(tag)
                    if element:
                        full_name = element.get_text(strip=True)
                        break
                else:
                    full_name = "DS 7 Crossback (title not found)"

            # Extract description with multiple approaches
            desc_tag = None
            desc_selectors = [
                {"data-sentry-element": "SubTitle"},
                {"class": lambda c: c and "subtitle" in c.lower()},
                {"class": lambda c: c and "desc" in c.lower()},
                {"class": lambda c: c and "detail" in c.lower()},
                {"class": lambda c: c and "parameter" in c.lower()},
            ]
            
            for selector in desc_selectors:
                for tag in ["p", "div", "span"]:
                    desc_tag = listing.find(tag, attrs=selector)
                    if desc_tag:
                        break
                if desc_tag:
                    break
            
            # If still no description, look for text near price or engine details
            if not desc_tag:
                # Look for text containing common engine or parameter terms
                for pattern in ["cm3", "cm³", "ccm", "kW", "KM", "HP", "benzyna", "diesel"]:
                    for tag in listing.find_all(["p", "div", "span"]):
                        if pattern in tag.get_text():
                            desc_tag = tag
                            break
                    if desc_tag:
                        break
            
            full_desc = desc_tag.get_text(strip=True) if desc_tag else ""
            
            # Split description by bullet points or similar separators
            for separator in ["•", "·", "●", "|", "-", ","]:
                if separator in full_desc:
                    parts = [part.strip() for part in full_desc.split(separator) if part.strip()]
                    if len(parts) >= 2:  # If we found reasonable parts
                        break
            else:
                # If no separator worked, use the whole string as one part
                parts = [full_desc] if full_desc else []

            # Extract engine capacity with improved logic
            engine_capacity_clean = 0
            if parts:
                # Look for cubic capacity patterns in all parts
                for part in parts:
                    if any(pattern in part.lower() for pattern in ["cm3", "cm³", "ccm", "pojemność"]):
                        digits = re.findall(r'\d+', part)
                        if digits:
                            # Join digits if there are spaces (e.g., "1 997" should become "1997")
                            joined_digits = ''.join(digits)
                            if 500 <= int(joined_digits) <= 9999:  # Realistic engine size
                                engine_capacity_clean = int(joined_digits)
                                break
            
            if engine_capacity_clean == 0 and parts:
                # Try first part as a fallback if it contains digits
                engine_capacity_text = parts[0]
                digits = re.findall(r'\d+', engine_capacity_text)
                if digits:
                    joined_digits = ''.join(digits)
                    if 500 <= int(joined_digits) <= 9999:
                        engine_capacity_clean = int(joined_digits)
            
            # Extract engine power
            engine_power = ""
            if len(parts) >= 2:
                # Look for power patterns
                for part in parts:
                    if any(pattern in part.lower() for pattern in ["km", "kw", "hp", "ps", "cv"]):
                        engine_power = part.strip()
                        break
                
                if not engine_power and len(parts) >= 2:
                    engine_power = parts[1].strip()  # Default to second part
            
            # Build description from remaining parts or the full description
            if len(parts) >= 3:
                description = " • ".join(parts[2:])
            else:
                description = full_desc
            
            # Enhanced year extraction
            year = 0
            year_tag = None
            
            # Try multiple selectors for year
            for param_name in ["year", "rok", "production_year"]:
                year_tag = listing.find(["dd", "div", "span"], {"data-parameter": param_name}) or \
                          listing.find(["dd", "div", "span"], {"data-code": param_name})
                if year_tag:
                    break
            
            # Look for year pattern in text if tag not found
            if not year_tag:
                # Look for text with year pattern (4 digits between 1990-2030)
                year_texts = re.findall(r'\b(19[9][0-9]|20[0-2][0-9])\b', str(listing))
                if year_texts:
                    try:
                        year = int(year_texts[0])
                    except ValueError:
                        year = 0
            else:
                year_str = year_tag.get_text(strip=True)
                try:
                    year = int(re.search(r'\d{4}', year_str).group(0))
                except (ValueError, AttributeError):
                    # Try to extract any 4-digit number
                    try:
                        year = int(re.search(r'\d{4}', year_str).group(0))
                    except (ValueError, AttributeError):
                        year = 0
            
            # Default to current year if nothing found and seems reasonable
            if year == 0 or year < 2000 or year > datetime.now().year:
                year = datetime.now().year - 2  # Default to 2 years old
            
            # Enhanced mileage extraction
            mileage_clean = 0
            mileage_tag = None
            
            # Try multiple selectors for mileage
            for param_name in ["mileage", "przebieg", "km"]:
                mileage_tag = listing.find(["dd", "div", "span"], {"data-parameter": param_name}) or \
                             listing.find(["dd", "div", "span"], {"data-code": param_name})
                if mileage_tag:
                    break
            
            # If not found by parameter, look for text with km pattern
            if not mileage_tag:
                for tag in listing.find_all(["p", "div", "span"]):
                    text = tag.get_text().lower()
                    if "km" in text and any(c.isdigit() for c in text):
                        mileage_tag = tag
                        break
            
            if mileage_tag:
                mileage_text = mileage_tag.get_text(strip=True)
                # Extract digits, handling formats like "35 000 km"
                digits = re.sub(r'\D', '', mileage_text)
                if digits:
                    mileage_clean = int(digits)
            
            # Enhanced fuel type extraction
            fuel_type = ""
            fuel_tag = None
            
            # Try multiple selectors for fuel type
            for param_name in ["fuel_type", "paliwo", "napęd", "fuel"]:
                fuel_tag = listing.find(["dd", "div", "span"], {"data-parameter": param_name}) or \
                          listing.find(["dd", "div", "span"], {"data-code": param_name})
                if fuel_tag:
                    break
            
            # If not found by parameter, look for specific fuel keywords
            if not fuel_tag:
                fuel_keywords = ["benzyna", "diesel", "hybryda", "elektryczny", "petrol", "gasoline", "hybrid", "electric"]
                for tag in listing.find_all(["p", "div", "span"]):
                    text = tag.get_text().lower()
                    for keyword in fuel_keywords:
                        if keyword in text:
                            fuel_tag = tag
                            fuel_type = keyword.capitalize()
                            break
                    if fuel_type:
                        break
            else:
                fuel_type = fuel_tag.get_text(strip=True)
            
            # Handle specific "Hybryda" case
            if fuel_type.strip().lower() == "hybryda":
                fuel_type = "Hybryda Plug-in"
            
            # Default if not found
            if not fuel_type:
                # Check title and description for clues
                if any(term in full_name.lower() for term in ["hybrid", "hybryda", "phev", "e-tense"]):
                    fuel_type = "Hybryda Plug-in"
                elif any(term in full_name.lower() for term in ["diesel", "bluehdi"]):
                    fuel_type = "Diesel"
                else:
                    fuel_type = "Benzyna"  # Default to gasoline
            
            # NOW that we have fuel_type defined, we can call determine_engine_capacity
            engine_capacity_clean = determine_engine_capacity(full_name, full_desc, fuel_type)
            
            # Enhanced price extraction
            price_pln = 0
            price_tag = None
            
            # Try multiple selectors for price
            price_selectors = [
                {"data-sentry-element": "Price"},
                {"data-testid": "ad-price"},
                {"class": lambda c: c and "price" in c.lower()},
                {"class": lambda c: c and "cena" in c.lower()},
                {"class": lambda c: c and "cost" in c.lower()}
            ]
            
            for selector in price_selectors:
                for tag_type in ["h3", "p", "div", "span", "strong"]:
                    price_tag = listing.find(tag_type, attrs=selector)
                    if price_tag:
                        break
                if price_tag:
                    break
            
            # If still no price, look for text with currency
            if not price_tag:
                for currency in ["zł", "PLN", "pln", "zl"]:
                    for tag in listing.find_all(["p", "div", "span", "strong"]):
                        if currency in tag.get_text():
                            price_tag = tag
                            break
                    if price_tag:
                        break
            
            if price_tag:
                raw_price = price_tag.get_text(strip=True)
                try:
                    # Extract digits, handling formats like "179 900 zł"
                    price_digits = re.sub(r'\D', '', raw_price)
                    if price_digits:
                        price_pln = int(price_digits)
                except ValueError:
                    price_pln = 0
            
            # Enhanced location extraction
            location_tag = None
            location_selectors = [
                {"class": "ooa-oj1jk2"},
                {"data-testid": "location-date"},
                {"class": lambda c: c and "location" in c.lower()},
                {"class": lambda c: c and "place" in c.lower()}
            ]
            
            for selector in location_selectors:
                for tag_type in ["p", "div", "span"]:
                    location_tag = listing.find(tag_type, attrs=selector)
                    if location_tag:
                        break
                if location_tag:
                    break
            
            # If not found, look for text with city names or location patterns
            if not location_tag:
                cities = ["warszawa", "kraków", "wrocław", "poznań", "gdańsk", "szczecin", "łódź", "lublin"]
                for tag in listing.find_all(["p", "div", "span"]):
                    text = tag.get_text().lower()
                    if any(city in text for city in cities) or "(" in text and ")" in text:
                        location_tag = tag
                        break
            
            location_str = location_tag.get_text(strip=True) if location_tag else ""
            city, voivodship = parse_location(location_str)
            
            # Default location if not found
            if not city:
                city = "Nieznana"
            if not voivodship:
                voivodship = "Nieznane"
            
            # Enhanced seller type extraction
            seller_type = "Firma"  # Default to dealer
            seller_elem = None
            
            seller_selectors = [
                {"class": lambda c: c and "ooa-12g3tpj" in c},
                {"data-testid": "seller-info"},
                {"class": lambda c: c and "seller" in c.lower()},
                {"class": lambda c: c and "dealer" in c.lower()},
                {"class": lambda c: c and "owner" in c.lower()}
            ]
            
            for selector in seller_selectors:
                for tag_type in ["article", "div", "section", "p"]:
                    seller_elem = listing.find(tag_type, attrs=selector)
                    if seller_elem:
                        break
                if seller_elem:
                    break
            
            if seller_elem:
                seller_text = seller_elem.get_text(strip=True)
                if "prywatny" in seller_text.lower():
                    seller_type = "Prywatny sprzedawca"
            
            # Generate other values
            scrape_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            found_version = extract_version(full_name, full_desc)
            
            # Ensure year and mileage are reasonable
            if year <= 2016 or year > datetime.now().year:
                year = 2020  # Default if unreasonable
            
            if mileage_clean > 500000 or mileage_clean < 10:
                mileage_clean = 50000  # Default if unreasonable
            
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
            logging.info(f"Successfully parsed listing: {full_name[:30]}...")
        except Exception as e:
            logging.error(f"Error parsing listing: {e}")
            import traceback
            logging.error(traceback.format_exc())
            
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

    try:
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
                    db_id = insert_into_db(car)
                    if db_id:
                        logging.info(f"Database insertion successful, ID: {db_id}")
                    else:
                        logging.error("Database insertion failed")
                except Exception as e:
                    logging.error(f"Error during database insertion: {e}")
                
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
                    db_id = insert_into_db(car)
                    if db_id:
                        logging.info(f"Database insertion successful, ID: {db_id}")
                    else:
                        logging.info("Database insertion failed")
                except Exception as e:
                    logging.error(f"Error during database insertion: {e}")

                # Add to the list of all cars (for CSV backup)
                all_cars.append(car)

            logging.info(f"After page {current_page}:")
            logging.info(f"- Total processed and collected: {processed_counter}")
            
            # Small delay to avoid overloading the server
            time.sleep(2)

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        import traceback
        logging.error(traceback.format_exc())

    logging.info(f"[DEBUG] run_scraper ended at {datetime.now()}")
    logging.info("\n=== FINAL RESULTS ===")
    logging.info(f"Total auctions processed and collected: {processed_counter}")
    write_to_csv(all_cars)
    
    # Summary of what was found
    logging.info(f"Auctions by year:")
    years = {}
    for car in all_cars:
        if car.year in years:
            years[car.year] += 1
        else:
            years[car.year] = 1
    
    for year, count in sorted(years.items()):
        logging.info(f" - {year}: {count} cars")


if __name__ == "__main__":
    run_scraper()
