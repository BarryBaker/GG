#!/usr/bin/env python3
"""
GGPoker Omaha Daily Leaderboard Scraper
Basic scraper to access the page and interact with the iframe
"""

import time
import datetime
from selenium import webdriver
import argparse
from database_manager import DatabaseManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
import os
try:
    from webdriver_manager.core.utils import ChromeType
except Exception:
    try:
        from webdriver_manager.chrome import ChromeType
    except Exception:
        from webdriver_manager.utils import ChromeType
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class GGPokerScraper:
    def __init__(self, headless=False):
        """
        Initialize the scraper
        
        Args:
            headless (bool): If True, run browser in headless mode. 
                           If False, browser will be visible so you can follow along.
        """
        self.driver = None
        self.headless = headless
        self.setup_driver()
        
        # Initialize database manager
        self.db_manager = DatabaseManager()
        print("✅ Database manager initialized")

    def setup_driver(self):
        """Set up the Chrome WebDriver with appropriate options"""
        chrome_options = Options()

        # Headless/non-GPU safe defaults for server/containers
        headless_env = os.getenv("HEADLESS", "1").lower() in ("1", "true", "yes")
        if self.headless or headless_env:
            chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")

        # Allow overriding Chrome binary (useful in Docker with Chromium)
        chrome_bin = os.getenv("CHROME_BIN")
        if chrome_bin:
            chrome_options.binary_location = chrome_bin

        # Install and setup ChromeDriver automatically, but allow a fixed binary path
        chromedriver_bin = os.getenv("CHROME_DRIVER_BIN")
        if chromedriver_bin and os.path.exists(chromedriver_bin):
            service = Service(executable_path=chromedriver_bin)
        else:
            chrome_type_env = os.getenv("CHROME_TYPE", "chrome").lower()
            if chrome_type_env == "chromium":
                service = Service(ChromeDriverManager(chrome_type=ChromeType.CHROMIUM).install())
            else:
                service = Service(ChromeDriverManager().install())
        
        try:
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            # Remove webdriver property to avoid detection
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            print("✅ WebDriver initialized successfully")
        except Exception as e:
            print(f"❌ Error setting up WebDriver: {e}")
            raise

    def access_ggpoker_page(self):
        """Access the GGPoker Omaha Daily Leaderboard page"""
        url = "https://ggpoker.com/promotions/omaha-daily-leaderboard/"
        
        try:
            print(f"🌐 Accessing: {url}")
            self.driver.get(url)
            
            # Wait for page to load
            time.sleep(3)
            
            # Wait for the main content to be present
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            print("✅ Page loaded successfully")
            return True
            
        except Exception as e:
            print(f"❌ Error accessing page: {e}")
            return False

    def find_plo_section(self):
        """Find the PLO section with the iframe"""
        try:
            # Look for the PLO heading
            plo_heading = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//h4[contains(text(), 'PLO')]"))
            )
            print("✅ Found PLO section heading")
            
            # Find the iframe within the PLO section
            # The iframe should be near the PLO headinggit branch -M main
            iframe = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "iframe"))
            )
            
            print("✅ Found iframe")
            
            # Get the iframe source URL
            iframe_src = iframe.get_attribute("src")
            print(f"🔗 Iframe source URL: {iframe_src}")
            
            return iframe, iframe_src
            
        except Exception as e:
            print(f"❌ Error finding PLO section: {e}")
            return None, None

    def click_iframe_link(self, iframe):
        """Click on the iframe to interact with it"""
        try:
            print("🖱️ Clicking on iframe...")
            
            # Scroll to the iframe to make it visible
            self.driver.execute_script("arguments[0].scrollIntoView(true);", iframe)
            time.sleep(1)
            
            # Click on the iframe
            iframe.click()
            print("✅ Clicked on iframe")
            
            # Wait a moment to see if anything happens
            time.sleep(2)
            
            return True
            
        except Exception as e:
            print(f"❌ Error clicking iframe: {e}")
            return False

    def explore_iframe_content(self, iframe_src):
        """Explore the iframe content by navigating to its source URL"""
        try:
            print(f"🔍 Exploring iframe content at: {iframe_src}")
            
            # Open the iframe URL in a new tab
            self.driver.execute_script(f"window.open('{iframe_src}', '_blank');")
            
            # Switch to the new tab
            self.driver.switch_to.window(self.driver.window_handles[-1])
            
            print("✅ Opened iframe content in new tab")
            
            # Wait for the new page to load
            time.sleep(3)
            
            return True
            
        except Exception as e:
            print(f"❌ Error exploring iframe content: {e}")
            return False

    def get_blind_levels_from_dropdown(self):
        """Find the dropdown-layer class and extract all blind level text from list elements"""
        try:
            print("🔍 Looking for dropdown-layer class...")
            
            # Wait for the dropdown to be present
            dropdown = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "dropdown-layer"))
            )
            print("✅ Found dropdown-layer element")
            
            # Wait a bit more for Angular content to load
            print("⏳ Waiting for Angular content to load...")
            time.sleep(3)
            
            # Find all list elements (li) within the dropdown
            list_elements = dropdown.find_elements(By.TAG_NAME, "li")
            print(f"📋 Found {len(list_elements)} list elements in dropdown")
            
            return list_elements
            
        except Exception as e:
            print(f"❌ Error extracting blind levels from dropdown: {e}")
            return []

    def click_through_blind_levels(self, list_elements):
        """Click through all blind level options to make them clickable"""
        try:
            print(f"🔄 Starting to click through {len(list_elements)} blind levels...")
            
            # Find the 'blind-text' class clickable element
            blind_text_element = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.CLASS_NAME, "blind-text"))
            )
            print(f"  ✅ Found 'blind-text' clickable element")
            
            dropdown = self.driver.find_element(By.CLASS_NAME, "dropdown-layer")
            for i, list_element in enumerate(list_elements[4:7]):
                try:
                    print(f"\n🖱️ Processing blind level {i}/{len(list_elements)}...")
                    
                    # Get the text content of the current list element
                    text_content = list_element.get_attribute('textContent').strip() if list_element.get_attribute('textContent') else ''
                    
                    # Click on it to open the dropdown
                    print(f"  🖱️ Clicking on 'blind-text' element...")
                    blind_text_element.click()
                    
                    # Wait a moment for the dropdown to open
                    time.sleep(1)
                    
                    # Check if the dropdown now has 'layer-open' class
                    classes = dropdown.get_attribute("class")
                    
                    if "layer-open" in classes:
                        print(f"  ✅ Dropdown opened successfully (has 'layer-open' class)")
                        print(f"  📋 List elements are now clickable")
                        # Click on the actual list element
                        print(f"  🖱️ Clicking on list element {i}...")

                        list_element.click()
                        time.sleep(5)

                        # Now extract player ranking data from the table
                        self.extract_player_ranking_data(text_content)
                        
                    else:
                        print(f"  ⚠️ Dropdown did not get 'layer-open' class")
                        print(f"  🔍 Current classes: {classes}")
                    
                    # Wait a bit before processing next element
                    time.sleep(2)
                    
                except Exception as e:
                    print(f"  ❌ Error processing blind level {i}: {e}")
                    continue
            
            print(f"\n✅ Completed clicking through all {len(list_elements)} blind levels")
            return True
            
        except Exception as e:
            print(f"❌ Error during blind level iteration: {e}")
            return False

    def extract_player_ranking_data(self, text_content, game='PLO'):
        """Extract player name and points from the ranking table and store in database"""
        try:
            print("    🔍 Looking for playerRankingBody class...")
            
            # Wait for the player ranking body to be present
            ranking_body = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "playerRankingBody"))
            )
            print("    ✅ Found playerRankingBody element")
            
            # Find all tr tags within the ranking body
            tr_elements = ranking_body.find_elements(By.TAG_NAME, "tr")
            print(f"    📋 Found {len(tr_elements)} table rows")
            
            # Get timestamp and blind level info
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
            print(f"  {timestamp} - {text_content}")
            
            # Extract blind level from text_content (e.g., "$0.01/$0.02")
            blind_level = text_content.strip()
            
            # Get or create the leaderboard table for this game and blind level
            table_name = self.db_manager.get_or_create_leaderboard_table(game, blind_level)
            print(f"    📊 Using table: {table_name}")
            
            # Add timestamp column to the table
            self.db_manager.add_timestamp_column(table_name, timestamp)
            
            # Process each player row in a single transaction for performance
            try:
                # Disable autocommit if enabled, to batch all updates
                if hasattr(self.db_manager, 'connection') and getattr(self.db_manager.connection, 'autocommit', False):
                    self.db_manager.connection.autocommit = False

                for i, tr in enumerate(tr_elements, 1):
                    try:
                        td_elements = tr.find_elements(By.TAG_NAME, "td")
                        if len(td_elements) >= 4:
                            player_name = td_elements[1].get_attribute('textContent').strip() if td_elements[1].get_attribute('textContent') else ''
                            points = td_elements[3].get_attribute('textContent').strip() if td_elements[3].get_attribute('textContent') else ''
                            if player_name and points:
                                self.db_manager.update_player_points(table_name, player_name, timestamp, points)
                            else:
                                print(f"      ⚠️ Row {i}: Missing name or points")
                        else:
                            print(f"      ⚠️ Row {i}: Not enough td elements ({len(td_elements)})")
                    except Exception as e:
                        print(f"      ❌ Error processing row {i}: {e}")
                        continue

                # Commit batched updates once
                self.db_manager.commit_changes()
                print(f"    ✅ Successfully processed {len(tr_elements)} player entries")
            except Exception as batch_err:
                print(f"    ❌ Batch update failed: {batch_err}")
                self.db_manager.rollback_changes()
            
        except Exception as e:
            print(f"    ❌ Error extracting player ranking data: {e}")
            
        except Exception as e:
            print(f"    ❌ Error extracting player ranking data: {e}")
            return []

    def run_scraping_session(self):
        """Run the complete scraping session"""
        try:
            print("🚀 Starting GGPoker scraping session...")
            
            # Step 1: Access the main page
            if not self.access_ggpoker_page():
                return False
            
            # Step 2: Find the PLO section and iframe
            iframe, iframe_src = self.find_plo_section()
            if not iframe:
                return False
            
            # Step 3: Click on the iframe
            if not self.click_iframe_link(iframe):
                return False
            
            # Step 4: Explore the iframe content
            if iframe_src:
                if not self.explore_iframe_content(iframe_src):
                    return False
            
            # Step 5: Extract blind levels from the dropdown
            print("\n🎯 Now extracting blind levels from dropdown...")
            list_elements = self.get_blind_levels_from_dropdown()
            
            if list_elements:
                print(f"\n🎉 Successfully found {len(list_elements)} list elements!")
                
                # Step 6: Click through all blind levels to make them clickable
                print("\n🔄 Now clicking through all blind levels...")
                if self.click_through_blind_levels(list_elements):
                    print("✅ Successfully clicked through all blind levels")
                else:
                    print("⚠️ Some issues occurred while clicking through blind levels")
                
            else:
                print("⚠️ No list elements found in dropdown")
            
            print("\n✅ Scraping session completed successfully!")
            print("\n📋 Next steps:")
            print("1. Blind levels have been extracted and printed above")
            print("2. You can manually explore the iframe content further")
            print("3. Let me know what data you want to extract next")
            
            return True
            
        except Exception as e:
            print(f"❌ Error during scraping session: {e}")
            return False
    
    def close(self):
        """Close the browser and database connection"""
        if self.driver:
            self.driver.quit()
            print("🔒 Browser closed")
        
        if hasattr(self, 'db_manager'):
            self.db_manager.close()


def main():
    """Main function to run the scraper"""
    parser = argparse.ArgumentParser(description="Run the GGPoker scraper")
    parser.add_argument("--headless", action="store_true", help="Run Chrome in headless mode")
    default_interval = int(os.getenv("INTERVAL", "0"))
    parser.add_argument("--interval", type=int, default=default_interval, help="Seconds between runs; 0 runs once")
    args = parser.parse_args()

    def run_once(headless_flag: bool) -> bool:
        scraper = None
        try:
            scraper = GGPokerScraper(headless=headless_flag)
            success = scraper.run_scraping_session()
            return success
        except KeyboardInterrupt:
            raise
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            return False
        finally:
            if scraper:
                scraper.close()

    if args.interval and args.interval > 0:
        print(f"🔁 Running every {args.interval} seconds. Press Ctrl+C to stop.")
        try:
            while True:
                start_ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print(f"\n⏱️ {start_ts} - Starting run")
                run_once(args.headless)
                print(f"⏳ Sleeping {args.interval} seconds...")
                time.sleep(args.interval)
        except KeyboardInterrupt:
            print("\n⏹️ Stopped by user")
    else:
        run_once(args.headless)

if __name__ == "__main__":
    main()

# docker-compose down
# DOCKER_BUILDKIT=0 docker-compose up -d --build