import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime
import os
import logging
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv
import sys
import subprocess

# Load environment variables
load_dotenv()
ALPHA_VANTAGE_API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY')

# Setup paths and logging
script_dir = os.path.dirname(os.path.abspath(__file__))
log_file = os.path.join(script_dir, 'update_missing_info.log')
csv_file = os.path.join(script_dir, 'insider_buys.csv')

# Configure logger with custom RotatingFileHandler
class CustomRotatingFileHandler(RotatingFileHandler):
    def __init__(self, filename, mode='a', max_entries=50, backup_count=1, encoding=None):
        self.max_entries = max_entries
        super().__init__(filename, mode=mode, maxBytes=0, backupCount=backup_count, encoding=encoding)
    
    def emit(self, record):
        try:
            if os.path.exists(self.baseFilename):
                with open(self.baseFilename, 'r') as f:
                    lines = f.readlines()
                if len(lines) >= self.max_entries:
                    with open(self.baseFilename, 'w') as f:
                        f.writelines(lines[-(self.max_entries-1):])
            super().emit(record)
        except Exception:
            self.handleError(record)

# Setup logger
logger = logging.getLogger('update_missing')
logger.setLevel(logging.INFO)

# Clear any existing handlers
if logger.handlers:
    logger.handlers.clear()

# Add new handler with 50 entry limit
handler = CustomRotatingFileHandler(
    log_file,
    mode='a',
    max_entries=50,
    backup_count=1
)
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

def fetch_price_from_alpha_vantage(ticker, filing_datetime):
    try:
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={ticker}&interval=5min&apikey={ALPHA_VANTAGE_API_KEY}&outputsize=full"
        response = requests.get(url)
        data = response.json()
        
        if 'Time Series (5min)' in data:
            time_series = data['Time Series (5min)']
            filing_str = filing_datetime.strftime('%Y-%m-%d %H:%M:00')
            
            if filing_str in time_series:
                return float(time_series[filing_str]['4. close'])
            
            # Find closest time before filing
            closest_time = None
            for timestamp in time_series:
                dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:00')
                if dt <= filing_datetime:
                    closest_time = timestamp
                    break
            
            if closest_time:
                return float(time_series[closest_time]['4. close'])
    except Exception as e:
        logger.error(f"Error fetching price for {ticker}: {str(e)}")
    return None

def fetch_transaction_price_from_openinsider(ticker, filing_date_str):
    try:
        url = "http://openinsider.com/insider-purchases"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table', {'class': 'tinytable'})
        
        if table:
            for row in table.find_all('tr')[1:]:
                cols = row.find_all('td')
                if len(cols) > 0:
                    row_filing_date = cols[1].text.strip()
                    row_ticker = cols[3].text.strip()
                    
                    if ticker == row_ticker and filing_date_str in row_filing_date:
                        price = cols[8].text.strip()
                        if price:
                            return float(price.replace('$', '').replace(',', ''))
        return None
    except Exception as e:
        logger.error(f"Error fetching transaction price for {ticker}: {str(e)}")
        return None

def update_missing_info():
    try:
        # Read the CSV file
        df = pd.read_csv(csv_file)
        df['Filing Date'] = pd.to_datetime(df['Filing Date'])
        modified = False
        
        # Create a backup of the original file
        backup_file = csv_file.replace('.csv', '_backup.csv')
        df.to_csv(backup_file, index=False)
        logger.info(f"Created backup at {backup_file}")
        
        # Process each row
        for index, row in df.iterrows():
            updates_made = False
            filing_date_str = row['Filing Date'].strftime('%Y-%m-%d %H:%M:%S')
            
            # Check for missing Transaction Price
            if pd.isna(row['Transaction Price']):
                logger.info(f"Fetching transaction price for {row['Ticker']} ({filing_date_str})")
                price = fetch_transaction_price_from_openinsider(row['Ticker'], filing_date_str)
                if price is not None:
                    df.loc[index, 'Transaction Price'] = price
                    updates_made = True
                    modified = True
                    logger.info(f"Updated transaction price for {row['Ticker']}: {price}")
                time.sleep(2)  # Rate limiting
            
            # Check for missing Price Bought
            if pd.isna(row['Price Bought']):
                logger.info(f"Missing bought price for {row['Ticker']} on {row['Filing Date']}")
                price = fetch_price_from_alpha_vantage(row['Ticker'], row['Filing Date'])
                if price:
                    df.at[index, 'Price Bought'] = price
                    logger.info(f"Updated bought price for {row['Ticker']}: {price}")
                time.sleep(12)  # Alpha Vantage rate limiting
            
            if updates_made:
                # Save after each successful update
                df.to_csv(csv_file, index=False)
                logger.info(f"Saved updates for {row['Ticker']}")
        
        if modified:
            # Final save and verification
            df.to_csv(csv_file, index=False)
            logger.info("All updates completed and saved")
            
            # Verify the save worked
            verification_df = pd.read_csv(csv_file)
            logger.info(f"Verification: CSV file has {len(verification_df)} rows")
        else:
            logger.info("No updates were needed")
            
    except Exception as e:
        logger.error(f"Error during update process: {str(e)}")
        # Restore from backup if something went wrong
        if os.path.exists(backup_file):
            os.replace(backup_file, csv_file)
            logger.info("Restored from backup due to error")
        raise

if __name__ == "__main__":
    # Check if running from GitHub Actions
    is_github_action = os.getenv('GITHUB_ACTIONS') == 'true'
    
    if not is_github_action and sys.platform == 'win32':
        # If running locally on Windows, run in hidden window
        CREATE_NO_WINDOW = 0x08000000
        subprocess.call(['python', __file__], creationflags=CREATE_NO_WINDOW)
    else:
        # Normal execution
        logger.info("Starting update process...")
        update_missing_info()
        logger.info("Update process completed") 