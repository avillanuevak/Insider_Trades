import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import logging
import os
import time
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
ALPHA_VANTAGE_API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY')

# Setup paths
script_dir = os.path.dirname(os.path.abspath(__file__))
log_file = os.path.join(script_dir, 'insider_scraping.log')
csv_file = os.path.join(script_dir, 'insider_buys.csv')

# Custom RotatingFileHandler that keeps last N lines
class LineCountRotatingFileHandler(logging.Handler):
    def __init__(self, filename, max_lines=100):
        super().__init__()
        self.filename = filename
        self.max_lines = max_lines

    def emit(self, record):
        try:
            # Read existing logs
            if os.path.exists(self.filename):
                with open(self.filename, 'r') as f:
                    lines = f.readlines()
            else:
                lines = []

            # Add new log entry
            msg = self.format(record)
            lines.append(f"{msg}\n")

            # Keep only last max_lines
            lines = lines[-self.max_lines:]

            # Write back to file
            with open(self.filename, 'w') as f:
                f.writelines(lines)
        except Exception:
            self.handleError(record)

# Configure logging
logger = logging.getLogger('insider_scraper')
logger.setLevel(logging.INFO)

# Clear any existing handlers
if logger.handlers:
    logger.handlers.clear()

# Add new handler with 100 line limit
handler = LineCountRotatingFileHandler(log_file, max_lines=100)
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

def get_price_at_filing(ticker, filing_datetime):
    try:
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={ticker}&interval=1min&apikey={ALPHA_VANTAGE_API_KEY}&outputsize=full"
        response = requests.get(url)
        data = response.json()
        
        if 'Time Series (1min)' in data:
            time_series = data['Time Series (1min)']
            filing_str = filing_datetime.strftime('%Y-%m-%d %H:%M:00')
            
            # Convert time series keys to datetime for accurate comparison
            times = []
            for timestamp in time_series.keys():
                try:
                    dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:00')
                    if dt <= filing_datetime:
                        times.append((dt, timestamp))
                except ValueError:
                    continue
            
            if times:
                # Sort by time difference to filing time
                times.sort(key=lambda x: abs((filing_datetime - x[0]).total_seconds()))
                closest_time = times[0][1]
                
                # Log the time difference for monitoring
                time_diff = abs((filing_datetime - times[0][0]).total_seconds())
                if time_diff > 300:  # If difference is more than 5 minutes
                    logger.warning(f"Price for {ticker} found {time_diff/60:.1f} minutes from filing time")
                
                return float(time_series[closest_time]['4. close'])
            
            logger.warning(f"No valid price found for {ticker} at {filing_datetime}")
            return None
            
        else:
            # Try backup method with 5min intervals if 1min not available
            url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={ticker}&interval=5min&apikey={ALPHA_VANTAGE_API_KEY}&outputsize=full"
            response = requests.get(url)
            data = response.json()
            
            if 'Time Series (5min)' in data:
                time_series = data['Time Series (5min)']
                times = []
                for timestamp in time_series.keys():
                    try:
                        dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:00')
                        if dt <= filing_datetime:
                            times.append((dt, timestamp))
                    except ValueError:
                        continue
                
                if times:
                    times.sort(key=lambda x: abs((filing_datetime - x[0]).total_seconds()))
                    closest_time = times[0][1]
                    
                    time_diff = abs((filing_datetime - times[0][0]).total_seconds())
                    if time_diff > 300:
                        logger.warning(f"Price for {ticker} found {time_diff/60:.1f} minutes from filing time (5min data)")
                    
                    return float(time_series[closest_time]['4. close'])
            
            logger.warning(f"No price data available for {ticker}")
            return None
            
    except Exception as e:
        logger.error(f"Error fetching price for {ticker}: {str(e)}")
        return None

def scrape_insider_buys():
    try:
        # Load existing data
        existing_df = pd.read_csv(csv_file) if os.path.exists(csv_file) else pd.DataFrame()
        if not existing_df.empty:
            existing_df['Filing Date'] = pd.to_datetime(existing_df['Filing Date'])
        
        # Scrape OpenInsider
        url = "http://openinsider.com/insider-purchases"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table', {'class': 'tinytable'})
        
        new_data = []
        for row in table.find_all('tr')[1:]:
            try:
                cols = row.find_all('td')
                if len(cols) > 0:
                    filing_date = cols[1].text.strip()
                    filing_datetime = datetime.strptime(filing_date, '%Y-%m-%d %H:%M:%S')
                    
                    if filing_datetime > datetime.now():
                        continue
                    
                    ticker = cols[3].text.strip()
                    company = cols[4].text.strip()
                    insider = cols[5].text.strip()
                    trans_price = float(cols[8].text.strip().replace('$', '').replace(',', '')) if cols[8].text.strip() else None
                    value = float(cols[12].text.strip().replace('$', '').replace('+', '').replace(',', ''))
                    
                    if value >= 500000:
                        if existing_df.empty or not ((existing_df['Ticker'] == ticker) & 
                            (existing_df['Filing Date'] == filing_datetime)).any():
                            
                            logger.info(f"Fetching price for {ticker} at {filing_datetime}")
                            price_bought = get_price_at_filing(ticker, filing_datetime)
                            
                            if price_bought:
                                new_data.append({
                                    'Filing Date': filing_date,
                                    'Ticker': ticker,
                                    'Company Name': company,
                                    'Insider Name': insider,
                                    'Transaction Price': trans_price,
                                    'Price Bought': price_bought,
                                    'Value': int(round(value))
                                })
                                # Log successful price fetch
                                logger.info(f"Successfully fetched price {price_bought} for {ticker}")
                                # Alpha Vantage rate limit
                                time.sleep(12)
                            else:
                                logger.warning(f"Could not get accurate price for {ticker}")
                                
            except Exception as e:
                logger.error(f"Error processing row: {str(e)}")
                continue
        
        # Process and save new data
        if new_data:
            new_df = pd.DataFrame(new_data)
            new_df['Filing Date'] = pd.to_datetime(new_df['Filing Date'])
            
            if not existing_df.empty:
                combined_df = pd.concat([existing_df, new_df])
                combined_df = combined_df.drop_duplicates(
                    subset=['Filing Date', 'Ticker', 'Insider Name'],
                    keep='first'
                )
                combined_df = combined_df.sort_values('Filing Date', ascending=False)
                combined_df.to_csv(csv_file, index=False)
                logger.info(f"Added {len(new_data)} new entries")
            else:
                new_df.to_csv(csv_file, index=False)
                logger.info(f"Created new file with {len(new_data)} entries")
            
            return True
        
        logger.info("No new entries found")
        return False
        
    except Exception as e:
        logger.error(f"Error in main scraping function: {str(e)}")
        return False

if __name__ == "__main__":
    logger.info("Starting scraping process...")
    scrape_insider_buys()
    logger.info("Scraping process completed")
