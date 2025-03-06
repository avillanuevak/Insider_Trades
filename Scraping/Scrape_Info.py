import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import logging
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Determine the script directory
try:
    script_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    script_dir = os.getcwd()  # Use current working directory if running in a Jupyter Notebook

log_file = os.path.join(script_dir, 'insider_scraping.log')
csv_file = os.path.join(script_dir, 'insider_buys.csv')

# Set up logging with FileHandler to ensure append mode
logger = logging.getLogger('insider_scraper')
logger.setLevel(logging.INFO)

# Check if logger already has handlers to avoid duplicate handlers
if not logger.handlers:
    handler = logging.FileHandler(log_file, mode='a')
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# Add console handler
if not logger.handlers:
    # File handler (existing code)
    file_handler = logging.FileHandler(log_file, mode='a')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

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
        # Create or load existing CSV
        if os.path.exists(csv_file):
            existing_df = pd.read_csv(csv_file)
            # Convert Filing Date to datetime for comparison
            existing_df['Filing Date'] = pd.to_datetime(existing_df['Filing Date'])
        else:
            existing_df = pd.DataFrame()
        
        url = "http://openinsider.com/insider-purchases"
        
        # Add headers to mimic a browser request
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # Make the request
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        # Parse the HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find the main table
        table = soup.find('table', {'class': 'tinytable'})
        
        # Initialize lists to store data
        data = []
        
        # Process each row
        for row in table.find_all('tr')[1:]:  # Skip header row
            cols = row.find_all('td')
            if len(cols) > 0:
                filing_date = cols[1].text.strip()
                # Verify the date format and ensure it's not in the future
                try:
                    filing_datetime = datetime.strptime(filing_date, '%Y-%m-%d %H:%M:%S')
                    if filing_datetime > datetime.now():
                        logger.error(f"Future date detected: {filing_date}, skipping entry")
                        continue
                except ValueError as e:
                    logger.error(f"Date parsing error: {e}")
                    continue
                    
                ticker = cols[3].text.strip()
                company_name = cols[4].text.strip()
                insider_name = cols[5].text.strip()
                transaction_price = float(cols[8].text.strip().replace('$', '').replace(',', ''))
                value = float(cols[12].text.strip().replace('$', '').replace('+', '').replace(',', ''))
                
                # Only process if value is over $500,000
                if value >= 500000:
                    data.append({
                        'Filing Date': filing_date,
                        'Ticker': ticker,
                        'Company Name': company_name,
                        'Insider Name': insider_name,  # Add insider name to the data
                        'Transaction Price': transaction_price,
                        'Price Bought': None,  # Placeholder for Price Bought
                        'Value': int(round(value))  # Round value to integer
                    })
        
        # Create DataFrame from new data
        new_df = pd.DataFrame(data)
        
        if not existing_df.empty:
            # Convert Filing Date to datetime for comparison
            new_df['Filing Date'] = pd.to_datetime(new_df['Filing Date'])
            
            # Exclude empty or all-NA columns before concatenation
            existing_df = existing_df.dropna(axis=1, how='all')
            new_df = new_df.dropna(axis=1, how='all')
            
            # Combine existing and new data
            combined_df = pd.concat([existing_df, new_df])
            
            # Keep only the first transaction per company per day
            combined_df['Filing_Date_Only'] = combined_df['Filing Date'].dt.date
            combined_df = combined_df.sort_values('Filing Date', ascending=True)  # Sort ascending to keep first
            combined_df = combined_df.drop_duplicates(
                subset=['Filing_Date_Only', 'Company Name'],
                keep='first'
            )
            combined_df = combined_df.drop('Filing_Date_Only', axis=1)  # Remove helper column
            
            # Sort by Filing Date (most recent first)
            combined_df = combined_df.sort_values('Filing Date', ascending=False)
            
            # Save combined data
            combined_df.to_csv(csv_file, index=False)
            logger.info(f"Updated existing file with {len(new_df)} new entries")
        else:
            # If no existing file, save new data
            new_df.to_csv(csv_file, index=False)
            logger.info(f"Created new file with {len(new_df)} entries")
        
        return combined_df if not existing_df.empty else new_df
        
    except Exception as e:
        logger.error(f"Error scraping website: {str(e)}")
        raise

if __name__ == "__main__":
    df = scrape_insider_buys()
    print(df)
