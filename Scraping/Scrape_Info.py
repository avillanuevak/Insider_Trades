import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import logging
import yfinance as yf
import os

# Use absolute paths based on script location
script_dir = os.path.dirname(os.path.abspath(__file__))
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
                ticker = cols[3].text.strip()
                company_name = cols[4].text.strip()
                transaction_price = float(cols[8].text.strip().replace('$', '').replace(',', ''))
                value = float(cols[12].text.strip().replace('$', '').replace('+', '').replace(',', ''))
                
                # Only process if value is over $500,000
                if value >= 500000:
                    try:
                        stock = yf.Ticker(ticker)
                        
                        # Get historical price at filing date only
                        filing_datetime = datetime.strptime(filing_date, '%Y-%m-%d %H:%M:%S')
                        historical_data = stock.history(start=filing_datetime.strftime('%Y-%m-%d'), 
                                                      end=(filing_datetime + pd.Timedelta(days=1)).strftime('%Y-%m-%d'))
                        price_bought = historical_data['Close'].iloc[0] if not historical_data.empty else None
                        
                    except Exception as e:
                        logger.error(f"Error fetching prices for {ticker}: {str(e)}")
                        price_bought = None
                    
                    data.append({
                        'Filing Date': filing_date,
                        'Ticker': ticker,
                        'Company Name': company_name,
                        'Transaction Price': transaction_price,
                        'Price Bought': price_bought,
                        'Value': value
                    })
        
        # Create DataFrame from new data
        new_df = pd.DataFrame(data)
        
        if not existing_df.empty:
            # Convert Filing Date to datetime for comparison
            new_df['Filing Date'] = pd.to_datetime(new_df['Filing Date'])
            
            # Combine existing and new data
            combined_df = pd.concat([existing_df, new_df])
            
            # Remove duplicates based on Filing Date, Ticker, and Transaction Price
            combined_df = combined_df.drop_duplicates(
                subset=['Filing Date', 'Ticker', 'Transaction Price'], 
                keep='last'
            )
            
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
