import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import logging
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_price(ticker, filing_datetime):
    try:
        stock = yf.Ticker(ticker)
        data = stock.history(start=filing_datetime.strftime('%Y-%m-%d'), end=(filing_datetime + timedelta(days=1)).strftime('%Y-%m-%d'), interval='1m')
        
        if not data.empty:
            # Find the closest price to the filing time
            data.index = data.index.tz_localize(None)  # Ensure the index is timezone-naive
            closest_time = min(data.index, key=lambda x: abs(x - filing_datetime))
            price = data.loc[closest_time]['Close']
            return float(price)  # Explicitly cast to float
        else:
            logging.warning(f"No intraday data found for {ticker} at {filing_datetime}, trying daily data")
            # Try to fetch daily data if intraday data is not available
            data = stock.history(start=filing_datetime.strftime('%Y-%m-%d'), end=(filing_datetime + timedelta(days=1)).strftime('%Y-%m-%d'))
            
            if not data.empty:
                price = data.iloc[0]['Close']
                return float(price)  # Explicitly cast to float
            else:
                logging.warning(f"No daily data found for {ticker} at {filing_datetime}")
                return None
    except Exception as e:
        logging.error(f"Error getting price for {ticker} at {filing_datetime}: {e}")
        return None

def update_missing_prices(csv_file):
    try:
        # Read the CSV file
        df = pd.read_csv(csv_file)
        
        # Iterate through rows with missing 'Price Bought'
        for index, row in df[df['Price Bought'].isna()].iterrows():
            ticker = row['Ticker']
            filing_datetime = pd.to_datetime(row['Filing Date'])
            
            # Fetch the price
            price = fetch_price(ticker, filing_datetime)
            
            if price:
                # Update the DataFrame
                df.at[index, 'Price Bought'] = round(price, 2)  # Round to 2 decimal places
                logging.info(f"Updated 'Price Bought' for {ticker} at {filing_datetime} with price {round(price, 2)}")
            else:
                logging.warning(f"Could not fetch price for {ticker} at {filing_datetime}")
        
        # Save the updated DataFrame back to the CSV file
        df.to_csv(csv_file, index=False)
    except Exception as e:
        logging.error(f"Error updating prices: {e}")

if __name__ == "__main__":
    csv_file = 'c:/Users/Albert/OneDrive/Desktop/Info/Projects/Insider_Trades/Scraping/insider_buys.csv'
    update_missing_prices(csv_file)