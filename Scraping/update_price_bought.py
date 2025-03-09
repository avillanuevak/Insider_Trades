import requests
import pandas as pd
from datetime import datetime
import logging
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Alpha Vantage API key
ALPHA_VANTAGE_API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY')

def fetch_price(ticker, filing_datetime):
    try:
        # Try to fetch intraday data first
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={ticker}&interval=1min&apikey={ALPHA_VANTAGE_API_KEY}&outputsize=full"
        response = requests.get(url)
        data = response.json()
        
        if 'Time Series (1min)' in data:
            time_series = data['Time Series (1min)']
            
            # Convert time series keys to datetime for accurate comparison
            times = []
            for timestamp in time_series.keys():
                try:
                    dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:00')
                    times.append((dt, timestamp))
                except ValueError as e:
                    logging.error(f"Error parsing timestamp {timestamp}: {e}")
            
            # Find the closest price to the filing time
            if times:
                closest_time = min(times, key=lambda x: abs(x[0] - filing_datetime))
                price = time_series[closest_time[1]]['4. close']
                return float(price)  # Explicitly cast to float
            else:
                logging.warning(f"No valid timestamps found for {ticker} at {filing_datetime}")
                return None
        else:
            logging.warning(f"No 'Time Series (1min)' data found for {ticker}, trying daily data")
            # Try to fetch daily data if intraday data is not available
            url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={ticker}&apikey={ALPHA_VANTAGE_API_KEY}&outputsize=full"
            response = requests.get(url)
            data = response.json()
            
            if 'Time Series (Daily)' in data:
                time_series = data['Time Series (Daily)']
                
                # Convert time series keys to datetime for accurate comparison
                times = []
                for timestamp in time_series.keys():
                    try:
                        dt = datetime.strptime(timestamp, '%Y-%m-%d')
                        times.append((dt, timestamp))
                    except ValueError as e:
                        logging.error(f"Error parsing timestamp {timestamp}: {e}")
                
                # Find the closest price to the filing time
                if times:
                    closest_time = min(times, key=lambda x: abs(x[0] - filing_datetime))
                    price = time_series[closest_time[1]]['4. close']
                    return float(price)  # Explicitly cast to float
                else:
                    logging.warning(f"No valid timestamps found for {ticker} at {filing_datetime}")
                    return None
            else:
                logging.warning(f"No 'Time Series (Daily)' data found for {ticker}")
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
                df.at[index, 'Price Bought'] = price
                logging.info(f"Updated 'Price Bought' for {ticker} at {filing_datetime} with price {price}")
            else:
                logging.warning(f"Could not fetch price for {ticker} at {filing_datetime}")
        
        # Save the updated DataFrame back to the CSV file
        df.to_csv(csv_file, index=False)
    except Exception as e:
        logging.error(f"Error updating prices: {e}")

if __name__ == "__main__":
    csv_file = 'c:/Users/Albert/OneDrive/Desktop/Info/Projects/Insider_Trades/Scraping/insider_buys.csv'
    update_missing_prices(csv_file)