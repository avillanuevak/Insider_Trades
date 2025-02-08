import os
import sys
from datetime import datetime, timedelta
import subprocess

def create_windows_task(interval_minutes=30):
    # Get the absolute path to the python script
    script_path = os.path.abspath("Scraping/Scrape_Info.py")
    python_path = sys.executable
    
    # Create the command with wake computer and catch-up behavior
    command = (
        f'schtasks /create /tn "InsiderTradesScraper" '
        f'/tr "{python_path} {script_path}" '
        f'/sc minute /mo {interval_minutes} '
        '/f '  # Force creation/overwrite
        '/wake '  # Wake computer to run task
        '/du 00:05 '  # Expected duration (5 minutes)
        '/ri 5 '  # Retry every 5 minutes if failed
        '/rp 24:00 '  # Retry for 24 hours
    )
    
    # Execute the command
    subprocess.run(command, shell=True)

if __name__ == "__main__":
    create_windows_task() 