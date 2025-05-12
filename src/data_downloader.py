"""
This script provides functionality to download GitHub Archive data files.

It includes a class `GHArchiveDownloader` that handles downloading individual files
or a range of files based on specified dates and hours. The script can be executed
as a standalone program to download data by providing command-line arguments.
"""

import argparse
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class GHArchiveDownloader:
    """A downloader for GitHub Archive data files."""
    
    BASE_URL = "https://data.gharchive.org"
    
    def __init__(self, output_dir):
        """
        Initialize the downloader with the specified output directory.

        Args:
            output_dir (str): The directory where downloaded files will be saved.
        """
        self.DOWNLOAD_DIR = output_dir
        # Create download directory if it doesn't exist
        Path(self.DOWNLOAD_DIR).mkdir(parents=True, exist_ok=True)
    
    def download_file(self, date_str):
        """
        Download a single file for a specific date and hour.

        Args:
            date_str (str): The date and hour in the format 'YYYY-MM-DD-H'.

        Returns:
            bool: True if the file was downloaded successfully or already exists, False otherwise.
        """
        filename = f"{date_str}.json.gz"
        url = f"{self.BASE_URL}/{filename}"
        output_path = os.path.join(self.DOWNLOAD_DIR, filename)
        
        # Check if file already exists
        if os.path.exists(output_path):
            logger.info(f"File already exists, skipping: {filename}")
            return True
            
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()  # Raise an exception for HTTP errors
            
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            logger.info(f"Successfully downloaded: {filename}")
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download {filename}: {str(e)}")
            return False
    
    def download_date_range(self, start_date, end_date):
        """
        Download files for a range of dates and hours.

        Args:
            start_date (str): The start date in 'YYYY-MM-DD' format.
            end_date (str): The end date in 'YYYY-MM-DD' format.
        """
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        
        failed_downloads = []
        current_date = start
        
        while current_date <= end:
            for hour in range(24):  # Iterate through all 24 hours
                date_str = current_date.strftime("%Y-%m-%d") + f"-{hour}"
                if not self.download_file(date_str):
                    failed_downloads.append(date_str)
            current_date += timedelta(days=1)
        
        if failed_downloads:
            logger.warning(f"Failed to download files for the following dates and hours: {', '.join(failed_downloads)}")
        else:
            logger.info("All files downloaded successfully!")

def main():
    """
    Parse command-line arguments and initiate the download process.
    """
    parser = argparse.ArgumentParser(description='Download GitHub Archive data')
    parser.add_argument('--start-date', required=True, help='Start date in YYYY-MM-DD format')
    parser.add_argument('--end-date', required=True, help='End date in YYYY-MM-DD format')
    parser.add_argument('--output-dir', default='data/raw', help='Output directory for downloaded files')
    
    args = parser.parse_args()
    
    downloader = GHArchiveDownloader(args.output_dir)
    logger.info(f"Starting download from {args.start_date} to {args.end_date}")
    downloader.download_date_range(args.start_date, args.end_date)

if __name__ == "__main__":
    main()