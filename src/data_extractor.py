import requests
from bs4 import BeautifulSoup
import re
import constants as c
import os
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from datetime import datetime


def get_links(dataset_pattern):
    """
    Fetches links from a web page based on the specified pattern.

    This function performs a GET request to a specific URL, parses the HTML content,
    and finds all <a> elements with href attributes matching the given dataset pattern.

    Parameters:
    -----------
    dataset_pattern : str
        A regular expression pattern used to filter and find specific dataset links.

    Returns:
    --------
    links : list
        A list of BeautifulSoup <a> elements containing the desired links.
    """
    # Perform a GET request to the web page
    response = requests.get(c.DATASET.URL)

    # Parse the HTML content
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find all <a> elements with href attributes matching the dataset pattern
    links = soup.find_all('a', href=re.compile(dataset_pattern))

    return links


def download_file(file_url: str, file_path: str, verbose: bool) -> None:
    """
    Downloads a file from the given URL to the specified file path.

    This function performs a GET request to the file URL and saves the content
    to the provided file path.

    Parameters:
    ----------
    file_url : str
        The URL of the file to be downloaded.
    file_path : str
        The path where the file will be saved.
    """
    file_response = requests.get(file_url)  # Perform a GET request to download the file
    with open(file_path, 'wb') as file:
        file.write(file_response.content)
        if verbose:
            print(f"File downloaded from {file_url} to {file_path}.")



def convert_str_date_to_date(date: str) -> datetime.date:
    """
    Converts a string representing a date in the format 'day-month-year' to a datetime.date object.

    Args:
    - date: String representing a date in the format 'day-month-year' (e.g., '21-11-2023')

    Returns:
    - datetime.date: Date object representing the converted date

    Raises:
    - ValueError: If the input date does not match the specified format 'day-month-year'
    """
    try:
        datetime_obj = datetime.strptime(date, "%d-%m-%Y").date()
        return datetime_obj
    except ValueError:
        raise ValueError(f"The date format should be 'day-month-year' (e.g., '21-11-2023'). Received {date}. Please check the format and try again.")



def generate_dataset(dataset_pattern: str, start_date: str, end_date: str, verbose=False) -> None:
    """
    Extracts and downloads data from provided links in parallel.

    This function fetches links, identifies downloadable content based on the provided dataset pattern,
    creates necessary directories, and downloads files from the given links in a parallel manner using ThreadPoolExecutor.

    Parameters:
    -----------
    dataset_pattern : str
        A regular expression pattern used to identify and filter specific dataset links.

    verbose : bool, optional
        If True, provides additional verbose output during the process. Default is False.

    Returns:
    --------
    None

    Example:
    --------
    generate_dataset(r'your_pattern_here', verbose=True)
    """

    links = get_links(dataset_pattern)  # Fetch links from a source
    # create dates to filter files
    start_date = convert_str_date_to_date(start_date)
    end_date = convert_str_date_to_date(end_date)


    # Use ThreadPoolExecutor for parallel execution based on available CPU cores
    num_threads = os.cpu_count()
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []  # List to hold concurrent tasks
        for link in links:
            file_url = link.get('href')  # Get the download link
            match = re.search(dataset_pattern, file_url)
            if match:
                dataset_name = match.group(1)
                year = match.group(2)
                month = match.group(3)

                # create datetime to filter
                file_date = convert_str_date_to_date(f"01-{month}-{year}")

                if start_date <= file_date <= end_date:
                    # Create directory for the year if it doesn't exist
                    year_directory = f"{c.DATASET.ROOT_DIR}/{dataset_name}/{year}"
                    os.makedirs(year_directory, exist_ok=True)

                    # Define the complete file path for download
                    file_path = f"{year_directory}/{month}.parquet"

                    # Download the file if it doesn't exist in the directory
                    if not os.path.exists(file_path):
                        # Creation of the future
                        future_task = executor.submit(download_file, file_url, file_path, verbose)
                        # Addition of the future to the list
                        futures.append(future_task)

                    elif verbose:
                            print(f"The file corresponding to {file_url} already exists at {file_path}.")
                elif verbose:
                    print(f"file {dataset_name}_{year}_{month}.parquet filtered out because {start_date} > {file_date} or  {file_date} > {end_date}")
            else:
                print(f"Link pattern no covered by {dataset_pattern}")

        # Wait for all futures (download tasks) to complete
        if num_threads < c.JOKES.NUM_THREADS:
            print(f"Maybe you should replace your pc, you only have {num_threads} threads available, which will make this process slower...")
        for future in tqdm(futures, desc=f"Downloading data in parallel using {num_threads} threads to build {dataset_name} dataset"):
            future.result()

