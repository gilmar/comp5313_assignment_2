# Complex Systems Analysis Project

This project analyzes complex systems using network analysis and data processing tools.

## Prerequisites

1. Python 3.7 or higher
2. [mamba](https://github.com/mamba-org/mamba) package manager installed

## Setting Up the Environment

This project uses `mamba` as the package manager. To set up the environment, follow these steps:

1. Install `mamba` if you don't already have it:
   ```bash
   conda install -n base -c conda-forge mamba
   ```

2. Create the environment from the `env.yml` file:
   ```bash
   mamba env create -f env.yml
   ```

3. Activate the environment:
   ```bash
   mamba activate comp5313_assignment_2
   ```

4. Install JupyterLab in the environment:
   ```bash
   mamba install jupyterlab
   ```

5. Run JupyterLab:
   ```bash
   mamba run jupyter-lab
   ```

## Project Structure

- `src/`: Contains the main source code
- `data/`: Contains data files for analysis
- `research/`: Contains research materials and documentation

## Dependencies

The project uses the following main packages:
- NetworkX: For network analysis
- Polars: For efficient data processing

## Data Downloader

The project includes a data downloader script that fetches GitHub Archive data. To run it:

1. Make sure you have the required dependencies installed:
   ```bash
   mamba install requests
   ```

2. Run the downloader script with the required parameters:
   ```bash
   python src/data_downloader.py --start-date YYYY-MM-DD --end-date YYYY-MM-DD [--output-dir OUTPUT_DIR]
   ```

Parameters:
- `--start-date`: Start date in YYYY-MM-DD format (required)
- `--end-date`: End date in YYYY-MM-DD format (required)
- `--output-dir`: Output directory for downloaded files (optional, defaults to 'data/raw')

Example:
```bash
python src/data_downloader.py --start-date 2015-01-01 --end-date 2015-01-31 --output-dir data/raw
```

The script will:
- Download hourly GitHub Archive data for the specified date range
- Store the files in the specified output directory
- Skip any files that already exist
- Log successful downloads and any failures to the console

## Downloading and Processing GitHub Archive Files

This project includes scripts to download and process GitHub Archive data efficiently.

### Step 1: Generate Download URLs

Use the `generate-gharchive-urls.sh` script to generate a list of URLs for the desired date range.

```bash
bash scripts/generate-gharchive-urls.sh <start_year> <end_year> <output_dir>
```

- `<start_year>`: The starting year of the data range (e.g., 2018).
- `<end_year>`: The ending year of the data range (e.g., 2022).
- `<output_dir>`: Directory to save the generated URL files.

Example:
```bash
bash scripts/generate-gharchive-urls.sh 2018 2022 data/urls
```

### Step 2: Download Files Using `aria2c`

Install `aria2c` if not already installed:
```bash
mamba install -c conda-forge aria2
```

Download the files using the generated URL lists:
```bash
aria2c -i <url_file> -d <output_dir>
```

- `<url_file>`: Path to the file containing the list of URLs (e.g., `data/urls/download-gharchive-2018.txt`).
- `<output_dir>`: Directory to save the downloaded files.

Example:
```bash
aria2c -i data/urls/download-gharchive-2018.txt -d data/raw
```

### Step 3: Process the Downloaded Files

Use the `run_duckdb_monthly.sh` script to process the downloaded files and generate CSV outputs.

```bash
bash scripts/run_duckdb_monthly.sh <year>
```

- `<year>`: The year of the data to process (e.g., 2018).

Example:
```bash
bash scripts/run_duckdb_monthly.sh 2018
```

This script will:
- Process the JSON files for each month of the specified year.
- Generate CSV files in the output directory (default: `~/gharchive`).
- Automatically handle different JSON schemas based on the year.