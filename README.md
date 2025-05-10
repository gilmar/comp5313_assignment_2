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