# Weather Forecast Heatmap Generator

A Python script that processes RAR files containing weather forecast data and generates comparative heatmaps showing RMSE (Root Mean Square Error) differences between various weather models (OpenMeteo, ECMWF, NOAA) relative to ERA5 reference data.

## Features

- **Batch Processing**: Automatically processes all RAR files in a specified directory
- **Cross-Platform**: Works on both Windows and Linux
- **Automatic Extraction**: Extracts RAR files automatically (supports rarfile library, WinRAR, and unrar)
- **Error Handling**: Robust error handling with automatic cleanup of failed extractions
- **Skip Existing**: Automatically skips RAR files that have already been processed (checks for existing PNG files)
- **Clean Output**: Saves all PNG files to a centralized result directory in the project folder

## Requirements

- Python 3.7 or higher
- Required Python packages (see `requirements.txt`):
  - `numpy >= 2.4.1`
  - `matplotlib >= 3.10.8`
  - `rarfile >= 4.0`

### Optional Dependencies

For RAR file extraction, one of the following is required:
- **rarfile** Python library (recommended): `pip install rarfile`
  - Note: On Windows, this requires the `unrar.exe` executable
  - On Linux/Mac, requires `unrar` command-line tool
- **WinRAR** (Windows only): Installed in default location
- **unrar** (Linux/Mac): `sudo apt-get install unrar` (Linux) or `brew install unrar` (Mac)

## Installation

1. Clone or download this repository:
   ```bash
   git clone <repository-url>
   cd "Weather Forecast"
   ```

2. Create a virtual environment (recommended):
   ```bash
   # Windows
   python -m venv venv
   venv\Scripts\activate

   # Linux/Mac
   python3 -m venv venv
   source venv/bin/activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Basic Usage

The script requires a `--data` argument specifying the directory containing RAR files:

```bash
python generating_heatmap.py --data <path_to_rar_files>
```

### Examples

#### Windows

```bash
# Using absolute path
python generating_heatmap.py --data "D:\weather_data\rar_files"

# Using relative path
python generating_heatmap.py --data ".\data"

# With custom result directory
python generating_heatmap.py --data "D:\weather_data" --result "D:\output"
```

#### Linux/Mac

```bash
# Using absolute path
python generating_heatmap.py --data /home/username/weather_data/rar_files

# Using relative path
python generating_heatmap.py --data ./data

# With custom result directory
python generating_heatmap.py --data /home/username/weather_data --result /home/username/output
```

### Command-Line Arguments

- `--data` (required): Path to directory containing RAR files (absolute or relative path)
- `--result` (optional): Path to directory for output PNG files (default: `result/` in project directory)

## How It Works

1. **Finds RAR Files**: Scans the specified data directory for all `.rar` files
2. **Checks Existing**: For each RAR file, checks if a corresponding PNG already exists in the result directory
3. **Extracts**: If PNG doesn't exist, extracts the RAR file to a temporary directory
4. **Loads Data**: Reads pickle files (`api_x.pkl` and `y.pkl`) from the extracted files
5. **Processes**: Calculates RMSE values and generates comparative heatmaps
6. **Saves**: Saves the PNG file to the result directory with the same name as the RAR file
7. **Cleans Up**: Automatically deletes extracted files after processing (or on error)

## Output

- **Location**: PNG files are saved to `result/` directory in the project folder (unless `--result` is specified)
- **Naming**: PNG files use the same name as the RAR file (e.g., `2025-12-18.rar` → `2025-12-18.png`)
- **Content**: Each PNG contains 6 rows (one for each weather feature) and 3 columns showing:
  - Column 1: OpenMeteo - ECMWF (relative to ERA5)
  - Column 2: ECMWF RMSE (absolute values)
  - Column 3: NOAA - ECMWF (relative to ERA5)

## Error Handling

The script includes comprehensive error handling:

- **Failed Extractions**: Automatically cleans up and skips problematic RAR files
- **Missing Files**: Detects missing pickle files and skips with cleanup
- **Processing Errors**: Handles data processing errors gracefully
- **File Locks**: Retries cleanup with exponential backoff if files are locked
- **Continues Processing**: Errors in one file don't stop processing of other files

## Project Structure

```
Weather Forecast/
├── generating_heatmap.py    # Main script
├── requirements.txt         # Python dependencies
├── README.md               # This file
├── .gitignore              # Git ignore rules
├── result/                  # Output directory for PNG files
└── venv/                   # Virtual environment (if used)
```

## Troubleshooting

### RAR Extraction Issues

**Windows:**
- Install WinRAR in default location, or
- Install `rarfile` and ensure `unrar.exe` is in your PATH

**Linux/Mac:**
- Install unrar: `sudo apt-get install unrar` (Linux) or `brew install unrar` (Mac)
- Or install `rarfile` Python package

### File Lock Errors

If you see warnings about files being locked:
- Close any applications that might have the extracted files open (PDF viewers, file explorers)
- The script will retry cleanup automatically
- If cleanup fails, you may need to manually delete the extracted directories

### Missing Pickle Files

If a RAR file doesn't contain the required `api_x.pkl` and `y.pkl` files:
- The script will skip it and continue with other files
- Check the RAR file structure to ensure it contains the expected data

## License

[Add your license information here]

## Contributing

[Add contribution guidelines if applicable]



