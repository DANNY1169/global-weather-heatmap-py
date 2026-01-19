
A Python script that processes RAR files containing weather forecast data and generates comparative heatmaps showing RMSE (Root Mean Square Error) differences between various weather models relative to ERA5 reference data. The script processes hourly forecast data (1h through 20h) and generates separate visualizations for each hour.

## Features

- **Batch Processing**: Automatically processes all RAR files in a specified directory
- **Hourly Analysis**: Generates separate heatmaps for each forecast hour (1h through 20h)
- **Multiple Models**: Compares 5 weather models (Best Match, ECMWF IFS, GFS Global, Graphcast, AIFS) against ERA5 reference data
- **Cross-Platform**: Works on both Windows and Linux
- **Automatic Extraction**: Extracts RAR files automatically (supports rarfile library, WinRAR, and unrar)
- **Error Handling**: Robust error handling with automatic cleanup of failed extractions
- **Skip Existing**: Automatically skips hours that have already been processed (checks for existing PNG files)
- **Clean Output**: Saves all PNG files to organized subdirectories in the result folder

## Requirements

- Python 3.7 or higher
- Required Python packages (see `requirements.txt`):
  - `numpy`
  - `matplotlib`
  - `rarfile` (optional, for RAR extraction)

### Optional Dependencies

For RAR file extraction, one of the following is required:
- **rarfile** Python library (recommended): `pip install rarfile`
  - Note: On Windows, this requires the `unrar.exe` executable
  - On Linux/Mac, requires `unrar` command-line tool
- **WinRAR** (Windows only): Installed in default location (`C:\Program Files\WinRAR\WinRAR.exe` or `C:\Program Files (x86)\WinRAR\WinRAR.exe`)
- **unrar** (Linux/Mac): `sudo apt-get install unrar` (Linux) or `brew install unrar` (Mac)

## Installation

1. Clone or download this repository:
   ```bash
   git clone <repository-url>
   cd global-weather-heatmap-py
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

The script accepts an optional `--data` argument (defaults to `D:` if not specified):

```bash
# Use default data directory (D:)
python generating_heatmap.py

# Specify custom data directory
python generating_heatmap.py --data <path_to_rar_files>

# Specify custom result directory
python generating_heatmap.py --data <path_to_rar_files> --result <output_path>
```

### Examples

#### Windows

```bash
# Using default data directory (D:)
python generating_heatmap.py

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

- `--data` (optional): Path to directory containing RAR files (absolute or relative path, default: `D:`)
- `--result` (optional): Path to directory for output PNG files (default: `result/` in project directory)

## How It Works

1. **Finds RAR Files**: Scans the specified data directory for all `.rar` files
2. **Checks Existing**: For each RAR file and each hour (1h-20h), checks if a corresponding PNG already exists
3. **Extracts**: If PNG doesn't exist, extracts the RAR file to a temporary directory
4. **Loads Data**: Reads pickle files (`api_x.pkl` and `y.pkl`) from the extracted files
5. **Processes**: For each hour, calculates RMSE values and generates comparative heatmaps
6. **Saves**: Saves PNG files to `result/<rar_filename>/<rar_filename>_<hour>.png` (e.g., `result/2025-12-19/2025-12-19_1h.png`)
7. **Cleans Up**: Automatically deletes extracted files after processing (or on error)

## Weather Models

The script compares the following weather models against ERA5 reference data:

1. **Best Match**: Optimal forecast combination
2. **ECMWF IFS**: European Centre for Medium-Range Weather Forecasts Integrated Forecasting System
3. **GFS Global**: Global Forecast System from NOAA
4. **Graphcast**: Google DeepMind's GraphCast model
5. **AIFS**: AI-based forecasting system

## Weather Features

Each heatmap visualizes 6 weather features:

1. **temperature_2t**: 2-meter temperature
2. **dewpoint_2d**: 2-meter dewpoint
3. **u100_100u**: 100-meter U-component wind speed (converted from m/s to km/h)
4. **v100_100v**: 100-meter V-component wind speed (converted from m/s to km/h)
5. **precipitation_tp**: Total precipitation
6. **sp**: Surface pressure

## Output

- **Location**: PNG files are saved to `result/<rar_filename>/` directory in the project folder (unless `--result` is specified)
- **Naming**: PNG files use the format `<rar_filename>_<hour>.png` (e.g., `2025-12-19_1h.png`, `2025-12-19_2h.png`, etc.)
- **Content**: Each PNG contains:
  - **6 rows**: One for each weather feature
  - **5 columns**: One for each weather model
    - Column 1: Best Match - ECMWF (relative to ERA5)
    - Column 2: ECMWF RMSE (absolute values)
    - Column 3: GFS Global - ECMWF (relative to ERA5)
    - Column 4: Graphcast - ECMWF (relative to ERA5)
    - Column 5: AIFS - ECMWF (relative to ERA5)
- **Visualization**: Uses diverging colormap (RdBu_r) for relative differences and viridis for absolute ECMWF RMSE values

## Error Handling

The script includes comprehensive error handling:

- **Failed Extractions**: Automatically tries fallback extraction methods (WinRAR/unrar) if rarfile library fails
- **Missing Files**: Detects missing pickle files and skips with cleanup
- **Processing Errors**: Handles data processing errors gracefully
- **File Locks**: Retries cleanup with exponential backoff if files are locked
- **Continues Processing**: Errors in one file don't stop processing of other files
- **Shape Validation**: Automatically handles array shape mismatches

## Project Structure

```
global-weather-heatmap-py/
├── generating_heatmap.py    # Main script
├── requirements.txt         # Python dependencies
├── README.md               # This file
├── result/                  # Output directory for PNG files
│   └── <rar_filename>/     # Subdirectory for each RAR file
│       ├── <filename>_1h.png
│       ├── <filename>_2h.png
│       └── ...
└── venv/                   # Virtual environment (if used)
```

## Troubleshooting

### RAR Extraction Issues

**Windows:**
- Install WinRAR in default location, or
- Install `rarfile` and ensure `unrar.exe` is in your PATH
- The script will automatically try WinRAR as a fallback if rarfile fails

**Linux/Mac:**
- Install unrar: `sudo apt-get install unrar` (Linux) or `brew install unrar` (Mac)
- Or install `rarfile` Python package

### File Lock Errors

If you see warnings about files being locked:
- Close any applications that might have the extracted files open (PDF viewers, file explorers)
- The script will retry cleanup automatically with exponential backoff
- If cleanup fails, you may need to manually delete the extracted directories

### Missing Pickle Files

If a RAR file doesn't contain the required `api_x.pkl` and `y.pkl` files:
- The script will skip it and continue with other files
- Check the RAR file structure to ensure it contains the expected data

### Shape Mismatch Errors

If you encounter broadcasting errors:
- The script includes automatic shape validation and squeezing
- Ensure your data files have the expected structure: `(720, 1440, 20, 6)` for model data and `(720, 1440, 20, 6)` for ERA5 data

## License

[Add your license information here]

## Contributing

[Add contribution guidelines if applicable]
