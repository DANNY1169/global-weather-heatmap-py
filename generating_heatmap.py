import numpy as np
import matplotlib.pyplot as plt
import pickle
import os
import subprocess
import sys
import shutil

# Extract RAR file if needed
rar_path = 'data/2025-12-18.rar'
extracted_path = 'data/2025-12-18'

def find_pickle_file(directory, filename):
    """Recursively search for a pickle file in the directory."""
    for root, dirs, files in os.walk(directory):
        if filename in files:
            return os.path.join(root, filename)
    return None

# Check if required files exist
api_x_path = find_pickle_file(extracted_path, 'api_x.pkl') if os.path.exists(extracted_path) else None
y_path = find_pickle_file(extracted_path, 'y.pkl') if os.path.exists(extracted_path) else None

# Extract if directory doesn't exist or required files are missing
if not api_x_path or not y_path:
    # Remove existing directory if it's empty or incomplete
    if os.path.exists(extracted_path):
        try:
            shutil.rmtree(extracted_path)
        except:
            pass
    
    print(f"Extracting {rar_path}...")
    os.makedirs(extracted_path, exist_ok=True)
    
    # Try using rarfile library first
    try:
        import rarfile
        with rarfile.RarFile(rar_path) as rf:
            rf.extractall(extracted_path)
        print("Extraction complete.")
    except ImportError:
        # Fallback: try using WinRAR command line (Windows)
        if sys.platform == 'win32':
            try:
                # Try common WinRAR installation paths
                winrar_paths = [
                    r'C:\Program Files\WinRAR\WinRAR.exe',
                    r'C:\Program Files (x86)\WinRAR\WinRAR.exe',
                ]
                winrar_exe = None
                for path in winrar_paths:
                    if os.path.exists(path):
                        winrar_exe = path
                        break
                
                if winrar_exe:
                    subprocess.run([winrar_exe, 'x', '-y', rar_path, extracted_path + '\\'], 
                                 check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    print("Extraction complete.")
                else:
                    raise Exception("WinRAR not found. Please install WinRAR or install rarfile: pip install rarfile")
            except Exception as e:
                print(f"Error extracting RAR file: {e}")
                print("Please either:")
                print("1. Install rarfile: pip install rarfile (requires unrar executable)")
                print("2. Manually extract the RAR file to: " + extracted_path)
                sys.exit(1)
        else:
            print("Error: rarfile library not installed.")
            print("Please install it with: pip install rarfile")
            print("Or manually extract the RAR file to: " + extracted_path)
            sys.exit(1)
    
    # Find the files after extraction
    api_x_path = find_pickle_file(extracted_path, 'api_x.pkl')
    y_path = find_pickle_file(extracted_path, 'y.pkl')
    
    if not api_x_path or not y_path:
        print(f"Error: Could not find required pickle files in {extracted_path}")
        print("Please check the RAR file structure.")
        sys.exit(1)

# Load data from pickle files
with open(api_x_path, 'rb') as f:
    data_3 = pickle.load(f)
data_3 = np.array(data_3)
openmeteo, ecmwf, noaa = np.split(data_3, 3, axis = -1)
openmeteo = openmeteo.squeeze(axis = -1)
ecmwf = ecmwf.squeeze(axis = -1)
noaa = noaa.squeeze(axis = -1)
openmeteo = np.transpose(openmeteo, (0,2,1,3))
ecmwf = np.transpose(ecmwf, (0,2,1,3))
noaa = np.transpose(noaa, (0,2,1,3))


with open(y_path, 'rb') as f:
    era5 = pickle.load(f)
era5 = np.array(era5)

era5 = np.transpose(era5, (0,2,1,3))

openmeteo = openmeteo.reshape(45,90,16,16,24,6)
ecmwf = ecmwf.reshape(45,90,16,16,24,6)
noaa = noaa.reshape(45,90,16,16,24,6)
era5 = era5.reshape(45,90,16,16,24,6)

openmeteo = openmeteo.transpose(0,2,1,3,4,5)
ecmwf = ecmwf.transpose(0,2,1,3,4,5)
noaa = noaa.transpose(0,2,1,3,4,5)
era5 = era5.transpose(0,2,1,3,4,5)

openmeteo = openmeteo.reshape(45*16,90*16,24,6)
ecmwf = ecmwf.reshape(45*16,90*16,24,6)
noaa = noaa.reshape(45*16,90*16,24,6)
era5 = era5.reshape(45*16,90*16,24,6)

openmeteo_2m_t, openmeteo_2m_d, openmeteo_u100, openmeteo_v100, openmeteo_precipitation, openmeteo_sp = np.split(openmeteo, 6, axis = -1)
ecmwf_2m_t, ecmwf_2m_d, ecmwf_u100, ecmwf_v100, ecmwf_precipitation, ecmwf_sp = np.split(ecmwf, 6, axis = -1)
noaa_2m_t, noaa_2m_d, noaa_u100, noaa_v100, noaa_precipitation, noaa_sp = np.split(noaa, 6, axis = -1)
era5_2m_t, era5_2m_d, era5_u100, era5_v100, era5_precipitation, era5_sp = np.split(era5, 6, axis = -1)

openmeteo_2m_t = openmeteo_2m_t.squeeze(axis = -1)
openmeteo_2m_d = openmeteo_2m_d.squeeze(axis = -1)
openmeteo_u100 = openmeteo_u100.squeeze(axis = -1)
openmeteo_v100 = openmeteo_v100.squeeze(axis = -1)
openmeteo_precipitation = openmeteo_precipitation.squeeze(axis = -1)
openmeteo_sp = openmeteo_sp.squeeze(axis = -1)

ecmwf_2m_t = ecmwf_2m_t.squeeze(axis = -1)
ecmwf_2m_d = ecmwf_2m_d.squeeze(axis = -1)
ecmwf_u100 = ecmwf_u100.squeeze(axis = -1)
ecmwf_v100 = ecmwf_v100.squeeze(axis = -1)
ecmwf_precipitation = ecmwf_precipitation.squeeze(axis = -1)
ecmwf_sp = ecmwf_sp.squeeze(axis = -1)

noaa_2m_t = noaa_2m_t.squeeze(axis = -1)
noaa_2m_d = noaa_2m_d.squeeze(axis = -1)
noaa_u100 = noaa_u100.squeeze(axis = -1)
noaa_v100 = noaa_v100.squeeze(axis = -1)
noaa_precipitation = noaa_precipitation.squeeze(axis = -1)
noaa_sp = noaa_sp.squeeze(axis = -1)

era5_2m_t = era5_2m_t.squeeze(axis = -1)
era5_2m_d = era5_2m_d.squeeze(axis = -1)
era5_u100 = era5_u100.squeeze(axis = -1)
era5_v100 = era5_v100.squeeze(axis = -1)
era5_precipitation = era5_precipitation.squeeze(axis = -1)
era5_sp = era5_sp.squeeze(axis = -1)

# Define feature names and corresponding data arrays
features = [
    ('temperature_2t', openmeteo_2m_t, ecmwf_2m_t, noaa_2m_t, era5_2m_t),
    ('dewpoint_2d', openmeteo_2m_d, ecmwf_2m_d, noaa_2m_d, era5_2m_d),
    ('u100', openmeteo_u100, ecmwf_u100, noaa_u100, era5_u100),
    ('v100', openmeteo_v100, ecmwf_v100, noaa_v100, era5_v100),
    ('precipitation', openmeteo_precipitation, ecmwf_precipitation, noaa_precipitation, era5_precipitation),
    ('sp', openmeteo_sp, ecmwf_sp, noaa_sp, era5_sp)
]

# Create figure with 6 rows (features) and 3 columns (comparisons)
fig, axes = plt.subplots(6, 3, figsize=(18, 30))

# Use diverging colormap for better contrast
cmap = 'RdBu_r'  # or 'coolwarm', 'seismic', 'bwr'

# Process each feature
for row_idx, (feature_name, om_data, ecmwf_data, noaa_data, era5_data) in enumerate(features):
    # Calculate RMSE for each model
    rmse_openmeteo = np.flip(np.sqrt(np.mean((om_data - era5_data) ** 2, axis=2)), axis=0)
    rmse_ecmwf = np.flip(np.sqrt(np.mean((ecmwf_data - era5_data) ** 2, axis=2)), axis=0)
    rmse_noaa = np.flip(np.sqrt(np.mean((noaa_data - era5_data) ** 2, axis=2)), axis=0)
    
    # Calculate differences relative to ECMWF
    data1 = rmse_openmeteo - rmse_ecmwf
    data3 = rmse_noaa - rmse_ecmwf
    
    # Find global min/max for symmetric color scaling
    vmax = max(np.abs(data1).max(), np.abs(data3).max())
    vmin = -vmax
    
    # Plot with symmetric color scaling
    im0 = axes[row_idx, 0].imshow(data1, cmap=cmap, vmin=vmin, vmax=vmax)
    axes[row_idx, 0].set_title(f'{feature_name}\nOpenMeteo - ECMWF\n(relative to ERA5)')
    axes[row_idx, 0].axis('off')
    
    # Second plot shows absolute ECMWF RMSE
    im1 = axes[row_idx, 1].imshow(rmse_ecmwf, cmap='viridis')
    axes[row_idx, 1].set_title(f'{feature_name}\nECMWF RMSE\n(absolute values)')
    axes[row_idx, 1].axis('off')
    
    im2 = axes[row_idx, 2].imshow(data3, cmap=cmap, vmin=vmin, vmax=vmax)
    axes[row_idx, 2].set_title(f'{feature_name}\nNOAA - ECMWF\n(relative to ERA5)')
    axes[row_idx, 2].axis('off')
    
    # Add colorbars
    fig.colorbar(im0, ax=axes[row_idx, 0], fraction=0.046, pad=0.04)
    fig.colorbar(im1, ax=axes[row_idx, 1], fraction=0.046, pad=0.04)
    fig.colorbar(im2, ax=axes[row_idx, 2], fraction=0.046, pad=0.04)

plt.tight_layout()

# Get the RAR filename without extension for the output PNG
rar_filename = os.path.splitext(os.path.basename(rar_path))[0]
result_dir = 'result'
os.makedirs(result_dir, exist_ok=True)
output_path = os.path.join(result_dir, f'{rar_filename}.png')
plt.savefig(output_path, dpi=300, bbox_inches='tight')
print(f"Saved heatmap to: {output_path}")

# Clean up: delete the extracted directory
if os.path.exists(extracted_path):
    try:
        shutil.rmtree(extracted_path)
        print(f"Deleted extracted directory: {extracted_path}")
    except Exception as e:
        print(f"Warning: Could not delete {extracted_path}: {e}")

plt.show()

