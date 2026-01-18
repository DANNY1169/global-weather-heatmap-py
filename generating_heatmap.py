import numpy as np
import matplotlib.pyplot as plt
import pickle
import os
import subprocess
import sys
import shutil
import glob
import argparse

def find_pickle_file(directory, filename):
    """Recursively search for a pickle file in the directory."""
    for root, dirs, files in os.walk(directory):
        if filename in files:
            return os.path.join(root, filename)
    return None

def extract_rar_file(rar_path, extracted_path):
    """Extract a RAR file to the specified directory."""
    # Convert to absolute paths
    rar_path = os.path.abspath(rar_path)
    extracted_path = os.path.abspath(extracted_path)
    
    # Remove existing directory if it exists
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
        return True
    except ImportError:
        # Fallback: try using WinRAR command line (Windows) or unrar (Linux)
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
                    # Use absolute paths for Windows
                    subprocess.run([winrar_exe, 'x', '-y', rar_path, extracted_path + os.sep], 
                                 check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    print("Extraction complete.")
                    return True
                else:
                    raise Exception("WinRAR not found. Please install WinRAR or install rarfile: pip install rarfile")
            except Exception as e:
                print(f"Error extracting RAR file: {e}")
                print("Please either:")
                print("1. Install rarfile: pip install rarfile (requires unrar executable)")
                print("2. Manually extract the RAR file to: " + extracted_path)
                return False
        else:
            # Try unrar on Linux/Mac
            try:
                subprocess.run(['unrar', 'x', '-y', rar_path, extracted_path + os.sep],
                             check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                print("Extraction complete.")
                return True
            except (subprocess.CalledProcessError, FileNotFoundError):
                print("Error: rarfile library not installed and unrar not found.")
                print("Please install it with: pip install rarfile")
                print("Or install unrar: sudo apt-get install unrar (Linux) or brew install unrar (Mac)")
                print("Or manually extract the RAR file to: " + extracted_path)
                return False

def process_rar_file(rar_path, data_dir, result_dir):
    """Process a single RAR file: extract, generate heatmap, and clean up."""
    print(f"\n{'='*60}")
    print(f"Processing: {os.path.basename(rar_path)}")
    print(f"{'='*60}")
    
    # Convert to absolute paths
    rar_path = os.path.abspath(rar_path)
    data_dir = os.path.abspath(data_dir)
    result_dir = os.path.abspath(result_dir)
    
    # Determine extracted path based on RAR filename (in data directory)
    rar_basename = os.path.splitext(os.path.basename(rar_path))[0]
    extracted_path = os.path.join(data_dir, rar_basename)
    fig = None
    
    def cleanup_extracted_files():
        """Helper function to clean up extracted files with retry logic."""
        if not os.path.exists(extracted_path):
            return
        
        max_retries = 5
        retry_delay = 0.5  # seconds
        
        for attempt in range(max_retries):
            try:
                # On Windows, try to remove read-only files first
                if sys.platform == 'win32':
                    import stat
                    def remove_readonly(func, path, exc):
                        try:
                            os.chmod(path, stat.S_IWRITE)
                            func(path)
                        except:
                            pass
                    
                    shutil.rmtree(extracted_path, onerror=remove_readonly)
                else:
                    shutil.rmtree(extracted_path)
                
                print(f"Cleaned up extracted directory: {extracted_path}")
                return
                
            except PermissionError as e:
                if attempt < max_retries - 1:
                    import time
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                    continue
                else:
                    # Last attempt: try more aggressive cleanup
                    try:
                        if sys.platform == 'win32':
                            # Try using Windows command to force delete
                            subprocess.run(['cmd', '/c', 'rmdir', '/s', '/q', extracted_path], 
                                         check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=5)
                        else:
                            # Try using rm -rf on Linux/Mac
                            subprocess.run(['rm', '-rf', extracted_path], 
                                         check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=5)
                        if not os.path.exists(extracted_path):
                            print(f"Cleaned up extracted directory: {extracted_path}")
                            return
                    except:
                        pass
                    print(f"Warning: Could not clean up {extracted_path} after {max_retries} attempts: {e}")
                    print("Directory may be locked by another process. Please delete manually if needed.")
                    
            except Exception as cleanup_error:
                if attempt < max_retries - 1:
                    import time
                    time.sleep(retry_delay)
                    retry_delay *= 2
                    continue
                else:
                    print(f"Warning: Could not clean up {extracted_path} after {max_retries} attempts: {cleanup_error}")
                    print("Directory may be locked by another process. Please delete manually if needed.")
    
    try:
        # Check if RAR file exists
        if not os.path.exists(rar_path):
            print(f"Error: RAR file not found: {rar_path}")
            cleanup_extracted_files()
            return False
        
        # Check if required files exist
        api_x_path = find_pickle_file(extracted_path, 'api_x.pkl') if os.path.exists(extracted_path) else None
        y_path = find_pickle_file(extracted_path, 'y.pkl') if os.path.exists(extracted_path) else None
        
        # Extract if directory doesn't exist or required files are missing
        if not api_x_path or not y_path:
            if not extract_rar_file(rar_path, extracted_path):
                print(f"Skipping {rar_path} due to extraction error.")
                cleanup_extracted_files()
                return False
            
            # Find the files after extraction
            api_x_path = find_pickle_file(extracted_path, 'api_x.pkl')
            y_path = find_pickle_file(extracted_path, 'y.pkl')
            
            if not api_x_path or not y_path:
                print(f"Error: Could not find required pickle files in {extracted_path}")
                print("Skipping this RAR file.")
                cleanup_extracted_files()
                return False

        # Load data from pickle files
        try:
            with open(api_x_path, 'rb') as f:
                data_3 = pickle.load(f)
            data_3 = np.array(data_3)
        except Exception as e:
            print(f"Error loading {api_x_path}: {e}")
            cleanup_extracted_files()
            return False
        
        openmeteo, ecmwf, noaa = np.split(data_3, 3, axis = -1)
        openmeteo = openmeteo.squeeze(axis = -1)
        ecmwf = ecmwf.squeeze(axis = -1)
        noaa = noaa.squeeze(axis = -1)
        openmeteo = np.transpose(openmeteo, (0,2,1,3))
        ecmwf = np.transpose(ecmwf, (0,2,1,3))
        noaa = np.transpose(noaa, (0,2,1,3))
        
        try:
            with open(y_path, 'rb') as f:
                era5 = pickle.load(f)
            era5 = np.array(era5)
        except Exception as e:
            print(f"Error loading {y_path}: {e}")
            cleanup_extracted_files()
            return False
        
        era5 = np.transpose(era5, (0,2,1,3))
        
        # Data processing with error handling
        try:
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
        except Exception as e:
            print(f"Error processing data arrays (reshape/transpose/split): {e}")
            cleanup_extracted_files()
            return False
        
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
        try:
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
        except Exception as e:
            print(f"Error during heatmap generation: {e}")
            cleanup_extracted_files()
            return False

        plt.tight_layout()

        # Get the RAR filename without extension for the output PNG
        try:
            rar_filename = os.path.splitext(os.path.basename(rar_path))[0]
            os.makedirs(result_dir, exist_ok=True)
            output_path = os.path.join(result_dir, f'{rar_filename}.png')
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            print(f"Saved heatmap to: {output_path}")
        except Exception as e:
            print(f"Error saving PNG file: {e}")
            cleanup_extracted_files()
            return False
        finally:
            # Close the figure to free memory
            if fig is not None:
                try:
                    plt.close(fig)
                except:
                    pass
            fig = None

        # Clean up: delete the extracted directory after successful processing
        cleanup_extracted_files()
        
        return True
    
    except Exception as e:
        print(f"Error processing {os.path.basename(rar_path)}: {e}")
        import traceback
        print("Traceback:")
        traceback.print_exc()
        
        # Clean up figure if it was created
        if fig is not None:
            try:
                plt.close(fig)
            except:
                pass
        
        # Always clean up extracted files for problematic RAR files
        cleanup_extracted_files()
        
        return False

# Main execution: Find and process all RAR files
if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Process RAR files and generate heatmaps.')
    parser.add_argument('--data', type=str, required=True,
                        help='Path to directory containing RAR files (absolute or relative path)')
    parser.add_argument('--result', type=str, default=None,
                        help='Path to directory for output PNG files (default: result/ in project directory)')
    
    args = parser.parse_args()
    
    # Convert to absolute paths
    data_dir = os.path.abspath(args.data)
    
    # Set result directory to project directory (where script is located), not input directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    if args.result:
        result_dir = os.path.abspath(args.result)
    else:
        result_dir = os.path.join(script_dir, 'result')
    
    # Check if data directory exists
    if not os.path.exists(data_dir):
        print(f"Error: Data directory does not exist: {data_dir}")
        sys.exit(1)
    
    if not os.path.isdir(data_dir):
        print(f"Error: Path is not a directory: {data_dir}")
        sys.exit(1)
    
    # Find all RAR files in the data directory
    rar_pattern = os.path.join(data_dir, '*.rar')
    rar_files = glob.glob(rar_pattern)
    
    if not rar_files:
        print(f"No RAR files found in: {data_dir}")
        sys.exit(1)
    
    print(f"Data directory: {data_dir}")
    print(f"Result directory: {result_dir}")
    print(f"Found {len(rar_files)} RAR file(s) to process.")
    
    # Process each RAR file one by one
    successful = 0
    failed = 0
    
    for rar_path in sorted(rar_files):
        try:
            if process_rar_file(rar_path, data_dir, result_dir):
                successful += 1
            else:
                failed += 1
        except Exception as e:
            print(f"Error processing {rar_path}: {e}")
            failed += 1
    
    print(f"\n{'='*60}")
    print(f"Processing complete!")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"{'='*60}")

