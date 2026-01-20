import numpy as np
import matplotlib.pyplot as plt
import h5py
import os
import subprocess
import sys
import shutil
import glob
import argparse

def find_h5_file(directory, filename):
    """Recursively search for an HDF5 file in the directory."""
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
        # rarfile library not installed, try fallback
        pass
    except Exception as e:
        # rarfile library error (BadRarFile, RarCannotExec, etc.), try fallback
        print(f"rarfile library error: {e}")
        print("Trying fallback extraction method...")
    
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
    
    # Create subdirectory for this RAR file in result directory
    rar_output_dir = os.path.join(result_dir, rar_basename)
    os.makedirs(rar_output_dir, exist_ok=True)
    
    # Check if all PNG files already exist (one per day: 1d, 2d, 3d, 4d, 5d)
    all_pngs_exist = True
    for day in ['1d', '2d', '3d', '4d', '5d']:
        png_path = os.path.join(rar_output_dir, f'{rar_basename}_{day}.png')
        if not os.path.exists(png_path):
            all_pngs_exist = False
            break
    
    if all_pngs_exist:
        print(f"All PNG files already exist for {rar_basename}")
        print("Skipping processing for this RAR file.")
        return True
    
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
        api_x_path = find_h5_file(extracted_path, 'api_x.h5') if os.path.exists(extracted_path) else None
        y_path = find_h5_file(extracted_path, 'y.h5') if os.path.exists(extracted_path) else None
        
        # Extract if directory doesn't exist or required files are missing
        if not api_x_path or not y_path:
            if not extract_rar_file(rar_path, extracted_path):
                print(f"Skipping {rar_path} due to extraction error.")
                cleanup_extracted_files()
                return False
            
            # Find the files after extraction
            api_x_path = find_h5_file(extracted_path, 'api_x.h5')
            y_path = find_h5_file(extracted_path, 'y.h5')
            
            if not api_x_path or not y_path:
                print(f"Error: Could not find required HDF5 files in {extracted_path}")
                print("Skipping this RAR file.")
                cleanup_extracted_files()
                return False

        # Load data from HDF5 files
        try:
            # Check if file exists and is readable
            if not os.path.exists(api_x_path):
                raise FileNotFoundError(f"HDF5 file not found: {api_x_path}")
            
            # Check file size (HDF5 files should be at least a few bytes)
            file_size = os.path.getsize(api_x_path)
            if file_size == 0:
                raise ValueError(f"HDF5 file is empty: {api_x_path}")
            
            with h5py.File(api_x_path, 'r') as f:
                # Try common dataset names, or use the first available dataset
                if 'data' in f:
                    data_3 = np.array(f['data'])
                elif 'dataset' in f:
                    data_3 = np.array(f['dataset'])
                else:
                    # Use the first dataset found
                    keys = list(f.keys())
                    if keys:
                        data_3 = np.array(f[keys[0]])
                    else:
                        raise ValueError("No datasets found in HDF5 file")
        except (OSError, IOError) as e:
            print(f"Error reading HDF5 file {api_x_path}: {e}")
            print("The file may be corrupted, truncated, or incomplete.")
            print("Please verify the RAR file extraction was successful.")
            cleanup_extracted_files()
            return False
        except Exception as e:
            print(f"Error loading {api_x_path}: {e}")
            print("Please verify the HDF5 file is valid and not corrupted.")
            cleanup_extracted_files()
            return False
        
        best_match, ecmwf_ifs, gfs_global, graphcast, aifs = np.split(data_3, 5, axis = -1)
        best_match = best_match.squeeze(axis = -1)
        ecmwf_ifs = ecmwf_ifs.squeeze(axis = -1)
        gfs_global = gfs_global.squeeze(axis = -1)
        graphcast = graphcast.squeeze(axis = -1)
        aifs = aifs.squeeze(axis = -1)

        best_match_1h = best_match[:,:,0,:]
        best_match_2h = best_match[:,:,1,:]
        best_match_3h = best_match[:,:,2,:]
        best_match_4h = best_match[:,:,3,:]
        best_match_5h = best_match[:,:,4,:]
        best_match_6h = best_match[:,:,5,:]
        best_match_7h = best_match[:,:,6,:]
        best_match_8h = best_match[:,:,7,:]
        best_match_9h = best_match[:,:,8,:]
        best_match_10h = best_match[:,:,9,:]
        best_match_11h = best_match[:,:,10,:]
        best_match_12h = best_match[:,:,11,:]
        best_match_13h = best_match[:,:,12,:]
        best_match_14h = best_match[:,:,13,:]
        best_match_15h = best_match[:,:,14,:]
        best_match_16h = best_match[:,:,15,:]
        best_match_17h = best_match[:,:,16,:]
        best_match_18h = best_match[:,:,17,:]
        best_match_19h = best_match[:,:,18,:]
        best_match_20h = best_match[:,:,19,:]

        ecmwf_ifs_1h = ecmwf_ifs[:,:,0,:]
        ecmwf_ifs_2h = ecmwf_ifs[:,:,1,:]
        ecmwf_ifs_3h = ecmwf_ifs[:,:,2,:]
        ecmwf_ifs_4h = ecmwf_ifs[:,:,3,:]
        ecmwf_ifs_5h = ecmwf_ifs[:,:,4,:]
        ecmwf_ifs_6h = ecmwf_ifs[:,:,5,:]
        ecmwf_ifs_7h = ecmwf_ifs[:,:,6,:]
        ecmwf_ifs_8h = ecmwf_ifs[:,:,7,:]
        ecmwf_ifs_9h = ecmwf_ifs[:,:,8,:]
        ecmwf_ifs_10h = ecmwf_ifs[:,:,9,:]
        ecmwf_ifs_11h = ecmwf_ifs[:,:,10,:]
        ecmwf_ifs_12h = ecmwf_ifs[:,:,11,:]
        ecmwf_ifs_13h = ecmwf_ifs[:,:,12,:]
        ecmwf_ifs_14h = ecmwf_ifs[:,:,13,:]
        ecmwf_ifs_15h = ecmwf_ifs[:,:,14,:]
        ecmwf_ifs_16h = ecmwf_ifs[:,:,15,:]
        ecmwf_ifs_17h = ecmwf_ifs[:,:,16,:]
        ecmwf_ifs_18h = ecmwf_ifs[:,:,17,:]
        ecmwf_ifs_19h = ecmwf_ifs[:,:,18,:]
        ecmwf_ifs_20h = ecmwf_ifs[:,:,19,:]

        gfs_global_1h = gfs_global[:,:,0,:]
        gfs_global_2h = gfs_global[:,:,1,:]
        gfs_global_3h = gfs_global[:,:,2,:]
        gfs_global_4h = gfs_global[:,:,3,:]
        gfs_global_5h = gfs_global[:,:,4,:]
        gfs_global_6h = gfs_global[:,:,5,:]
        gfs_global_7h = gfs_global[:,:,6,:]
        gfs_global_8h = gfs_global[:,:,7,:]
        gfs_global_9h = gfs_global[:,:,8,:]
        gfs_global_10h = gfs_global[:,:,9,:]
        gfs_global_11h = gfs_global[:,:,10,:]
        gfs_global_12h = gfs_global[:,:,11,:]
        gfs_global_13h = gfs_global[:,:,12,:]
        gfs_global_14h = gfs_global[:,:,13,:]
        gfs_global_15h = gfs_global[:,:,14,:]
        gfs_global_16h = gfs_global[:,:,15,:]
        gfs_global_17h = gfs_global[:,:,16,:]
        gfs_global_18h = gfs_global[:,:,17,:]
        gfs_global_19h = gfs_global[:,:,18,:]
        gfs_global_20h = gfs_global[:,:,19,:]

        graphcast_1h = graphcast[:,:,0,:]
        graphcast_2h = graphcast[:,:,1,:]
        graphcast_3h = graphcast[:,:,2,:]
        graphcast_4h = graphcast[:,:,3,:]
        graphcast_5h = graphcast[:,:,4,:]
        graphcast_6h = graphcast[:,:,5,:]
        graphcast_7h = graphcast[:,:,6,:]
        graphcast_8h = graphcast[:,:,7,:]
        graphcast_9h = graphcast[:,:,8,:]
        graphcast_10h = graphcast[:,:,9,:]
        graphcast_11h = graphcast[:,:,10,:]
        graphcast_12h = graphcast[:,:,11,:]
        graphcast_13h = graphcast[:,:,12,:]
        graphcast_14h = graphcast[:,:,13,:]
        graphcast_15h = graphcast[:,:,14,:]
        graphcast_16h = graphcast[:,:,15,:]
        graphcast_17h = graphcast[:,:,16,:]
        graphcast_18h = graphcast[:,:,17,:]
        graphcast_19h = graphcast[:,:,18,:]
        graphcast_20h = graphcast[:,:,19,:]

        aifs_1h = aifs[:,:,0,:]
        aifs_2h = aifs[:,:,1,:]
        aifs_3h = aifs[:,:,2,:]
        aifs_4h = aifs[:,:,3,:]
        aifs_5h = aifs[:,:,4,:]
        aifs_6h = aifs[:,:,5,:]
        aifs_7h = aifs[:,:,6,:]
        aifs_8h = aifs[:,:,7,:]
        aifs_9h = aifs[:,:,8,:]
        aifs_10h = aifs[:,:,9,:]
        aifs_11h = aifs[:,:,10,:]
        aifs_12h = aifs[:,:,11,:]
        aifs_13h = aifs[:,:,12,:]
        aifs_14h = aifs[:,:,13,:]
        aifs_15h = aifs[:,:,14,:]
        aifs_16h = aifs[:,:,15,:]
        aifs_17h = aifs[:,:,16,:]
        aifs_18h = aifs[:,:,17,:]
        aifs_19h = aifs[:,:,18,:]
        aifs_20h = aifs[:,:,19,:]

        try:
            # Check if file exists and is readable
            if not os.path.exists(y_path):
                raise FileNotFoundError(f"HDF5 file not found: {y_path}")
            
            # Check file size (HDF5 files should be at least a few bytes)
            file_size = os.path.getsize(y_path)
            if file_size == 0:
                raise ValueError(f"HDF5 file is empty: {y_path}")
            
            with h5py.File(y_path, 'r') as f:
                # Try common dataset names, or use the first available dataset
                if 'data' in f:
                    era5 = np.array(f['data'])
                elif 'dataset' in f:
                    era5 = np.array(f['dataset'])
                else:
                    # Use the first dataset found
                    keys = list(f.keys())
                    if keys:
                        era5 = np.array(f[keys[0]])
                    else:
                        raise ValueError("No datasets found in HDF5 file")
        except (OSError, IOError) as e:
            print(f"Error reading HDF5 file {y_path}: {e}")
            print("The file may be corrupted, truncated, or incomplete.")
            print("Please verify the RAR file extraction was successful.")
            cleanup_extracted_files()
            return False
        except Exception as e:
            print(f"Error loading {y_path}: {e}")
            print("Please verify the HDF5 file is valid and not corrupted.")
            cleanup_extracted_files()
            return False
        

        era5_1h = era5[:,:,0,:]
        era5_2h = era5[:,:,1,:]
        era5_3h = era5[:,:,2,:]
        era5_4h = era5[:,:,3,:]
        era5_5h = era5[:,:,4,:]
        era5_6h = era5[:,:,5,:]
        era5_7h = era5[:,:,6,:]
        era5_8h = era5[:,:,7,:]
        era5_9h = era5[:,:,8,:]
        era5_10h = era5[:,:,9,:]
        era5_11h = era5[:,:,10,:]
        era5_12h = era5[:,:,11,:]
        era5_13h = era5[:,:,12,:]
        era5_14h = era5[:,:,13,:]
        era5_15h = era5[:,:,14,:]
        era5_16h = era5[:,:,15,:]
        era5_17h = era5[:,:,16,:]
        era5_18h = era5[:,:,17,:]
        era5_19h = era5[:,:,18,:]
        era5_20h = era5[:,:,19,:]

        best_match_1h_2t = best_match_1h[:, :, 0]
        best_match_1h_2d = best_match_1h[:, :, 1]
        best_match_1h_100u = best_match_1h[:, :, 2]/3.6
        best_match_1h_100v = best_match_1h[:, :, 3]/3.6
        best_match_1h_tp = best_match_1h[:, :, 4]
        best_match_1h_sp = best_match_1h[:, :, 5]

        best_match_2h_2t = best_match_2h[:, :, 0]
        best_match_2h_2d = best_match_2h[:, :, 1]
        best_match_2h_100u = best_match_2h[:, :, 2]/3.6
        best_match_2h_100v = best_match_2h[:, :, 3]/3.6
        best_match_2h_tp = best_match_2h[:, :, 4]
        best_match_2h_sp = best_match_2h[:, :, 5]


        best_match_3h_2t = best_match_3h[:, :, 0]
        best_match_3h_2d = best_match_3h[:, :, 1]
        best_match_3h_100u = best_match_3h[:, :, 2]/3.6
        best_match_3h_100v = best_match_3h[:, :, 3]/3.6
        best_match_3h_tp = best_match_3h[:, :, 4]
        best_match_3h_sp = best_match_3h[:, :, 5]

        best_match_4h_2t = best_match_4h[:, :, 0]
        best_match_4h_2d = best_match_4h[:, :, 1]
        best_match_4h_100u = best_match_4h[:, :, 2]/3.6
        best_match_4h_100v = best_match_4h[:, :, 3]/3.6
        best_match_4h_tp = best_match_4h[:, :, 4]
        best_match_4h_sp = best_match_4h[:, :, 5]

        best_match_5h_2t = best_match_5h[:, :, 0]
        best_match_5h_2d = best_match_5h[:, :, 1]
        best_match_5h_100u = best_match_5h[:, :, 2]/3.6
        best_match_5h_100v = best_match_5h[:, :, 3]/3.6
        best_match_5h_tp = best_match_5h[:, :, 4]
        best_match_5h_sp = best_match_5h[:, :, 5]

        best_match_6h_2t = best_match_6h[:, :, 0]
        best_match_6h_2d = best_match_6h[:, :, 1]
        best_match_6h_100u = best_match_6h[:, :, 2]/3.6
        best_match_6h_100v = best_match_6h[:, :, 3]/3.6
        best_match_6h_tp = best_match_6h[:, :, 4]
        best_match_6h_sp = best_match_6h[:, :, 5]

        best_match_7h_2t = best_match_7h[:, :, 0]
        best_match_7h_2d = best_match_7h[:, :, 1]
        best_match_7h_100u = best_match_7h[:, :, 2]/3.6
        best_match_7h_100v = best_match_7h[:, :, 3]/3.6
        best_match_7h_tp = best_match_7h[:, :, 4]
        best_match_7h_sp = best_match_7h[:, :, 5]

        best_match_8h_2t = best_match_8h[:, :, 0]
        best_match_8h_2d = best_match_8h[:, :, 1]
        best_match_8h_100u = best_match_8h[:, :, 2]/3.6
        best_match_8h_100v = best_match_8h[:, :, 3]/3.6
        best_match_8h_tp = best_match_8h[:, :, 4]
        best_match_8h_sp = best_match_8h[:, :, 5]

        best_match_9h_2t = best_match_9h[:, :, 0]
        best_match_9h_2d = best_match_9h[:, :, 1]
        best_match_9h_100u = best_match_9h[:, :, 2]/3.6
        best_match_9h_100v = best_match_9h[:, :, 3]/3.6
        best_match_9h_tp = best_match_9h[:, :, 4]
        best_match_9h_sp = best_match_9h[:, :, 5]

        best_match_10h_2t = best_match_10h[:, :, 0]
        best_match_10h_2d = best_match_10h[:, :, 1]
        best_match_10h_100u = best_match_10h[:, :, 2]/3.6
        best_match_10h_100v = best_match_10h[:, :, 3]/3.6
        best_match_10h_tp = best_match_10h[:, :, 4]
        best_match_10h_sp = best_match_10h[:, :, 5]
        
        best_match_11h_2t = best_match_11h[:, :, 0]
        best_match_11h_2d = best_match_11h[:, :, 1]
        best_match_11h_100u = best_match_11h[:, :, 2]/3.6
        best_match_11h_100v = best_match_11h[:, :, 3]/3.6
        best_match_11h_tp = best_match_11h[:, :, 4]
        best_match_11h_sp = best_match_11h[:, :, 5]
        
        best_match_12h_2t = best_match_12h[:, :, 0]
        best_match_12h_2d = best_match_12h[:, :, 1]
        best_match_12h_100u = best_match_12h[:, :, 2]/3.6
        best_match_12h_100v = best_match_12h[:, :, 3]/3.6
        best_match_12h_tp = best_match_12h[:, :, 4]
        best_match_12h_sp = best_match_12h[:, :, 5]
        
        
        best_match_13h_2t = best_match_13h[:, :, 0]
        best_match_13h_2d = best_match_13h[:, :, 1]
        best_match_13h_100u = best_match_13h[:, :, 2]/3.6
        best_match_13h_100v = best_match_13h[:, :, 3]/3.6
        best_match_13h_tp = best_match_13h[:, :, 4]
        best_match_13h_sp = best_match_13h[:, :, 5]
        

        best_match_14h_2t = best_match_14h[:, :, 0]
        best_match_14h_2d = best_match_14h[:, :, 1]
        best_match_14h_100u = best_match_14h[:, :, 2]/3.6
        best_match_14h_100v = best_match_14h[:, :, 3]/3.6
        best_match_14h_tp = best_match_14h[:, :, 4]
        best_match_14h_sp = best_match_14h[:, :, 5]
        
        best_match_15h_2t = best_match_15h[:, :, 0]
        best_match_15h_2d = best_match_15h[:, :, 1]
        best_match_15h_100u = best_match_15h[:, :, 2]/3.6
        best_match_15h_100v = best_match_15h[:, :, 3]/3.6
        best_match_15h_tp = best_match_15h[:, :, 4]
        best_match_15h_sp = best_match_15h[:, :, 5]
        

        best_match_16h_2t = best_match_16h[:, :, 0]
        best_match_16h_2d = best_match_16h[:, :, 1]
        best_match_16h_100u = best_match_16h[:, :, 2]/3.6
        best_match_16h_100v = best_match_16h[:, :, 3]/3.6
        best_match_16h_tp = best_match_16h[:, :, 4]
        best_match_16h_sp = best_match_16h[:, :, 5]
        

        best_match_17h_2t = best_match_17h[:, :, 0]
        best_match_17h_2d = best_match_17h[:, :, 1]
        best_match_17h_100u = best_match_17h[:, :, 2]/3.6
        best_match_17h_100v = best_match_17h[:, :, 3]/3.6
        best_match_17h_tp = best_match_17h[:, :, 4]
        best_match_17h_sp = best_match_17h[:, :, 5]
        

        best_match_18h_2t = best_match_18h[:, :, 0]
        best_match_18h_2d = best_match_18h[:, :, 1]
        best_match_18h_100u = best_match_18h[:, :, 2]/3.6
        best_match_18h_100v = best_match_18h[:, :, 3]/3.6
        best_match_18h_tp = best_match_18h[:, :, 4]
        best_match_18h_sp = best_match_18h[:, :, 5]
        
        best_match_19h_2t = best_match_19h[:, :, 0]
        best_match_19h_2d = best_match_19h[:, :, 1]
        best_match_19h_100u = best_match_19h[:, :, 2]/3.6
        best_match_19h_100v = best_match_19h[:, :, 3]/3.6
        best_match_19h_tp = best_match_19h[:, :, 4]
        best_match_19h_sp = best_match_19h[:, :, 5]
        
        best_match_20h_2t = best_match_20h[:, :, 0]
        best_match_20h_2d = best_match_20h[:, :, 1]
        best_match_20h_100u = best_match_20h[:, :, 2]/3.6
        best_match_20h_100v = best_match_20h[:, :, 3]/3.6
        best_match_20h_tp = best_match_20h[:, :, 4]
        best_match_20h_sp = best_match_20h[:, :, 5]
        
        ecmwf_ifs_1h_2t = ecmwf_ifs_1h[:, :, 0]
        ecmwf_ifs_1h_2d = ecmwf_ifs_1h[:, :, 1]
        ecmwf_ifs_1h_100u = ecmwf_ifs_1h[:, :, 2]/3.6
        ecmwf_ifs_1h_100v = ecmwf_ifs_1h[:, :, 3]/3.6
        ecmwf_ifs_1h_tp = ecmwf_ifs_1h[:, :, 4]
        ecmwf_ifs_1h_sp = ecmwf_ifs_1h[:, :, 5]
        
        ecmwf_ifs_2h_2t = ecmwf_ifs_2h[:, :, 0]
        ecmwf_ifs_2h_2d = ecmwf_ifs_2h[:, :, 1]
        ecmwf_ifs_2h_100u = ecmwf_ifs_2h[:, :, 2]/3.6
        ecmwf_ifs_2h_100v = ecmwf_ifs_2h[:, :, 3]/3.6
        ecmwf_ifs_2h_tp = ecmwf_ifs_2h[:, :, 4]
        ecmwf_ifs_2h_sp = ecmwf_ifs_2h[:, :, 5]
        
        ecmwf_ifs_3h_2t = ecmwf_ifs_3h[:, :, 0]
        ecmwf_ifs_3h_2d = ecmwf_ifs_3h[:, :, 1]
        ecmwf_ifs_3h_100u = ecmwf_ifs_3h[:, :, 2]/3.6
        ecmwf_ifs_3h_100v = ecmwf_ifs_3h[:, :, 3]/3.6
        ecmwf_ifs_3h_tp = ecmwf_ifs_3h[:, :, 4]
        ecmwf_ifs_3h_sp = ecmwf_ifs_3h[:, :, 5]
        
        ecmwf_ifs_4h_2t = ecmwf_ifs_4h[:, :, 0]
        ecmwf_ifs_4h_2d = ecmwf_ifs_4h[:, :, 1]
        ecmwf_ifs_4h_100u = ecmwf_ifs_4h[:, :, 2]/3.6
        ecmwf_ifs_4h_100v = ecmwf_ifs_4h[:, :, 3]/3.6
        ecmwf_ifs_4h_tp = ecmwf_ifs_4h[:, :, 4]
        ecmwf_ifs_4h_sp = ecmwf_ifs_4h[:, :, 5]
        
        ecmwf_ifs_5h_2t = ecmwf_ifs_5h[:, :, 0]
        ecmwf_ifs_5h_2d = ecmwf_ifs_5h[:, :, 1]
        ecmwf_ifs_5h_100u = ecmwf_ifs_5h[:, :, 2]/3.6
        ecmwf_ifs_5h_100v = ecmwf_ifs_5h[:, :, 3]/3.6
        ecmwf_ifs_5h_tp = ecmwf_ifs_5h[:, :, 4]
        ecmwf_ifs_5h_sp = ecmwf_ifs_5h[:, :, 5]

        ecmwf_ifs_6h_2t = ecmwf_ifs_6h[:, :, 0]
        ecmwf_ifs_6h_2d = ecmwf_ifs_6h[:, :, 1]
        ecmwf_ifs_6h_100u = ecmwf_ifs_6h[:, :, 2]/3.6
        ecmwf_ifs_6h_100v = ecmwf_ifs_6h[:, :, 3]/3.6
        ecmwf_ifs_6h_tp = ecmwf_ifs_6h[:, :, 4]
        ecmwf_ifs_6h_sp = ecmwf_ifs_6h[:, :, 5]
        
        ecmwf_ifs_7h_2t = ecmwf_ifs_7h[:, :, 0]
        ecmwf_ifs_7h_2d = ecmwf_ifs_7h[:, :, 1]
        ecmwf_ifs_7h_100u = ecmwf_ifs_7h[:, :, 2]/3.6
        ecmwf_ifs_7h_100v = ecmwf_ifs_7h[:, :, 3]/3.6
        ecmwf_ifs_7h_tp = ecmwf_ifs_7h[:, :, 4]
        ecmwf_ifs_7h_sp = ecmwf_ifs_7h[:, :, 5]
        
        ecmwf_ifs_8h_2t = ecmwf_ifs_8h[:, :, 0]
        ecmwf_ifs_8h_2d = ecmwf_ifs_8h[:, :, 1]
        ecmwf_ifs_8h_100u = ecmwf_ifs_8h[:, :, 2]/3.6
        ecmwf_ifs_8h_100v = ecmwf_ifs_8h[:, :, 3]/3.6
        ecmwf_ifs_8h_tp = ecmwf_ifs_8h[:, :, 4]
        ecmwf_ifs_8h_sp = ecmwf_ifs_8h[:, :, 5]
        
        ecmwf_ifs_9h_2t = ecmwf_ifs_9h[:, :, 0]
        ecmwf_ifs_9h_2d = ecmwf_ifs_9h[:, :, 1]
        ecmwf_ifs_9h_100u = ecmwf_ifs_9h[:, :, 2]/3.6
        ecmwf_ifs_9h_100v = ecmwf_ifs_9h[:, :, 3]/3.6
        ecmwf_ifs_9h_tp = ecmwf_ifs_9h[:, :, 4]
        ecmwf_ifs_9h_sp = ecmwf_ifs_9h[:, :, 5]
        
        ecmwf_ifs_10h_2t = ecmwf_ifs_10h[:, :, 0]
        ecmwf_ifs_10h_2d = ecmwf_ifs_10h[:, :, 1]
        ecmwf_ifs_10h_100u = ecmwf_ifs_10h[:, :, 2]/3.6
        ecmwf_ifs_10h_100v = ecmwf_ifs_10h[:, :, 3]/3.6
        ecmwf_ifs_10h_tp = ecmwf_ifs_10h[:, :, 4]
        ecmwf_ifs_10h_sp = ecmwf_ifs_10h[:, :, 5]
        
        ecmwf_ifs_11h_2t = ecmwf_ifs_11h[:, :, 0]
        ecmwf_ifs_11h_2d = ecmwf_ifs_11h[:, :, 1]
        ecmwf_ifs_11h_100u = ecmwf_ifs_11h[:, :, 2]/3.6
        ecmwf_ifs_11h_100v = ecmwf_ifs_11h[:, :, 3]/3.6
        ecmwf_ifs_11h_tp = ecmwf_ifs_11h[:, :, 4]
        ecmwf_ifs_11h_sp = ecmwf_ifs_11h[:, :, 5]
        
        ecmwf_ifs_12h_2t = ecmwf_ifs_12h[:, :, 0]
        ecmwf_ifs_12h_2d = ecmwf_ifs_12h[:, :, 1]
        ecmwf_ifs_12h_100u = ecmwf_ifs_12h[:, :, 2]/3.6
        ecmwf_ifs_12h_100v = ecmwf_ifs_12h[:, :, 3]/3.6
        ecmwf_ifs_12h_tp = ecmwf_ifs_12h[:, :, 4]
        ecmwf_ifs_12h_sp = ecmwf_ifs_12h[:, :, 5]
        
        ecmwf_ifs_13h_2t = ecmwf_ifs_13h[:, :, 0]
        ecmwf_ifs_13h_2d = ecmwf_ifs_13h[:, :, 1]
        ecmwf_ifs_13h_100u = ecmwf_ifs_13h[:, :, 2]/3.6
        ecmwf_ifs_13h_100v = ecmwf_ifs_13h[:, :, 3]/3.6
        ecmwf_ifs_13h_tp = ecmwf_ifs_13h[:, :, 4]
        ecmwf_ifs_13h_sp = ecmwf_ifs_13h[:, :, 5]
        
        ecmwf_ifs_14h_2t = ecmwf_ifs_14h[:, :, 0]
        ecmwf_ifs_14h_2d = ecmwf_ifs_14h[:, :, 1]
        ecmwf_ifs_14h_100u = ecmwf_ifs_14h[:, :, 2]/3.6
        ecmwf_ifs_14h_100v = ecmwf_ifs_14h[:, :, 3]/3.6
        ecmwf_ifs_14h_tp = ecmwf_ifs_14h[:, :, 4]
        ecmwf_ifs_14h_sp = ecmwf_ifs_14h[:, :, 5]
        
        ecmwf_ifs_15h_2t = ecmwf_ifs_15h[:, :, 0]
        ecmwf_ifs_15h_2d = ecmwf_ifs_15h[:, :, 1]
        ecmwf_ifs_15h_100u = ecmwf_ifs_15h[:, :, 2]/3.6
        ecmwf_ifs_15h_100v = ecmwf_ifs_15h[:, :, 3]/3.6
        ecmwf_ifs_15h_tp = ecmwf_ifs_15h[:, :, 4]
        ecmwf_ifs_15h_sp = ecmwf_ifs_15h[:, :, 5]
        
        ecmwf_ifs_16h_2t = ecmwf_ifs_16h[:, :, 0]
        ecmwf_ifs_16h_2d = ecmwf_ifs_16h[:, :, 1]
        ecmwf_ifs_16h_100u = ecmwf_ifs_16h[:, :, 2]/3.6
        ecmwf_ifs_16h_100v = ecmwf_ifs_16h[:, :, 3]/3.6
        ecmwf_ifs_16h_tp = ecmwf_ifs_16h[:, :, 4]
        ecmwf_ifs_16h_sp = ecmwf_ifs_16h[:, :, 5]

        ecmwf_ifs_17h_2t = ecmwf_ifs_17h[:, :, 0]
        ecmwf_ifs_17h_2d = ecmwf_ifs_17h[:, :, 1]
        ecmwf_ifs_17h_100u = ecmwf_ifs_17h[:, :, 2]/3.6
        ecmwf_ifs_17h_100v = ecmwf_ifs_17h[:, :, 3]/3.6
        ecmwf_ifs_17h_tp = ecmwf_ifs_17h[:, :, 4]
        ecmwf_ifs_17h_sp = ecmwf_ifs_17h[:, :, 5]
        
        ecmwf_ifs_18h_2t = ecmwf_ifs_18h[:, :, 0]
        ecmwf_ifs_18h_2d = ecmwf_ifs_18h[:, :, 1]
        ecmwf_ifs_18h_100u = ecmwf_ifs_18h[:, :, 2]/3.6
        ecmwf_ifs_18h_100v = ecmwf_ifs_18h[:, :, 3]/3.6
        ecmwf_ifs_18h_tp = ecmwf_ifs_18h[:, :, 4]
        ecmwf_ifs_18h_sp = ecmwf_ifs_18h[:, :, 5]
        
        ecmwf_ifs_19h_2t = ecmwf_ifs_19h[:, :, 0]
        ecmwf_ifs_19h_2d = ecmwf_ifs_19h[:, :, 1]
        ecmwf_ifs_19h_100u = ecmwf_ifs_19h[:, :, 2]/3.6
        ecmwf_ifs_19h_100v = ecmwf_ifs_19h[:, :, 3]/3.6
        ecmwf_ifs_19h_tp = ecmwf_ifs_19h[:, :, 4]
        ecmwf_ifs_19h_sp = ecmwf_ifs_19h[:, :, 5]
        
        ecmwf_ifs_20h_2t = ecmwf_ifs_20h[:, :, 0]
        ecmwf_ifs_20h_2d = ecmwf_ifs_20h[:, :, 1]
        ecmwf_ifs_20h_100u = ecmwf_ifs_20h[:, :, 2]/3.6
        ecmwf_ifs_20h_100v = ecmwf_ifs_20h[:, :, 3]/3.6
        ecmwf_ifs_20h_tp = ecmwf_ifs_20h[:, :, 4]
        ecmwf_ifs_20h_sp = ecmwf_ifs_20h[:, :, 5]

        gfs_global_1h_2t = gfs_global_1h[:, :, 0]
        gfs_global_1h_2d = gfs_global_1h[:, :, 1]
        gfs_global_1h_100u = gfs_global_1h[:, :, 2]/3.6
        gfs_global_1h_100v = gfs_global_1h[:, :, 3]/3.6
        gfs_global_1h_tp = gfs_global_1h[:, :, 4]
        gfs_global_1h_sp = gfs_global_1h[:, :, 5]
        
        gfs_global_2h_2t = gfs_global_2h[:, :, 0]
        gfs_global_2h_2d = gfs_global_2h[:, :, 1]
        gfs_global_2h_100u = gfs_global_2h[:, :, 2]/3.6
        gfs_global_2h_100v = gfs_global_2h[:, :, 3]/3.6
        gfs_global_2h_tp = gfs_global_2h[:, :, 4]
        gfs_global_2h_sp = gfs_global_2h[:, :, 5]
        
        gfs_global_3h_2t = gfs_global_3h[:, :, 0]
        gfs_global_3h_2d = gfs_global_3h[:, :, 1]
        gfs_global_3h_100u = gfs_global_3h[:, :, 2]/3.6
        gfs_global_3h_100v = gfs_global_3h[:, :, 3]/3.6
        gfs_global_3h_tp = gfs_global_3h[:, :, 4]
        gfs_global_3h_sp = gfs_global_3h[:, :, 5]
        
        gfs_global_4h_2t = gfs_global_4h[:, :, 0]
        gfs_global_4h_2d = gfs_global_4h[:, :, 1]
        gfs_global_4h_100u = gfs_global_4h[:, :, 2]/3.6
        gfs_global_4h_100v = gfs_global_4h[:, :, 3]/3.6
        gfs_global_4h_tp = gfs_global_4h[:, :, 4]
        gfs_global_4h_sp = gfs_global_4h[:, :, 5]
        
        gfs_global_5h_2t = gfs_global_5h[:, :, 0]
        gfs_global_5h_2d = gfs_global_5h[:, :, 1]
        gfs_global_5h_100u = gfs_global_5h[:, :, 2]/3.6
        gfs_global_5h_100v = gfs_global_5h[:, :, 3]/3.6
        gfs_global_5h_tp = gfs_global_5h[:, :, 4]
        gfs_global_5h_sp = gfs_global_5h[:, :, 5]

        gfs_global_6h_2t = gfs_global_6h[:, :, 0]
        gfs_global_6h_2d = gfs_global_6h[:, :, 1]
        gfs_global_6h_100u = gfs_global_6h[:, :, 2]/3.6
        gfs_global_6h_100v = gfs_global_6h[:, :, 3]/3.6
        gfs_global_6h_tp = gfs_global_6h[:, :, 4]
        gfs_global_6h_sp = gfs_global_6h[:, :, 5]

        gfs_global_7h_2t = gfs_global_7h[:, :, 0]
        gfs_global_7h_2d = gfs_global_7h[:, :, 1]
        gfs_global_7h_100u = gfs_global_7h[:, :, 2]/3.6
        gfs_global_7h_100v = gfs_global_7h[:, :, 3]/3.6
        gfs_global_7h_tp = gfs_global_7h[:, :, 4]
        gfs_global_7h_sp = gfs_global_7h[:, :, 5]
        
        gfs_global_8h_2t = gfs_global_8h[:, :, 0]
        gfs_global_8h_2d = gfs_global_8h[:, :, 1]
        gfs_global_8h_100u = gfs_global_8h[:, :, 2]/3.6
        gfs_global_8h_100v = gfs_global_8h[:, :, 3]/3.6
        gfs_global_8h_tp = gfs_global_8h[:, :, 4]
        gfs_global_8h_sp = gfs_global_8h[:, :, 5]
        
        gfs_global_9h_2t = gfs_global_9h[:, :, 0]
        gfs_global_9h_2d = gfs_global_9h[:, :, 1]
        gfs_global_9h_100u = gfs_global_9h[:, :, 2]/3.6
        gfs_global_9h_100v = gfs_global_9h[:, :, 3]/3.6
        gfs_global_9h_tp = gfs_global_9h[:, :, 4]
        gfs_global_9h_sp = gfs_global_9h[:, :, 5]
        
        gfs_global_10h_2t = gfs_global_10h[:, :, 0]
        gfs_global_10h_2d = gfs_global_10h[:, :, 1]
        gfs_global_10h_100u = gfs_global_10h[:, :, 2]/3.6
        gfs_global_10h_100v = gfs_global_10h[:, :, 3]/3.6
        gfs_global_10h_tp = gfs_global_10h[:, :, 4]
        gfs_global_10h_sp = gfs_global_10h[:, :, 5]
        
        gfs_global_11h_2t = gfs_global_11h[:, :, 0]
        gfs_global_11h_2d = gfs_global_11h[:, :, 1]
        gfs_global_11h_100u = gfs_global_11h[:, :, 2]/3.6
        gfs_global_11h_100v = gfs_global_11h[:, :, 3]/3.6
        gfs_global_11h_tp = gfs_global_11h[:, :, 4]
        gfs_global_11h_sp = gfs_global_11h[:, :, 5]
        
        gfs_global_12h_2t = gfs_global_12h[:, :, 0]
        gfs_global_12h_2d = gfs_global_12h[:, :, 1]
        gfs_global_12h_100u = gfs_global_12h[:, :, 2]/3.6
        gfs_global_12h_100v = gfs_global_12h[:, :, 3]/3.6
        gfs_global_12h_tp = gfs_global_12h[:, :, 4]
        gfs_global_12h_sp = gfs_global_12h[:, :, 5]
        
        gfs_global_13h_2t = gfs_global_13h[:, :, 0]
        gfs_global_13h_2d = gfs_global_13h[:, :, 1]
        gfs_global_13h_100u = gfs_global_13h[:, :, 2]/3.6
        gfs_global_13h_100v = gfs_global_13h[:, :, 3]/3.6
        gfs_global_13h_tp = gfs_global_13h[:, :, 4]
        gfs_global_13h_sp = gfs_global_13h[:, :, 5]
        
        gfs_global_14h_2t = gfs_global_14h[:, :, 0]
        gfs_global_14h_2d = gfs_global_14h[:, :, 1]
        gfs_global_14h_100u = gfs_global_14h[:, :, 2]/3.6
        gfs_global_14h_100v = gfs_global_14h[:, :, 3]/3.6
        gfs_global_14h_tp = gfs_global_14h[:, :, 4]
        gfs_global_14h_sp = gfs_global_14h[:, :, 5]
        
        gfs_global_15h_2t = gfs_global_15h[:, :, 0]
        gfs_global_15h_2d = gfs_global_15h[:, :, 1]
        gfs_global_15h_100u = gfs_global_15h[:, :, 2]/3.6
        gfs_global_15h_100v = gfs_global_15h[:, :, 3]/3.6
        gfs_global_15h_tp = gfs_global_15h[:, :, 4]
        gfs_global_15h_sp = gfs_global_15h[:, :, 5]
        
        gfs_global_16h_2t = gfs_global_16h[:, :, 0]
        gfs_global_16h_2d = gfs_global_16h[:, :, 1]
        gfs_global_16h_100u = gfs_global_16h[:, :, 2]/3.6
        gfs_global_16h_100v = gfs_global_16h[:, :, 3]/3.6
        gfs_global_16h_tp = gfs_global_16h[:, :, 4]
        gfs_global_16h_sp = gfs_global_16h[:, :, 5]
        
        gfs_global_17h_2t = gfs_global_17h[:, :, 0]
        gfs_global_17h_2d = gfs_global_17h[:, :, 1]
        gfs_global_17h_100u = gfs_global_17h[:, :, 2]/3.6
        gfs_global_17h_100v = gfs_global_17h[:, :, 3]/3.6
        gfs_global_17h_tp = gfs_global_17h[:, :, 4]
        gfs_global_17h_sp = gfs_global_17h[:, :, 5]
        
        gfs_global_18h_2t = gfs_global_18h[:, :, 0]
        gfs_global_18h_2d = gfs_global_18h[:, :, 1]
        gfs_global_18h_100u = gfs_global_18h[:, :, 2]/3.6
        gfs_global_18h_100v = gfs_global_18h[:, :, 3]/3.6
        gfs_global_18h_tp = gfs_global_18h[:, :, 4]
        gfs_global_18h_sp = gfs_global_18h[:, :, 5]
        
        gfs_global_19h_2t = gfs_global_19h[:, :, 0]
        gfs_global_19h_2d = gfs_global_19h[:, :, 1]
        gfs_global_19h_100u = gfs_global_19h[:, :, 2]/3.6
        gfs_global_19h_100v = gfs_global_19h[:, :, 3]/3.6
        gfs_global_19h_tp = gfs_global_19h[:, :, 4]
        gfs_global_19h_sp = gfs_global_19h[:, :, 5]
        
        gfs_global_20h_2t = gfs_global_20h[:, :, 0]
        gfs_global_20h_2d = gfs_global_20h[:, :, 1]
        gfs_global_20h_100u = gfs_global_20h[:, :, 2]/3.6
        gfs_global_20h_100v = gfs_global_20h[:, :, 3]/3.6
        gfs_global_20h_tp = gfs_global_20h[:, :, 4]
        gfs_global_20h_sp = gfs_global_20h[:, :, 5]
        
        graphcast_1h_2t = graphcast_1h[:, :, 0]
        graphcast_1h_2d = graphcast_1h[:, :, 1]
        graphcast_1h_100u = graphcast_1h[:, :, 2]/3.6
        graphcast_1h_100v = graphcast_1h[:, :, 3]/3.6
        graphcast_1h_tp = graphcast_1h[:, :, 4]
        graphcast_1h_sp = graphcast_1h[:, :, 5]
        
        graphcast_2h_2t = graphcast_2h[:, :, 0]
        graphcast_2h_2d = graphcast_2h[:, :, 1]
        graphcast_2h_100u = graphcast_2h[:, :, 2]/3.6
        graphcast_2h_100v = graphcast_2h[:, :, 3]/3.6
        graphcast_2h_tp = graphcast_2h[:, :, 4]
        graphcast_2h_sp = graphcast_2h[:, :, 5]
        
        graphcast_3h_2t = graphcast_3h[:, :, 0]
        graphcast_3h_2d = graphcast_3h[:, :, 1]
        graphcast_3h_100u = graphcast_3h[:, :, 2]/3.6
        graphcast_3h_100v = graphcast_3h[:, :, 3]/3.6
        graphcast_3h_tp = graphcast_3h[:, :, 4]
        graphcast_3h_sp = graphcast_3h[:, :, 5]
        
        graphcast_4h_2t = graphcast_4h[:, :, 0]
        graphcast_4h_2d = graphcast_4h[:, :, 1]
        graphcast_4h_100u = graphcast_4h[:, :, 2]/3.6
        graphcast_4h_100v = graphcast_4h[:, :, 3]/3.6
        graphcast_4h_tp = graphcast_4h[:, :, 4]
        graphcast_4h_sp = graphcast_4h[:, :, 5]
        
        graphcast_5h_2t = graphcast_5h[:, :, 0]
        graphcast_5h_2d = graphcast_5h[:, :, 1]
        graphcast_5h_100u = graphcast_5h[:, :, 2]/3.6
        graphcast_5h_100v = graphcast_5h[:, :, 3]/3.6
        graphcast_5h_tp = graphcast_5h[:, :, 4]
        graphcast_5h_sp = graphcast_5h[:, :, 5]
        
        graphcast_6h_2t = graphcast_6h[:, :, 0]
        graphcast_6h_2d = graphcast_6h[:, :, 1]
        graphcast_6h_100u = graphcast_6h[:, :, 2]/3.6
        graphcast_6h_100v = graphcast_6h[:, :, 3]/3.6
        graphcast_6h_tp = graphcast_6h[:, :, 4]
        graphcast_6h_sp = graphcast_6h[:, :, 5]
        
        graphcast_7h_2t = graphcast_7h[:, :, 0]
        graphcast_7h_2d = graphcast_7h[:, :, 1]
        graphcast_7h_100u = graphcast_7h[:, :, 2]/3.6
        graphcast_7h_100v = graphcast_7h[:, :, 3]/3.6
        graphcast_7h_tp = graphcast_7h[:, :, 4]
        graphcast_7h_sp = graphcast_7h[:, :, 5]
        
        graphcast_8h_2t = graphcast_8h[:, :, 0]
        graphcast_8h_2d = graphcast_8h[:, :, 1]
        graphcast_8h_100u = graphcast_8h[:, :, 2]/3.6
        graphcast_8h_100v = graphcast_8h[:, :, 3]/3.6
        graphcast_8h_tp = graphcast_8h[:, :, 4]
        graphcast_8h_sp = graphcast_8h[:, :, 5]
        
        
        graphcast_9h_2t = graphcast_9h[:, :, 0]
        graphcast_9h_2d = graphcast_9h[:, :, 1]
        graphcast_9h_100u = graphcast_9h[:, :, 2]/3.6
        graphcast_9h_100v = graphcast_9h[:, :, 3]/3.6
        graphcast_9h_tp = graphcast_9h[:, :, 4]
        graphcast_9h_sp = graphcast_9h[:, :, 5]
        
        graphcast_10h_2t = graphcast_10h[:, :, 0]
        graphcast_10h_2d = graphcast_10h[:, :, 1]
        graphcast_10h_100u = graphcast_10h[:, :, 2]/3.6
        graphcast_10h_100v = graphcast_10h[:, :, 3]/3.6
        graphcast_10h_tp = graphcast_10h[:, :, 4]
        graphcast_10h_sp = graphcast_10h[:, :, 5]
        
        graphcast_11h_2t = graphcast_11h[:, :, 0]
        graphcast_11h_2d = graphcast_11h[:, :, 1]
        graphcast_11h_100u = graphcast_11h[:, :, 2]/3.6
        graphcast_11h_100v = graphcast_11h[:, :, 3]/3.6
        graphcast_11h_tp = graphcast_11h[:, :, 4]
        graphcast_11h_sp = graphcast_11h[:, :, 5]
        
        graphcast_12h_2t = graphcast_12h[:, :, 0]
        graphcast_12h_2d = graphcast_12h[:, :, 1]
        graphcast_12h_100u = graphcast_12h[:, :, 2]/3.6
        graphcast_12h_100v = graphcast_12h[:, :, 3]/3.6
        graphcast_12h_tp = graphcast_12h[:, :, 4]
        graphcast_12h_sp = graphcast_12h[:, :, 5]
        
        graphcast_13h_2t = graphcast_13h[:, :, 0]
        graphcast_13h_2d = graphcast_13h[:, :, 1]
        graphcast_13h_100u = graphcast_13h[:, :, 2]/3.6
        graphcast_13h_100v = graphcast_13h[:, :, 3]/3.6
        graphcast_13h_tp = graphcast_13h[:, :, 4]
        graphcast_13h_sp = graphcast_13h[:, :, 5]
        
        graphcast_14h_2t = graphcast_14h[:, :, 0]
        graphcast_14h_2d = graphcast_14h[:, :, 1]
        graphcast_14h_100u = graphcast_14h[:, :, 2]/3.6
        graphcast_14h_100v = graphcast_14h[:, :, 3]/3.6
        graphcast_14h_tp = graphcast_14h[:, :, 4]
        graphcast_14h_sp = graphcast_14h[:, :, 5]

        graphcast_15h_2t = graphcast_15h[:, :, 0]
        graphcast_15h_2d = graphcast_15h[:, :, 1]
        graphcast_15h_100u = graphcast_15h[:, :, 2]/3.6
        graphcast_15h_100v = graphcast_15h[:, :, 3]/3.6
        graphcast_15h_tp = graphcast_15h[:, :, 4]
        graphcast_15h_sp = graphcast_15h[:, :, 5]
        
        graphcast_15h_2d = graphcast_15h[:, :, 1]
        graphcast_15h_100u = graphcast_15h[:, :, 2]/3.6
        graphcast_15h_100v = graphcast_15h[:, :, 3]/3.6
        graphcast_15h_tp = graphcast_15h[:, :, 4]
        graphcast_15h_sp = graphcast_15h[:, :, 5]
        
        graphcast_16h_2t = graphcast_16h[:, :, 0]
        graphcast_16h_2d = graphcast_16h[:, :, 1]
        graphcast_16h_100u = graphcast_16h[:, :, 2]/3.6
        graphcast_16h_100v = graphcast_16h[:, :, 3]/3.6
        graphcast_16h_tp = graphcast_16h[:, :, 4]
        graphcast_16h_sp = graphcast_16h[:, :, 5]

        graphcast_17h_2t = graphcast_17h[:, :, 0]
        graphcast_17h_2d = graphcast_17h[:, :, 1]
        graphcast_17h_100u = graphcast_17h[:, :, 2]/3.6
        graphcast_17h_100v = graphcast_17h[:, :, 3]/3.6
        graphcast_17h_tp = graphcast_17h[:, :, 4]
        graphcast_17h_sp = graphcast_17h[:, :, 5]
        
        graphcast_18h_2t = graphcast_18h[:, :, 0]
        graphcast_18h_2d = graphcast_18h[:, :, 1]
        graphcast_18h_100u = graphcast_18h[:, :, 2]/3.6
        graphcast_18h_100v = graphcast_18h[:, :, 3]/3.6
        graphcast_18h_tp = graphcast_18h[:, :, 4]
        graphcast_18h_sp = graphcast_18h[:, :, 5]
        
        graphcast_19h_2t = graphcast_19h[:, :, 0]
        graphcast_19h_2d = graphcast_19h[:, :, 1]
        graphcast_19h_100u = graphcast_19h[:, :, 2]/3.6
        graphcast_19h_100v = graphcast_19h[:, :, 3]/3.6
        graphcast_19h_tp = graphcast_19h[:, :, 4]
        graphcast_19h_sp = graphcast_19h[:, :, 5]
        
        graphcast_20h_2t = graphcast_20h[:, :, 0]
        graphcast_20h_2d = graphcast_20h[:, :, 1]
        graphcast_20h_100u = graphcast_20h[:, :, 2]/3.6
        graphcast_20h_100v = graphcast_20h[:, :, 3]/3.6
        graphcast_20h_tp = graphcast_20h[:, :, 4]
        graphcast_20h_sp = graphcast_20h[:, :, 5]
        
        aifs_1h_2t = aifs_1h[:, :, 0]
        aifs_1h_2d = aifs_1h[:, :, 1]
        aifs_1h_100u = aifs_1h[:, :, 2]/3.6
        aifs_1h_100v = aifs_1h[:, :, 3]/3.6
        aifs_1h_tp = aifs_1h[:, :, 4]
        aifs_1h_sp = aifs_1h[:, :, 5]
        
        aifs_2h_2t = aifs_2h[:, :, 0]
        aifs_2h_2d = aifs_2h[:, :, 1]
        aifs_2h_100u = aifs_2h[:, :, 2]/3.6
        aifs_2h_100v = aifs_2h[:, :, 3]/3.6
        aifs_2h_tp = aifs_2h[:, :, 4]
        aifs_2h_sp = aifs_2h[:, :, 5]
        
        aifs_3h_2t = aifs_3h[:, :, 0]
        aifs_3h_2d = aifs_3h[:, :, 1]
        aifs_3h_100u = aifs_3h[:, :, 2]/3.6
        aifs_3h_100v = aifs_3h[:, :, 3]/3.6
        aifs_3h_tp = aifs_3h[:, :, 4]
        aifs_3h_sp = aifs_3h[:, :, 5]
        
        aifs_4h_2t = aifs_4h[:, :, 0]
        aifs_4h_2d = aifs_4h[:, :, 1]
        aifs_4h_100u = aifs_4h[:, :, 2]/3.6
        aifs_4h_100v = aifs_4h[:, :, 3]/3.6
        aifs_4h_tp = aifs_4h[:, :, 4]
        aifs_4h_sp = aifs_4h[:, :, 5]
        
        aifs_5h_2t = aifs_5h[:, :, 0]
        aifs_5h_2d = aifs_5h[:, :, 1]
        aifs_5h_100u = aifs_5h[:, :, 2]/3.6
        aifs_5h_100v = aifs_5h[:, :, 3]/3.6
        aifs_5h_tp = aifs_5h[:, :, 4]
        aifs_5h_sp = aifs_5h[:, :, 5]

        aifs_6h_2t = aifs_6h[:, :, 0]
        aifs_6h_2d = aifs_6h[:, :, 1]
        aifs_6h_100u = aifs_6h[:, :, 2]/3.6
        aifs_6h_100v = aifs_6h[:, :, 3]/3.6
        aifs_6h_tp = aifs_6h[:, :, 4]
        aifs_6h_sp = aifs_6h[:, :, 5]
        
        aifs_7h_2t = aifs_7h[:, :, 0]
        aifs_7h_2d = aifs_7h[:, :, 1]
        aifs_7h_100u = aifs_7h[:, :, 2]/3.6
        aifs_7h_100v = aifs_7h[:, :, 3]/3.6
        aifs_7h_tp = aifs_7h[:, :, 4]
        aifs_7h_sp = aifs_7h[:, :, 5]
        
        aifs_8h_2t = aifs_8h[:, :, 0]
        aifs_8h_2d = aifs_8h[:, :, 1]
        aifs_8h_100u = aifs_8h[:, :, 2]/3.6
        aifs_8h_100v = aifs_8h[:, :, 3]/3.6
        aifs_8h_tp = aifs_8h[:, :, 4]
        aifs_8h_sp = aifs_8h[:, :, 5]
        
        aifs_9h_2t = aifs_9h[:, :, 0]
        aifs_9h_2d = aifs_9h[:, :, 1]
        aifs_9h_100u = aifs_9h[:, :, 2]/3.6
        aifs_9h_100v = aifs_9h[:, :, 3]/3.6
        aifs_9h_tp = aifs_9h[:, :, 4]
        aifs_9h_sp = aifs_9h[:, :, 5]
        
        aifs_10h_2t = aifs_10h[:, :, 0]
        aifs_10h_2d = aifs_10h[:, :, 1]
        aifs_10h_100u = aifs_10h[:, :, 2]/3.6
        aifs_10h_100v = aifs_10h[:, :, 3]/3.6
        aifs_10h_tp = aifs_10h[:, :, 4]
        aifs_10h_sp = aifs_10h[:, :, 5]
        
        aifs_11h_2t = aifs_11h[:, :, 0]
        aifs_11h_2d = aifs_11h[:, :, 1]
        aifs_11h_100u = aifs_11h[:, :, 2]/3.6
        aifs_11h_100v = aifs_11h[:, :, 3]/3.6
        aifs_11h_tp = aifs_11h[:, :, 4]
        aifs_11h_sp = aifs_11h[:, :, 5]
        
        aifs_12h_2t = aifs_12h[:, :, 0]
        aifs_12h_2d = aifs_12h[:, :, 1]
        aifs_12h_100u = aifs_12h[:, :, 2]/3.6
        aifs_12h_100v = aifs_12h[:, :, 3]/3.6
        aifs_12h_tp = aifs_12h[:, :, 4]
        aifs_12h_sp = aifs_12h[:, :, 5]
        
        aifs_13h_2t = aifs_13h[:, :, 0]
        aifs_13h_2d = aifs_13h[:, :, 1]
        aifs_13h_100u = aifs_13h[:, :, 2]/3.6
        aifs_13h_100v = aifs_13h[:, :, 3]/3.6
        aifs_13h_tp = aifs_13h[:, :, 4]
        aifs_13h_sp = aifs_13h[:, :, 5]
        
        aifs_14h_2t = aifs_14h[:, :, 0]
        aifs_14h_2d = aifs_14h[:, :, 1]
        aifs_14h_100u = aifs_14h[:, :, 2]/3.6
        aifs_14h_100v = aifs_14h[:, :, 3]/3.6
        aifs_14h_tp = aifs_14h[:, :, 4]
        aifs_14h_sp = aifs_14h[:, :, 5]
        
        aifs_15h_2t = aifs_15h[:, :, 0]
        aifs_15h_2d = aifs_15h[:, :, 1]
        aifs_15h_100u = aifs_15h[:, :, 2]/3.6
        aifs_15h_100v = aifs_15h[:, :, 3]/3.6
        aifs_15h_tp = aifs_15h[:, :, 4]
        aifs_15h_sp = aifs_15h[:, :, 5]
        
        aifs_16h_2t = aifs_16h[:, :, 0]
        aifs_16h_2d = aifs_16h[:, :, 1]
        aifs_16h_100u = aifs_16h[:, :, 2]/3.6
        aifs_16h_100v = aifs_16h[:, :, 3]/3.6
        aifs_16h_tp = aifs_16h[:, :, 4]
        aifs_16h_sp = aifs_16h[:, :, 5]
        
        aifs_17h_2t = aifs_17h[:, :, 0]
        aifs_17h_2d = aifs_17h[:, :, 1]
        aifs_17h_100u = aifs_17h[:, :, 2]/3.6
        aifs_17h_100v = aifs_17h[:, :, 3]/3.6
        aifs_17h_tp = aifs_17h[:, :, 4]
        aifs_17h_sp = aifs_17h[:, :, 5]
        
        aifs_18h_2t = aifs_18h[:, :, 0]
        aifs_18h_2d = aifs_18h[:, :, 1]
        aifs_18h_100u = aifs_18h[:, :, 2]/3.6
        aifs_18h_100v = aifs_18h[:, :, 3]/3.6
        aifs_18h_tp = aifs_18h[:, :, 4]
        aifs_18h_sp = aifs_18h[:, :, 5]
        
        aifs_19h_2t = aifs_19h[:, :, 0]
        aifs_19h_2d = aifs_19h[:, :, 1]
        aifs_19h_100u = aifs_19h[:, :, 2]/3.6
        aifs_19h_100v = aifs_19h[:, :, 3]/3.6
        aifs_19h_tp = aifs_19h[:, :, 4]
        aifs_19h_sp = aifs_19h[:, :, 5]
        
        aifs_20h_2t = aifs_20h[:, :, 0]
        aifs_20h_2d = aifs_20h[:, :, 1]
        aifs_20h_100u = aifs_20h[:, :, 2]/3.6
        aifs_20h_100v = aifs_20h[:, :, 3]/3.6
        aifs_20h_tp = aifs_20h[:, :, 4]
        aifs_20h_sp = aifs_20h[:, :, 5]

        era5_1h_2t = era5_1h[:, :, 0]
        era5_1h_2d = era5_1h[:, :, 1]
        era5_1h_100u = era5_1h[:, :, 2]/3.6
        era5_1h_100v = era5_1h[:, :, 3]/3.6
        era5_1h_tp = era5_1h[:, :, 4]
        era5_1h_sp = era5_1h[:, :, 5]
        
        era5_2h_2t = era5_2h[:, :, 0]
        era5_2h_2d = era5_2h[:, :, 1]
        era5_2h_100u = era5_2h[:, :, 2]/3.6
        era5_2h_100v = era5_2h[:, :, 3]/3.6
        era5_2h_tp = era5_2h[:, :, 4]
        era5_2h_sp = era5_2h[:, :, 5]

        era5_3h_2t = era5_3h[:, :, 0]
        era5_3h_2d = era5_3h[:, :, 1]
        era5_3h_100u = era5_3h[:, :, 2]/3.6
        era5_3h_100v = era5_3h[:, :, 3]/3.6
        era5_3h_tp = era5_3h[:, :, 4]
        era5_3h_sp = era5_3h[:, :, 5]
        

        era5_4h_2t = era5_4h[:, :, 0]
        era5_4h_2d = era5_4h[:, :, 1]
        era5_4h_100u = era5_4h[:, :, 2]/3.6
        era5_4h_100v = era5_4h[:, :, 3]/3.6
        era5_4h_tp = era5_4h[:, :, 4]
        era5_4h_sp = era5_4h[:, :, 5]
        

        era5_5h_2t = era5_5h[:, :, 0]
        era5_5h_2d = era5_5h[:, :, 1]
        era5_5h_100u = era5_5h[:, :, 2]/3.6
        era5_5h_100v = era5_5h[:, :, 3]/3.6
        era5_5h_tp = era5_5h[:, :, 4]
        era5_5h_sp = era5_5h[:, :, 5]
        

        era5_6h_2t = era5_6h[:, :, 0]
        era5_6h_2d = era5_6h[:, :, 1]
        era5_6h_100u = era5_6h[:, :, 2]/3.6
        era5_6h_100v = era5_6h[:, :, 3]/3.6
        era5_6h_tp = era5_6h[:, :, 4]
        era5_6h_sp = era5_6h[:, :, 5]

        era5_7h_2t = era5_7h[:, :, 0]
        era5_7h_2d = era5_7h[:, :, 1]
        era5_7h_100u = era5_7h[:, :, 2]/3.6
        era5_7h_100v = era5_7h[:, :, 3]/3.6
        era5_7h_tp = era5_7h[:, :, 4]
        era5_7h_sp = era5_7h[:, :, 5]
        
        
        era5_8h_2t = era5_8h[:, :, 0]
        era5_8h_2d = era5_8h[:, :, 1]
        era5_8h_100u = era5_8h[:, :, 2]/3.6
        era5_8h_100v = era5_8h[:, :, 3]/3.6
        era5_8h_tp = era5_8h[:, :, 4]
        era5_8h_sp = era5_8h[:, :, 5]
        
        
        era5_9h_2t = era5_9h[:, :, 0]
        era5_9h_2d = era5_9h[:, :, 1]
        era5_9h_100u = era5_9h[:, :, 2]/3.6
        era5_9h_100v = era5_9h[:, :, 3]/3.6
        era5_9h_tp = era5_9h[:, :, 4]
        era5_9h_sp = era5_9h[:, :, 5]

        era5_10h_2t = era5_10h[:, :, 0]
        era5_10h_2d = era5_10h[:, :, 1]
        era5_10h_100u = era5_10h[:, :, 2]/3.6
        era5_10h_100v = era5_10h[:, :, 3]/3.6
        era5_10h_tp = era5_10h[:, :, 4]
        era5_10h_sp = era5_10h[:, :, 5]
        
        era5_11h_2t = era5_11h[:, :, 0]
        era5_11h_2d = era5_11h[:, :, 1]
        era5_11h_100u = era5_11h[:, :, 2]/3.6
        era5_11h_100v = era5_11h[:, :, 3]/3.6
        era5_11h_tp = era5_11h[:, :, 4]
        era5_11h_sp = era5_11h[:, :, 5]
        
        
        era5_12h_2t = era5_12h[:, :, 0]
        era5_12h_2d = era5_12h[:, :, 1]
        era5_12h_100u = era5_12h[:, :, 2]/3.6
        era5_12h_100v = era5_12h[:, :, 3]/3.6
        era5_12h_tp = era5_12h[:, :, 4]
        era5_12h_sp = era5_12h[:, :, 5]
        
        
        era5_13h_2t = era5_13h[:, :, 0]
        era5_13h_2d = era5_13h[:, :, 1]
        era5_13h_100u = era5_13h[:, :, 2]/3.6
        era5_13h_100v = era5_13h[:, :, 3]/3.6
        era5_13h_tp = era5_13h[:, :, 4]
        era5_13h_sp = era5_13h[:, :, 5]
        
        
        era5_14h_2t = era5_14h[:, :, 0]
        era5_14h_2d = era5_14h[:, :, 1]
        era5_14h_100u = era5_14h[:, :, 2]/3.6
        era5_14h_100v = era5_14h[:, :, 3]/3.6
        era5_14h_tp = era5_14h[:, :, 4]
        era5_14h_sp = era5_14h[:, :, 5]
        
        
        
        era5_15h_2t = era5_15h[:, :, 0]
        era5_15h_2d = era5_15h[:, :, 1]
        era5_15h_100u = era5_15h[:, :, 2]/3.6
        era5_15h_100v = era5_15h[:, :, 3]/3.6
        era5_15h_tp = era5_15h[:, :, 4]
        era5_15h_sp = era5_15h[:, :, 5]


        era5_16h_2t = era5_16h[:, :, 0]
        era5_16h_2d = era5_16h[:, :, 1]
        era5_16h_100u = era5_16h[:, :, 2]/3.6
        era5_16h_100v = era5_16h[:, :, 3]/3.6
        era5_16h_tp = era5_16h[:, :, 4]
        era5_16h_sp = era5_16h[:, :, 5]

        era5_17h_2t = era5_17h[:, :, 0]
        era5_17h_2d = era5_17h[:, :, 1]
        era5_17h_100u = era5_17h[:, :, 2]/3.6
        era5_17h_100v = era5_17h[:, :, 3]/3.6
        era5_17h_tp = era5_17h[:, :, 4]
        era5_17h_sp = era5_17h[:, :, 5]
        
        
        era5_18h_2t = era5_18h[:, :, 0]
        era5_18h_2d = era5_18h[:, :, 1]
        era5_18h_100u = era5_18h[:, :, 2]/3.6
        era5_18h_100v = era5_18h[:, :, 3]/3.6
        era5_18h_tp = era5_18h[:, :, 4]
        era5_18h_sp = era5_18h[:, :, 5]
        
        era5_19h_2t = era5_19h[:, :, 0]
        era5_19h_2d = era5_19h[:, :, 1]
        era5_19h_100u = era5_19h[:, :, 2]/3.6
        era5_19h_100v = era5_19h[:, :, 3]/3.6
        era5_19h_tp = era5_19h[:, :, 4]
        era5_19h_sp = era5_19h[:, :, 5]
        
        era5_20h_2t = era5_20h[:, :, 0]
        era5_20h_2d = era5_20h[:, :, 1]
        era5_20h_100u = era5_20h[:, :, 2]/3.6
        era5_20h_100v = era5_20h[:, :, 3]/3.6
        era5_20h_tp = era5_20h[:, :, 4]
        era5_20h_sp = era5_20h[:, :, 5]
        
        # Group features by hour and create one PNG per hour
        # Hours: 1h, 2h, 3h, 4h, 5h, 6h, 7h, 8h, 9h, 10h, 11h, 12h, 13h, 14h, 15h, 16h, 17h, 18h, 19h, 20h
        hours_data = {
            '1h': {
                'best_match': [best_match_1h_2t, best_match_1h_2d, best_match_1h_100u, best_match_1h_100v, best_match_1h_tp, best_match_1h_sp],
                'ecmwf_ifs': [ecmwf_ifs_1h_2t, ecmwf_ifs_1h_2d, ecmwf_ifs_1h_100u, ecmwf_ifs_1h_100v, ecmwf_ifs_1h_tp, ecmwf_ifs_1h_sp],
                'gfs_global': [gfs_global_1h_2t, gfs_global_1h_2d, gfs_global_1h_100u, gfs_global_1h_100v, gfs_global_1h_tp, gfs_global_1h_sp],
                'graphcast': [graphcast_1h_2t, graphcast_1h_2d, graphcast_1h_100u, graphcast_1h_100v, graphcast_1h_tp, graphcast_1h_sp],
                'aifs': [aifs_1h_2t, aifs_1h_2d, aifs_1h_100u, aifs_1h_100v, aifs_1h_tp, aifs_1h_sp],
                'era5': [era5_1h_2t, era5_1h_2d, era5_1h_100u, era5_1h_100v, era5_1h_tp, era5_1h_sp],
                'feature_names': ['temperature_2t', 'dewpoint_2d', 'u100_100u', 'v100_100v', 'precipitation_tp', 'sp']
            },
            '2h': {
                'best_match': [best_match_2h_2t, best_match_2h_2d, best_match_2h_100u, best_match_2h_100v, best_match_2h_tp, best_match_2h_sp],
                'ecmwf_ifs': [ecmwf_ifs_2h_2t, ecmwf_ifs_2h_2d, ecmwf_ifs_2h_100u, ecmwf_ifs_2h_100v, ecmwf_ifs_2h_tp, ecmwf_ifs_2h_sp],
                'gfs_global': [gfs_global_2h_2t, gfs_global_2h_2d, gfs_global_2h_100u, gfs_global_2h_100v, gfs_global_2h_tp, gfs_global_2h_sp],
                'graphcast': [graphcast_2h_2t, graphcast_2h_2d, graphcast_2h_100u, graphcast_2h_100v, graphcast_2h_tp, graphcast_2h_sp],
                'aifs': [aifs_2h_2t, aifs_2h_2d, aifs_2h_100u, aifs_2h_100v, aifs_2h_tp, aifs_2h_sp],
                'era5': [era5_2h_2t, era5_2h_2d, era5_2h_100u, era5_2h_100v, era5_2h_tp, era5_2h_sp],
                'feature_names': ['temperature_2t', 'dewpoint_2d', 'u100_100u', 'v100_100v', 'precipitation_tp', 'sp']
            },
            '3h': {
                'best_match': [best_match_3h_2t, best_match_3h_2d, best_match_3h_100u, best_match_3h_100v, best_match_3h_tp, best_match_3h_sp],
                'ecmwf_ifs': [ecmwf_ifs_3h_2t, ecmwf_ifs_3h_2d, ecmwf_ifs_3h_100u, ecmwf_ifs_3h_100v, ecmwf_ifs_3h_tp, ecmwf_ifs_3h_sp],
                'gfs_global': [gfs_global_3h_2t, gfs_global_3h_2d, gfs_global_3h_100u, gfs_global_3h_100v, gfs_global_3h_tp, gfs_global_3h_sp],
                'graphcast': [graphcast_3h_2t, graphcast_3h_2d, graphcast_3h_100u, graphcast_3h_100v, graphcast_3h_tp, graphcast_3h_sp],
                'aifs': [aifs_3h_2t, aifs_3h_2d, aifs_3h_100u, aifs_3h_100v, aifs_3h_tp, aifs_3h_sp],
                'era5': [era5_3h_2t, era5_3h_2d, era5_3h_100u, era5_3h_100v, era5_3h_tp, era5_3h_sp],
                'feature_names': ['temperature_2t', 'dewpoint_2d', 'u100_100u', 'v100_100v', 'precipitation_tp', 'sp']
            },
            '4h': {
                'best_match': [best_match_4h_2t, best_match_4h_2d, best_match_4h_100u, best_match_4h_100v, best_match_4h_tp, best_match_4h_sp],
                'ecmwf_ifs': [ecmwf_ifs_4h_2t, ecmwf_ifs_4h_2d, ecmwf_ifs_4h_100u, ecmwf_ifs_4h_100v, ecmwf_ifs_4h_tp, ecmwf_ifs_4h_sp],
                'gfs_global': [gfs_global_4h_2t, gfs_global_4h_2d, gfs_global_4h_100u, gfs_global_4h_100v, gfs_global_4h_tp, gfs_global_4h_sp],
                'graphcast': [graphcast_4h_2t, graphcast_4h_2d, graphcast_4h_100u, graphcast_4h_100v, graphcast_4h_tp, graphcast_4h_sp],
                'aifs': [aifs_4h_2t, aifs_4h_2d, aifs_4h_100u, aifs_4h_100v, aifs_4h_tp, aifs_4h_sp],
                'era5': [era5_4h_2t, era5_4h_2d, era5_4h_100u, era5_4h_100v, era5_4h_tp, era5_4h_sp],
                'feature_names': ['temperature_2t', 'dewpoint_2d', 'u100_100u', 'v100_100v', 'precipitation_tp', 'sp']
            },
            '5h': {
                'best_match': [best_match_5h_2t, best_match_5h_2d, best_match_5h_100u, best_match_5h_100v, best_match_5h_tp, best_match_5h_sp],
                'ecmwf_ifs': [ecmwf_ifs_5h_2t, ecmwf_ifs_5h_2d, ecmwf_ifs_5h_100u, ecmwf_ifs_5h_100v, ecmwf_ifs_5h_tp, ecmwf_ifs_5h_sp],
                'gfs_global': [gfs_global_5h_2t, gfs_global_5h_2d, gfs_global_5h_100u, gfs_global_5h_100v, gfs_global_5h_tp, gfs_global_5h_sp],
                'graphcast': [graphcast_5h_2t, graphcast_5h_2d, graphcast_5h_100u, graphcast_5h_100v, graphcast_5h_tp, graphcast_5h_sp],
                'aifs': [aifs_5h_2t, aifs_5h_2d, aifs_5h_100u, aifs_5h_100v, aifs_5h_tp, aifs_5h_sp],
                'era5': [era5_5h_2t, era5_5h_2d, era5_5h_100u, era5_5h_100v, era5_5h_tp, era5_5h_sp],
                'feature_names': ['temperature_2t', 'dewpoint_2d', 'u100_100u', 'v100_100v', 'precipitation_tp', 'sp']
            },
            '6h': {
                'best_match': [best_match_6h_2t, best_match_6h_2d, best_match_6h_100u, best_match_6h_100v, best_match_6h_tp, best_match_6h_sp],
                'ecmwf_ifs': [ecmwf_ifs_6h_2t, ecmwf_ifs_6h_2d, ecmwf_ifs_6h_100u, ecmwf_ifs_6h_100v, ecmwf_ifs_6h_tp, ecmwf_ifs_6h_sp],
                'gfs_global': [gfs_global_6h_2t, gfs_global_6h_2d, gfs_global_6h_100u, gfs_global_6h_100v, gfs_global_6h_tp, gfs_global_6h_sp],
                'graphcast': [graphcast_6h_2t, graphcast_6h_2d, graphcast_6h_100u, graphcast_6h_100v, graphcast_6h_tp, graphcast_6h_sp],
                'aifs': [aifs_6h_2t, aifs_6h_2d, aifs_6h_100u, aifs_6h_100v, aifs_6h_tp, aifs_6h_sp],
                'era5': [era5_6h_2t, era5_6h_2d, era5_6h_100u, era5_6h_100v, era5_6h_tp, era5_6h_sp],
                'feature_names': ['temperature_2t', 'dewpoint_2d', 'u100_100u', 'v100_100v', 'precipitation_tp', 'sp']
            },
            '7h': {
                'best_match': [best_match_7h_2t, best_match_7h_2d, best_match_7h_100u, best_match_7h_100v, best_match_7h_tp, best_match_7h_sp],
                'ecmwf_ifs': [ecmwf_ifs_7h_2t, ecmwf_ifs_7h_2d, ecmwf_ifs_7h_100u, ecmwf_ifs_7h_100v, ecmwf_ifs_7h_tp, ecmwf_ifs_7h_sp],
                'gfs_global': [gfs_global_7h_2t, gfs_global_7h_2d, gfs_global_7h_100u, gfs_global_7h_100v, gfs_global_7h_tp, gfs_global_7h_sp],
                'graphcast': [graphcast_7h_2t, graphcast_7h_2d, graphcast_7h_100u, graphcast_7h_100v, graphcast_7h_tp, graphcast_7h_sp],
                'aifs': [aifs_7h_2t, aifs_7h_2d, aifs_7h_100u, aifs_7h_100v, aifs_7h_tp, aifs_7h_sp],
                'era5': [era5_7h_2t, era5_7h_2d, era5_7h_100u, era5_7h_100v, era5_7h_tp, era5_7h_sp],
                'feature_names': ['temperature_2t', 'dewpoint_2d', 'u100_100u', 'v100_100v', 'precipitation_tp', 'sp']
            },
            '8h': {
                'best_match': [best_match_8h_2t, best_match_8h_2d, best_match_8h_100u, best_match_8h_100v, best_match_8h_tp, best_match_8h_sp],
                'ecmwf_ifs': [ecmwf_ifs_8h_2t, ecmwf_ifs_8h_2d, ecmwf_ifs_8h_100u, ecmwf_ifs_8h_100v, ecmwf_ifs_8h_tp, ecmwf_ifs_8h_sp],
                'gfs_global': [gfs_global_8h_2t, gfs_global_8h_2d, gfs_global_8h_100u, gfs_global_8h_100v, gfs_global_8h_tp, gfs_global_8h_sp],
                'graphcast': [graphcast_8h_2t, graphcast_8h_2d, graphcast_8h_100u, graphcast_8h_100v, graphcast_8h_tp, graphcast_8h_sp],
                'aifs': [aifs_8h_2t, aifs_8h_2d, aifs_8h_100u, aifs_8h_100v, aifs_8h_tp, aifs_8h_sp],
                'era5': [era5_8h_2t, era5_8h_2d, era5_8h_100u, era5_8h_100v, era5_8h_tp, era5_8h_sp],
                'feature_names': ['temperature_2t', 'dewpoint_2d', 'u100_100u', 'v100_100v', 'precipitation_tp', 'sp']
            },
            '9h': {
                'best_match': [best_match_9h_2t, best_match_9h_2d, best_match_9h_100u, best_match_9h_100v, best_match_9h_tp, best_match_9h_sp],
                'ecmwf_ifs': [ecmwf_ifs_9h_2t, ecmwf_ifs_9h_2d, ecmwf_ifs_9h_100u, ecmwf_ifs_9h_100v, ecmwf_ifs_9h_tp, ecmwf_ifs_9h_sp],
                'gfs_global': [gfs_global_9h_2t, gfs_global_9h_2d, gfs_global_9h_100u, gfs_global_9h_100v, gfs_global_9h_tp, gfs_global_9h_sp],
                'graphcast': [graphcast_9h_2t, graphcast_9h_2d, graphcast_9h_100u, graphcast_9h_100v, graphcast_9h_tp, graphcast_9h_sp],
                'aifs': [aifs_9h_2t, aifs_9h_2d, aifs_9h_100u, aifs_9h_100v, aifs_9h_tp, aifs_9h_sp],
                'era5': [era5_9h_2t, era5_9h_2d, era5_9h_100u, era5_9h_100v, era5_9h_tp, era5_9h_sp],
                'feature_names': ['temperature_2t', 'dewpoint_2d', 'u100_100u', 'v100_100v', 'precipitation_tp', 'sp']
            },
            '10h': {
                'best_match': [best_match_10h_2t, best_match_10h_2d, best_match_10h_100u, best_match_10h_100v, best_match_10h_tp, best_match_10h_sp],
                'ecmwf_ifs': [ecmwf_ifs_10h_2t, ecmwf_ifs_10h_2d, ecmwf_ifs_10h_100u, ecmwf_ifs_10h_100v, ecmwf_ifs_10h_tp, ecmwf_ifs_10h_sp],
                'gfs_global': [gfs_global_10h_2t, gfs_global_10h_2d, gfs_global_10h_100u, gfs_global_10h_100v, gfs_global_10h_tp, gfs_global_10h_sp],
                'graphcast': [graphcast_10h_2t, graphcast_10h_2d, graphcast_10h_100u, graphcast_10h_100v, graphcast_10h_tp, graphcast_10h_sp],
                'aifs': [aifs_10h_2t, aifs_10h_2d, aifs_10h_100u, aifs_10h_100v, aifs_10h_tp, aifs_10h_sp],
                'era5': [era5_10h_2t, era5_10h_2d, era5_10h_100u, era5_10h_100v, era5_10h_tp, era5_10h_sp],
                'feature_names': ['temperature_2t', 'dewpoint_2d', 'u100_100u', 'v100_100v', 'precipitation_tp', 'sp']
            },
            '11h': {
                'best_match': [best_match_11h_2t, best_match_11h_2d, best_match_11h_100u, best_match_11h_100v, best_match_11h_tp, best_match_11h_sp],
                'ecmwf_ifs': [ecmwf_ifs_11h_2t, ecmwf_ifs_11h_2d, ecmwf_ifs_11h_100u, ecmwf_ifs_11h_100v, ecmwf_ifs_11h_tp, ecmwf_ifs_11h_sp],
                'gfs_global': [gfs_global_11h_2t, gfs_global_11h_2d, gfs_global_11h_100u, gfs_global_11h_100v, gfs_global_11h_tp, gfs_global_11h_sp],
                'graphcast': [graphcast_11h_2t, graphcast_11h_2d, graphcast_11h_100u, graphcast_11h_100v, graphcast_11h_tp, graphcast_11h_sp],
                'aifs': [aifs_11h_2t, aifs_11h_2d, aifs_11h_100u, aifs_11h_100v, aifs_11h_tp, aifs_11h_sp],
                'era5': [era5_11h_2t, era5_11h_2d, era5_11h_100u, era5_11h_100v, era5_11h_tp, era5_11h_sp],
                'feature_names': ['temperature_2t', 'dewpoint_2d', 'u100_100u', 'v100_100v', 'precipitation_tp', 'sp']
            },
            '12h': {
                'best_match': [best_match_12h_2t, best_match_12h_2d, best_match_12h_100u, best_match_12h_100v, best_match_12h_tp, best_match_12h_sp],
                'ecmwf_ifs': [ecmwf_ifs_12h_2t, ecmwf_ifs_12h_2d, ecmwf_ifs_12h_100u, ecmwf_ifs_12h_100v, ecmwf_ifs_12h_tp, ecmwf_ifs_12h_sp],
                'gfs_global': [gfs_global_12h_2t, gfs_global_12h_2d, gfs_global_12h_100u, gfs_global_12h_100v, gfs_global_12h_tp, gfs_global_12h_sp],
                'graphcast': [graphcast_12h_2t, graphcast_12h_2d, graphcast_12h_100u, graphcast_12h_100v, graphcast_12h_tp, graphcast_12h_sp],
                'aifs': [aifs_12h_2t, aifs_12h_2d, aifs_12h_100u, aifs_12h_100v, aifs_12h_tp, aifs_12h_sp],
                'era5': [era5_12h_2t, era5_12h_2d, era5_12h_100u, era5_12h_100v, era5_12h_tp, era5_12h_sp],
                'feature_names': ['temperature_2t', 'dewpoint_2d', 'u100_100u', 'v100_100v', 'precipitation_tp', 'sp']
            },
            '13h': {
                'best_match': [best_match_13h_2t, best_match_13h_2d, best_match_13h_100u, best_match_13h_100v, best_match_13h_tp, best_match_13h_sp],
                'ecmwf_ifs': [ecmwf_ifs_13h_2t, ecmwf_ifs_13h_2d, ecmwf_ifs_13h_100u, ecmwf_ifs_13h_100v, ecmwf_ifs_13h_tp, ecmwf_ifs_13h_sp],
                'gfs_global': [gfs_global_13h_2t, gfs_global_13h_2d, gfs_global_13h_100u, gfs_global_13h_100v, gfs_global_13h_tp, gfs_global_13h_sp],
                'graphcast': [graphcast_13h_2t, graphcast_13h_2d, graphcast_13h_100u, graphcast_13h_100v, graphcast_13h_tp, graphcast_13h_sp],
                'aifs': [aifs_13h_2t, aifs_13h_2d, aifs_13h_100u, aifs_13h_100v, aifs_13h_tp, aifs_13h_sp],
                'era5': [era5_13h_2t, era5_13h_2d, era5_13h_100u, era5_13h_100v, era5_13h_tp, era5_13h_sp],
                'feature_names': ['temperature_2t', 'dewpoint_2d', 'u100_100u', 'v100_100v', 'precipitation_tp', 'sp']
            },
            '14h': {
                'best_match': [best_match_14h_2t, best_match_14h_2d, best_match_14h_100u, best_match_14h_100v, best_match_14h_tp, best_match_14h_sp],
                'ecmwf_ifs': [ecmwf_ifs_14h_2t, ecmwf_ifs_14h_2d, ecmwf_ifs_14h_100u, ecmwf_ifs_14h_100v, ecmwf_ifs_14h_tp, ecmwf_ifs_14h_sp],
                'gfs_global': [gfs_global_14h_2t, gfs_global_14h_2d, gfs_global_14h_100u, gfs_global_14h_100v, gfs_global_14h_tp, gfs_global_14h_sp],
                'graphcast': [graphcast_14h_2t, graphcast_14h_2d, graphcast_14h_100u, graphcast_14h_100v, graphcast_14h_tp, graphcast_14h_sp],
                'aifs': [aifs_14h_2t, aifs_14h_2d, aifs_14h_100u, aifs_14h_100v, aifs_14h_tp, aifs_14h_sp],
                'era5': [era5_14h_2t, era5_14h_2d, era5_14h_100u, era5_14h_100v, era5_14h_tp, era5_14h_sp],
                'feature_names': ['temperature_2t', 'dewpoint_2d', 'u100_100u', 'v100_100v', 'precipitation_tp', 'sp']
            },
            '15h': {
                'best_match': [best_match_15h_2t, best_match_15h_2d, best_match_15h_100u, best_match_15h_100v, best_match_15h_tp, best_match_15h_sp],
                'ecmwf_ifs': [ecmwf_ifs_15h_2t, ecmwf_ifs_15h_2d, ecmwf_ifs_15h_100u, ecmwf_ifs_15h_100v, ecmwf_ifs_15h_tp, ecmwf_ifs_15h_sp],
                'gfs_global': [gfs_global_15h_2t, gfs_global_15h_2d, gfs_global_15h_100u, gfs_global_15h_100v, gfs_global_15h_tp, gfs_global_15h_sp],
                'graphcast': [graphcast_15h_2t, graphcast_15h_2d, graphcast_15h_100u, graphcast_15h_100v, graphcast_15h_tp, graphcast_15h_sp],
                'aifs': [aifs_15h_2t, aifs_15h_2d, aifs_15h_100u, aifs_15h_100v, aifs_15h_tp, aifs_15h_sp],
                'era5': [era5_15h_2t, era5_15h_2d, era5_15h_100u, era5_15h_100v, era5_15h_tp, era5_15h_sp],
                'feature_names': ['temperature_2t', 'dewpoint_2d', 'u100_100u', 'v100_100v', 'precipitation_tp', 'sp']
            },
            '16h': {
                'best_match': [best_match_16h_2t, best_match_16h_2d, best_match_16h_100u, best_match_16h_100v, best_match_16h_tp, best_match_16h_sp],
                'ecmwf_ifs': [ecmwf_ifs_16h_2t, ecmwf_ifs_16h_2d, ecmwf_ifs_16h_100u, ecmwf_ifs_16h_100v, ecmwf_ifs_16h_tp, ecmwf_ifs_16h_sp],
                'gfs_global': [gfs_global_16h_2t, gfs_global_16h_2d, gfs_global_16h_100u, gfs_global_16h_100v, gfs_global_16h_tp, gfs_global_16h_sp],
                'graphcast': [graphcast_16h_2t, graphcast_16h_2d, graphcast_16h_100u, graphcast_16h_100v, graphcast_16h_tp, graphcast_16h_sp],
                'aifs': [aifs_16h_2t, aifs_16h_2d, aifs_16h_100u, aifs_16h_100v, aifs_16h_tp, aifs_16h_sp],
                'era5': [era5_16h_2t, era5_16h_2d, era5_16h_100u, era5_16h_100v, era5_16h_tp, era5_16h_sp],
                'feature_names': ['temperature_2t', 'dewpoint_2d', 'u100_100u', 'v100_100v', 'precipitation_tp', 'sp']
            },
            '17h': {
                'best_match': [best_match_17h_2t, best_match_17h_2d, best_match_17h_100u, best_match_17h_100v, best_match_17h_tp, best_match_17h_sp],
                'ecmwf_ifs': [ecmwf_ifs_17h_2t, ecmwf_ifs_17h_2d, ecmwf_ifs_17h_100u, ecmwf_ifs_17h_100v, ecmwf_ifs_17h_tp, ecmwf_ifs_17h_sp],
                'gfs_global': [gfs_global_17h_2t, gfs_global_17h_2d, gfs_global_17h_100u, gfs_global_17h_100v, gfs_global_17h_tp, gfs_global_17h_sp],
                'graphcast': [graphcast_17h_2t, graphcast_17h_2d, graphcast_17h_100u, graphcast_17h_100v, graphcast_17h_tp, graphcast_17h_sp],
                'aifs': [aifs_17h_2t, aifs_17h_2d, aifs_17h_100u, aifs_17h_100v, aifs_17h_tp, aifs_17h_sp],
                'era5': [era5_17h_2t, era5_17h_2d, era5_17h_100u, era5_17h_100v, era5_17h_tp, era5_17h_sp],
                'feature_names': ['temperature_2t', 'dewpoint_2d', 'u100_100u', 'v100_100v', 'precipitation_tp', 'sp']
            },
            '18h': {
                'best_match': [best_match_18h_2t, best_match_18h_2d, best_match_18h_100u, best_match_18h_100v, best_match_18h_tp, best_match_18h_sp],
                'ecmwf_ifs': [ecmwf_ifs_18h_2t, ecmwf_ifs_18h_2d, ecmwf_ifs_18h_100u, ecmwf_ifs_18h_100v, ecmwf_ifs_18h_tp, ecmwf_ifs_18h_sp],
                'gfs_global': [gfs_global_18h_2t, gfs_global_18h_2d, gfs_global_18h_100u, gfs_global_18h_100v, gfs_global_18h_tp, gfs_global_18h_sp],
                'graphcast': [graphcast_18h_2t, graphcast_18h_2d, graphcast_18h_100u, graphcast_18h_100v, graphcast_18h_tp, graphcast_18h_sp],
                'aifs': [aifs_18h_2t, aifs_18h_2d, aifs_18h_100u, aifs_18h_100v, aifs_18h_tp, aifs_18h_sp],
                'era5': [era5_18h_2t, era5_18h_2d, era5_18h_100u, era5_18h_100v, era5_18h_tp, era5_18h_sp],
                'feature_names': ['temperature_2t', 'dewpoint_2d', 'u100_100u', 'v100_100v', 'precipitation_tp', 'sp']
            },
            '19h': {
                'best_match': [best_match_19h_2t, best_match_19h_2d, best_match_19h_100u, best_match_19h_100v, best_match_19h_tp, best_match_19h_sp],
                'ecmwf_ifs': [ecmwf_ifs_19h_2t, ecmwf_ifs_19h_2d, ecmwf_ifs_19h_100u, ecmwf_ifs_19h_100v, ecmwf_ifs_19h_tp, ecmwf_ifs_19h_sp],
                'gfs_global': [gfs_global_19h_2t, gfs_global_19h_2d, gfs_global_19h_100u, gfs_global_19h_100v, gfs_global_19h_tp, gfs_global_19h_sp],
                'graphcast': [graphcast_19h_2t, graphcast_19h_2d, graphcast_19h_100u, graphcast_19h_100v, graphcast_19h_tp, graphcast_19h_sp],
                'aifs': [aifs_19h_2t, aifs_19h_2d, aifs_19h_100u, aifs_19h_100v, aifs_19h_tp, aifs_19h_sp],
                'era5': [era5_19h_2t, era5_19h_2d, era5_19h_100u, era5_19h_100v, era5_19h_tp, era5_19h_sp],
                'feature_names': ['temperature_2t', 'dewpoint_2d', 'u100_100u', 'v100_100v', 'precipitation_tp', 'sp']
            },
            '20h': {
                'best_match': [best_match_20h_2t, best_match_20h_2d, best_match_20h_100u, best_match_20h_100v, best_match_20h_tp, best_match_20h_sp],
                'ecmwf_ifs': [ecmwf_ifs_20h_2t, ecmwf_ifs_20h_2d, ecmwf_ifs_20h_100u, ecmwf_ifs_20h_100v, ecmwf_ifs_20h_tp, ecmwf_ifs_20h_sp],
                'gfs_global': [gfs_global_20h_2t, gfs_global_20h_2d, gfs_global_20h_100u, gfs_global_20h_100v, gfs_global_20h_tp, gfs_global_20h_sp],
                'graphcast': [graphcast_20h_2t, graphcast_20h_2d, graphcast_20h_100u, graphcast_20h_100v, graphcast_20h_tp, graphcast_20h_sp],
                'aifs': [aifs_20h_2t, aifs_20h_2d, aifs_20h_100u, aifs_20h_100v, aifs_20h_tp, aifs_20h_sp],
                'era5': [era5_20h_2t, era5_20h_2d, era5_20h_100u, era5_20h_100v, era5_20h_tp, era5_20h_sp],
                'feature_names': ['temperature_2t', 'dewpoint_2d', 'u100_100u', 'v100_100v', 'precipitation_tp', 'sp']
            }
        }
        
        # Use diverging colormap for better contrast
        cmap = 'RdBu_r'  # or 'coolwarm', 'seismic', 'bwr'
        
        # Process each hour separately
        try:
            for hour in ['1h', '2h', '3h', '4h', '5h', '6h', '7h', '8h', '9h', '10h', '11h', '12h', '13h', '14h', '15h', '16h', '17h', '18h', '19h', '20h']:
                hour_data = hours_data[hour]
                
                # Check if PNG already exists for this hour
                output_path = os.path.join(rar_output_dir, f'{rar_basename}_{hour}.png')
                if os.path.exists(output_path):
                    print(f"PNG file already exists: {output_path}, skipping...")
                    continue
                
                # Create figure with 6 rows (features) and 5 columns (models)
                fig, axes = plt.subplots(6, 5, figsize=(18, 30))
                
                # Process each feature for this hour
                for row_idx in range(6):
                    best_match_data = hour_data['best_match'][row_idx]
                    ecmwf_ifs_data = hour_data['ecmwf_ifs'][row_idx]
                    gfs_global_data = hour_data['gfs_global'][row_idx]
                    graphcast_data = hour_data['graphcast'][row_idx]
                    aifs_data = hour_data['aifs'][row_idx]
                    era5_data = hour_data['era5'][row_idx]
                    feature_name = hour_data['feature_names'][row_idx]
                    
                    # Calculate RMSE for each model
                    rmse_best_match = np.flip(np.abs(best_match_data - era5_data), axis=0)
                    rmse_ecmwf_ifs = np.flip(np.abs(ecmwf_ifs_data - era5_data), axis=0)
                    rmse_gfs_global = np.flip(np.abs(gfs_global_data - era5_data), axis=0)
                    rmse_graphcast = np.flip(np.abs(graphcast_data - era5_data), axis=0)
                    rmse_aifs = np.flip(np.abs(aifs_data - era5_data), axis=0)
                    
                    global_rmse_ecmwf_ifs = np.sqrt(np.mean(rmse_ecmwf_ifs ** 2))
                    global_rmse_best_match = np.sqrt(np.mean(rmse_best_match ** 2)) - global_rmse_ecmwf_ifs
                    global_rmse_gfs_global = np.sqrt(np.mean(rmse_gfs_global ** 2)) - global_rmse_ecmwf_ifs
                    global_rmse_graphcast = np.sqrt(np.mean(rmse_graphcast ** 2)) - global_rmse_ecmwf_ifs
                    global_rmse_aifs = np.sqrt(np.mean(rmse_aifs ** 2)) - global_rmse_ecmwf_ifs

                    # Calculate differences relative to ECMWF
                    data1 = rmse_best_match - rmse_ecmwf_ifs
                    data3 = rmse_gfs_global - rmse_ecmwf_ifs
                    data4 = rmse_graphcast - rmse_ecmwf_ifs
                    data5 = rmse_aifs - rmse_ecmwf_ifs
                    
                    # Find global min/max for symmetric color scaling
                    vmax = max(np.abs(data1).max(), np.abs(data3).max(), np.abs(data4).max(), np.abs(data5).max())
                    vmin = -vmax
                    
                    # Plot with symmetric color scaling
                    im0 = axes[row_idx, 0].imshow(data1, cmap=cmap, vmin=vmin, vmax=vmax)
                    axes[row_idx, 0].set_title(f'{feature_name} ({hour})\nBest Match - ECMWF\n(relative to ERA5)')
                    axes[row_idx, 0].axis('off')
                    axes[row_idx, 0].text(0.5, -0.08, f'Global RMSE: {global_rmse_best_match:.4f}', 
                                          transform=axes[row_idx, 0].transAxes, ha='center', va='top', fontsize=9)
                    
                    # Second plot shows absolute ECMWF RMSE
                    im1 = axes[row_idx, 1].imshow(rmse_ecmwf_ifs, cmap='viridis')
                    axes[row_idx, 1].set_title(f'{feature_name} ({hour})\nECMWF RMSE\n(absolute values)')
                    axes[row_idx, 1].axis('off')
                    axes[row_idx, 1].text(0.5, -0.08, f'Global RMSE: {global_rmse_ecmwf_ifs:.4f}', 
                                          transform=axes[row_idx, 1].transAxes, ha='center', va='top', fontsize=9)
                    
                    im2 = axes[row_idx, 2].imshow(data3, cmap=cmap, vmin=vmin, vmax=vmax)
                    axes[row_idx, 2].set_title(f'{feature_name} ({hour})\nGFS Global - ECMWF\n(relative to ERA5)')
                    axes[row_idx, 2].axis('off')
                    axes[row_idx, 2].text(0.5, -0.08, f'Global RMSE: {global_rmse_gfs_global:.4f}', 
                                          transform=axes[row_idx, 2].transAxes, ha='center', va='top', fontsize=9)
                    
                    im3 = axes[row_idx, 3].imshow(data4, cmap=cmap, vmin=vmin, vmax=vmax)
                    axes[row_idx, 3].set_title(f'{feature_name} ({hour})\nGraphcast - ECMWF\n(relative to ERA5)')
                    axes[row_idx, 3].axis('off')
                    axes[row_idx, 3].text(0.5, -0.08, f'Global RMSE: {global_rmse_graphcast:.4f}', 
                                          transform=axes[row_idx, 3].transAxes, ha='center', va='top', fontsize=9)
                    
                    im4 = axes[row_idx, 4].imshow(data5, cmap=cmap, vmin=vmin, vmax=vmax)
                    axes[row_idx, 4].set_title(f'{feature_name} ({hour})\nAIFS - ECMWF\n(relative to ERA5)')
                    axes[row_idx, 4].axis('off')
                    axes[row_idx, 4].text(0.5, -0.08, f'Global RMSE: {global_rmse_aifs:.4f}', 
                                          transform=axes[row_idx, 4].transAxes, ha='center', va='top', fontsize=9)

                    # Add colorbars
                    fig.colorbar(im0, ax=axes[row_idx, 0], fraction=0.046, pad=0.04)
                    fig.colorbar(im1, ax=axes[row_idx, 1], fraction=0.046, pad=0.04)
                    fig.colorbar(im2, ax=axes[row_idx, 2], fraction=0.046, pad=0.04)
                    fig.colorbar(im3, ax=axes[row_idx, 3], fraction=0.046, pad=0.04)
                    fig.colorbar(im4, ax=axes[row_idx, 4], fraction=0.046, pad=0.04)
                
                plt.tight_layout(rect=[0, 0.03, 1, 0.98])  # Leave space at bottom for global RMSE text
                plt.subplots_adjust(bottom=0.05)  # Additional bottom margin for text
                
                # Save the PNG file for this hour
                try:
                    # Ensure output directory exists
                    os.makedirs(os.path.dirname(output_path), exist_ok=True)
                    plt.savefig(output_path, dpi=300, bbox_inches='tight')
                    print(f"Saved heatmap to: {output_path}")
                except Exception as e:
                    print(f"Error saving PNG file {output_path}: {e}")
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
        
        except Exception as e:
            print(f"Error during heatmap generation: {e}")
            cleanup_extracted_files()
            return False

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
    parser.add_argument('--data', type=str, default='D:',
                        help='Path to directory containing RAR files (absolute or relative path, default: D:)')
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
    
    # Create result directory if it doesn't exist
    os.makedirs(result_dir, exist_ok=True)
    
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

