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
        
        # Check if required files exist (handle case where h5 files are in a subdirectory)
        api_x_path = find_h5_file(extracted_path, 'api_x.h5') if os.path.exists(extracted_path) else None
        y_path = find_h5_file(extracted_path, 'y.h5') if os.path.exists(extracted_path) else None
        
        # Extract if directory doesn't exist or required files are missing
        if not api_x_path or not y_path:
            if not extract_rar_file(rar_path, extracted_path):
                print(f"Skipping {rar_path} due to extraction error.")
                cleanup_extracted_files()
                return False
            
            # Find the files after extraction (recursively search in subdirectories)
            api_x_path = find_h5_file(extracted_path, 'api_x.h5')
            y_path = find_h5_file(extracted_path, 'y.h5')
            
            if not api_x_path or not y_path:
                # Print directory structure for debugging
                print(f"Error: Could not find required HDF5 files in {extracted_path}")
                print(f"Searching in extracted directory structure:")
                if os.path.exists(extracted_path):
                    for root, dirs, files in os.walk(extracted_path):
                        level = root.replace(extracted_path, '').count(os.sep)
                        indent = ' ' * 2 * level
                        print(f"{indent}{os.path.basename(root)}/")
                        subindent = ' ' * 2 * (level + 1)
                        for file in files:
                            if file.endswith('.h5'):
                                print(f"{subindent}{file}")
                print("Skipping this RAR file.")
                cleanup_extracted_files()
                return False
            
            # Print where files were found (helpful for debugging)
            print(f"Found api_x.h5 at: {api_x_path}")
            print(f"Found y.h5 at: {y_path}")

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

        # data_3[:,:,:,4,:] *= 1000
        
        best_match, ecmwf_ifs, gfs_global, AIFS, CMA = np.split(data_3, 5, axis = -1)
        best_match = best_match.squeeze(axis = -1)
        ecmwf_ifs = ecmwf_ifs.squeeze(axis = -1)
        gfs_global = gfs_global.squeeze(axis = -1)
        AIFS = AIFS.squeeze(axis = -1)
        CMA = CMA.squeeze(axis = -1)
        

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
        best_match_21h = best_match[:,:,20,:]
        best_match_22h = best_match[:,:,21,:]
        best_match_23h = best_match[:,:,22,:]
        best_match_24h = best_match[:,:,23,:]

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
        ecmwf_ifs_21h = ecmwf_ifs[:,:,20,:]
        ecmwf_ifs_22h = ecmwf_ifs[:,:,21,:]
        ecmwf_ifs_23h = ecmwf_ifs[:,:,22,:]
        ecmwf_ifs_24h = ecmwf_ifs[:,:,23,:]

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
        gfs_global_21h = gfs_global[:,:,20,:]
        gfs_global_22h = gfs_global[:,:,21,:]
        gfs_global_23h = gfs_global[:,:,22,:]
        gfs_global_24h = gfs_global[:,:,23,:]

        AIFS_1h = AIFS[:,:,0,:]
        AIFS_2h = AIFS[:,:,1,:]
        AIFS_3h = AIFS[:,:,2,:]
        AIFS_4h = AIFS[:,:,3,:]
        AIFS_5h = AIFS[:,:,4,:]
        AIFS_6h = AIFS[:,:,5,:]
        AIFS_7h = AIFS[:,:,6,:]
        AIFS_8h = AIFS[:,:,7,:]
        AIFS_9h = AIFS[:,:,8,:]
        AIFS_10h = AIFS[:,:,9,:]
        AIFS_11h = AIFS[:,:,10,:]
        AIFS_12h = AIFS[:,:,11,:]
        AIFS_13h = AIFS[:,:,12,:]
        AIFS_14h = AIFS[:,:,13,:]
        AIFS_15h = AIFS[:,:,14,:]
        AIFS_16h = AIFS[:,:,15,:]
        AIFS_17h = AIFS[:,:,16,:]
        AIFS_18h = AIFS[:,:,17,:]
        AIFS_19h = AIFS[:,:,18,:]
        AIFS_20h = AIFS[:,:,19,:]
        AIFS_21h = AIFS[:,:,20,:]
        AIFS_22h = AIFS[:,:,21,:]
        AIFS_23h = AIFS[:,:,22,:]
        AIFS_24h = AIFS[:,:,23,:]

        CMA_1h = CMA[:,:,0,:]
        CMA_2h = CMA[:,:,1,:]
        CMA_3h = CMA[:,:,2,:]
        CMA_4h = CMA[:,:,3,:]
        CMA_5h = CMA[:,:,4,:]
        CMA_6h = CMA[:,:,5,:]
        CMA_7h = CMA[:,:,6,:]
        CMA_8h = CMA[:,:,7,:]
        CMA_9h = CMA[:,:,8,:]
        CMA_10h = CMA[:,:,9,:]
        CMA_11h = CMA[:,:,10,:]
        CMA_12h = CMA[:,:,11,:]
        CMA_13h = CMA[:,:,12,:]
        CMA_14h = CMA[:,:,13,:]
        CMA_15h = CMA[:,:,14,:]
        CMA_16h = CMA[:,:,15,:]
        CMA_17h = CMA[:,:,16,:]
        CMA_18h = CMA[:,:,17,:]
        CMA_19h = CMA[:,:,18,:]
        CMA_20h = CMA[:,:,19,:]
        CMA_21h = CMA[:,:,20,:]
        CMA_22h = CMA[:,:,21,:]
        CMA_23h = CMA[:,:,22,:]
        CMA_24h = CMA[:,:,23,:]

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
        
        # Find and load z.h5 into ens_aifs (same way as era5 from y.h5)
        z_path = find_h5_file(extracted_path, 'z.h5')
        ens_aifs = None
        
        if z_path:
            try:
                # Check if file exists and is readable
                if not os.path.exists(z_path):
                    raise FileNotFoundError(f"HDF5 file not found: {z_path}")
                
                # Check file size (HDF5 files should be at least a few bytes)
                file_size = os.path.getsize(z_path)
                if file_size == 0:
                    raise ValueError(f"HDF5 file is empty: {z_path}")
                
                with h5py.File(z_path, 'r') as f:
                    # Try common dataset names, or use the first available dataset
                    if 'data' in f:
                        ens_aifs = np.array(f['data'])
                    elif 'dataset' in f:
                        ens_aifs = np.array(f['dataset'])
                    else:
                        # Use the first dataset found
                        keys = list(f.keys())
                        if keys:
                            ens_aifs = np.array(f[keys[0]])
                        else:
                            raise ValueError("No datasets found in HDF5 file")
                
                print(f"Successfully loaded ens_aifs data from: {z_path}")
            except (OSError, IOError) as e:
                print(f"Warning: Error reading HDF5 file {z_path}: {e}")
                print("The file may be corrupted, truncated, or incomplete.")
                print("Continuing without ens_aifs data.")
                ens_aifs = None
            except Exception as e:
                print(f"Warning: Error loading {z_path}: {e}")
                print("Continuing without ens_aifs data.")
                ens_aifs = None
        else:
            print(f"Warning: z.h5 file not found in {extracted_path}")
            print("Continuing without ens_aifs data.")

        ens_aifs = ens_aifs.squeeze(axis = -1)
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
        era5_21h = era5[:,:,20,:]
        era5_22h = era5[:,:,21,:]
        era5_23h = era5[:,:,22,:]
        era5_24h = era5[:,:,23,:]

        ens_aifs_1h = ens_aifs[:,:,0,:]
        ens_aifs_2h = ens_aifs[:,:,1,:]
        ens_aifs_3h = ens_aifs[:,:,2,:]
        ens_aifs_4h = ens_aifs[:,:,3,:]
        ens_aifs_5h = ens_aifs[:,:,4,:]
        ens_aifs_6h = ens_aifs[:,:,5,:]
        ens_aifs_7h = ens_aifs[:,:,6,:]
        ens_aifs_8h = ens_aifs[:,:,7,:]
        ens_aifs_9h = ens_aifs[:,:,8,:]
        ens_aifs_10h = ens_aifs[:,:,9,:]
        ens_aifs_11h = ens_aifs[:,:,10,:]
        ens_aifs_12h = ens_aifs[:,:,11,:]
        ens_aifs_13h = ens_aifs[:,:,12,:]
        ens_aifs_14h = ens_aifs[:,:,13,:]
        ens_aifs_15h = ens_aifs[:,:,14,:]
        ens_aifs_16h = ens_aifs[:,:,15,:]
        ens_aifs_17h = ens_aifs[:,:,16,:]
        ens_aifs_18h = ens_aifs[:,:,17,:]
        ens_aifs_19h = ens_aifs[:,:,18,:]
        ens_aifs_20h = ens_aifs[:,:,19,:]
        ens_aifs_21h = ens_aifs[:,:,20,:]
        ens_aifs_22h = ens_aifs[:,:,21,:]
        ens_aifs_23h = ens_aifs[:,:,22,:]
        ens_aifs_24h = ens_aifs[:,:,23,:]

        best_match_1h_2t = best_match_1h[:, :, 0]
        best_match_1h_2d = best_match_1h[:, :, 1]
        best_match_1h_100u = best_match_1h[:, :, 2]
        best_match_1h_100v = best_match_1h[:, :, 3]
        best_match_1h_tp = best_match_1h[:, :, 4]
        best_match_1h_sp = best_match_1h[:, :, 5]

        best_match_2h_2t = best_match_2h[:, :, 0]
        best_match_2h_2d = best_match_2h[:, :, 1]
        best_match_2h_100u = best_match_2h[:, :, 2]
        best_match_2h_100v = best_match_2h[:, :, 3]
        best_match_2h_tp = best_match_2h[:, :, 4]
        best_match_2h_sp = best_match_2h[:, :, 5]


        best_match_3h_2t = best_match_3h[:, :, 0]
        best_match_3h_2d = best_match_3h[:, :, 1]
        best_match_3h_100u = best_match_3h[:, :, 2]
        best_match_3h_100v = best_match_3h[:, :, 3]
        best_match_3h_tp = best_match_3h[:, :, 4]
        best_match_3h_sp = best_match_3h[:, :, 5]

        best_match_4h_2t = best_match_4h[:, :, 0]
        best_match_4h_2d = best_match_4h[:, :, 1]
        best_match_4h_100u = best_match_4h[:, :, 2]
        best_match_4h_100v = best_match_4h[:, :, 3]
        best_match_4h_tp = best_match_4h[:, :, 4]
        best_match_4h_sp = best_match_4h[:, :, 5]

        best_match_5h_2t = best_match_5h[:, :, 0]
        best_match_5h_2d = best_match_5h[:, :, 1]
        best_match_5h_100u = best_match_5h[:, :, 2]
        best_match_5h_100v = best_match_5h[:, :, 3]
        best_match_5h_tp = best_match_5h[:, :, 4]
        best_match_5h_sp = best_match_5h[:, :, 5]

        best_match_6h_2t = best_match_6h[:, :, 0]
        best_match_6h_2d = best_match_6h[:, :, 1]
        best_match_6h_100u = best_match_6h[:, :, 2]
        best_match_6h_100v = best_match_6h[:, :, 3]
        best_match_6h_tp = best_match_6h[:, :, 4]
        best_match_6h_sp = best_match_6h[:, :, 5]

        best_match_7h_2t = best_match_7h[:, :, 0]
        best_match_7h_2d = best_match_7h[:, :, 1]
        best_match_7h_100u = best_match_7h[:, :, 2]
        best_match_7h_100v = best_match_7h[:, :, 3]
        best_match_7h_tp = best_match_7h[:, :, 4]
        best_match_7h_sp = best_match_7h[:, :, 5]

        best_match_8h_2t = best_match_8h[:, :, 0]
        best_match_8h_2d = best_match_8h[:, :, 1]
        best_match_8h_100u = best_match_8h[:, :, 2]
        best_match_8h_100v = best_match_8h[:, :, 3]
        best_match_8h_tp = best_match_8h[:, :, 4]
        best_match_8h_sp = best_match_8h[:, :, 5]

        best_match_9h_2t = best_match_9h[:, :, 0]
        best_match_9h_2d = best_match_9h[:, :, 1]
        best_match_9h_100u = best_match_9h[:, :, 2]
        best_match_9h_100v = best_match_9h[:, :, 3]
        best_match_9h_tp = best_match_9h[:, :, 4]
        best_match_9h_sp = best_match_9h[:, :, 5]

        best_match_10h_2t = best_match_10h[:, :, 0]
        best_match_10h_2d = best_match_10h[:, :, 1]
        best_match_10h_100u = best_match_10h[:, :, 2]
        best_match_10h_100v = best_match_10h[:, :, 3]
        best_match_10h_tp = best_match_10h[:, :, 4]
        best_match_10h_sp = best_match_10h[:, :, 5]
        
        best_match_11h_2t = best_match_11h[:, :, 0]
        best_match_11h_2d = best_match_11h[:, :, 1]
        best_match_11h_100u = best_match_11h[:, :, 2]
        best_match_11h_100v = best_match_11h[:, :, 3]
        best_match_11h_tp = best_match_11h[:, :, 4]
        best_match_11h_sp = best_match_11h[:, :, 5]
        
        best_match_12h_2t = best_match_12h[:, :, 0]
        best_match_12h_2d = best_match_12h[:, :, 1]
        best_match_12h_100u = best_match_12h[:, :, 2]
        best_match_12h_100v = best_match_12h[:, :, 3]
        best_match_12h_tp = best_match_12h[:, :, 4]
        best_match_12h_sp = best_match_12h[:, :, 5]
        
        
        best_match_13h_2t = best_match_13h[:, :, 0]
        best_match_13h_2d = best_match_13h[:, :, 1]
        best_match_13h_100u = best_match_13h[:, :, 2]
        best_match_13h_100v = best_match_13h[:, :, 3]
        best_match_13h_tp = best_match_13h[:, :, 4]
        best_match_13h_sp = best_match_13h[:, :, 5]
        

        best_match_14h_2t = best_match_14h[:, :, 0]
        best_match_14h_2d = best_match_14h[:, :, 1]
        best_match_14h_100u = best_match_14h[:, :, 2]
        best_match_14h_100v = best_match_14h[:, :, 3]
        best_match_14h_tp = best_match_14h[:, :, 4]
        best_match_14h_sp = best_match_14h[:, :, 5]
        
        best_match_15h_2t = best_match_15h[:, :, 0]
        best_match_15h_2d = best_match_15h[:, :, 1]
        best_match_15h_100u = best_match_15h[:, :, 2]
        best_match_15h_100v = best_match_15h[:, :, 3]
        best_match_15h_tp = best_match_15h[:, :, 4]
        best_match_15h_sp = best_match_15h[:, :, 5]
        

        best_match_16h_2t = best_match_16h[:, :, 0]
        best_match_16h_2d = best_match_16h[:, :, 1]
        best_match_16h_100u = best_match_16h[:, :, 2]
        best_match_16h_100v = best_match_16h[:, :, 3]
        best_match_16h_tp = best_match_16h[:, :, 4]
        best_match_16h_sp = best_match_16h[:, :, 5]
        

        best_match_17h_2t = best_match_17h[:, :, 0]
        best_match_17h_2d = best_match_17h[:, :, 1]
        best_match_17h_100u = best_match_17h[:, :, 2]
        best_match_17h_100v = best_match_17h[:, :, 3]
        best_match_17h_tp = best_match_17h[:, :, 4]
        best_match_17h_sp = best_match_17h[:, :, 5]
        

        best_match_18h_2t = best_match_18h[:, :, 0]
        best_match_18h_2d = best_match_18h[:, :, 1]
        best_match_18h_100u = best_match_18h[:, :, 2]
        best_match_18h_100v = best_match_18h[:, :, 3]
        best_match_18h_tp = best_match_18h[:, :, 4]
        best_match_18h_sp = best_match_18h[:, :, 5]
        
        best_match_19h_2t = best_match_19h[:, :, 0]
        best_match_19h_2d = best_match_19h[:, :, 1]
        best_match_19h_100u = best_match_19h[:, :, 2]
        best_match_19h_100v = best_match_19h[:, :, 3]
        best_match_19h_tp = best_match_19h[:, :, 4]
        best_match_19h_sp = best_match_19h[:, :, 5]
        
        best_match_20h_2t = best_match_20h[:, :, 0]
        best_match_20h_2d = best_match_20h[:, :, 1]
        best_match_20h_100u = best_match_20h[:, :, 2]
        best_match_20h_100v = best_match_20h[:, :, 3]
        best_match_20h_tp = best_match_20h[:, :, 4]
        best_match_20h_sp = best_match_20h[:, :, 5]
        
        best_match_21h_2t = best_match_21h[:, :, 0]
        best_match_21h_2d = best_match_21h[:, :, 1]
        best_match_21h_100u = best_match_21h[:, :, 2]
        best_match_21h_100v = best_match_21h[:, :, 3]
        best_match_21h_tp = best_match_21h[:, :, 4]
        best_match_21h_sp = best_match_21h[:, :, 5]
        
        best_match_22h_2t = best_match_22h[:, :, 0]
        best_match_22h_2d = best_match_22h[:, :, 1]
        best_match_22h_100u = best_match_22h[:, :, 2]
        best_match_22h_100v = best_match_22h[:, :, 3]
        best_match_22h_tp = best_match_22h[:, :, 4]
        best_match_22h_sp = best_match_22h[:, :, 5]

        best_match_23h_2t = best_match_23h[:, :, 0]
        best_match_23h_2d = best_match_23h[:, :, 1]
        best_match_23h_100u = best_match_23h[:, :, 2]
        best_match_23h_100v = best_match_23h[:, :, 3]
        best_match_23h_tp = best_match_23h[:, :, 4]
        best_match_23h_sp = best_match_23h[:, :, 5]
        
        best_match_24h_2t = best_match_24h[:, :, 0]
        best_match_24h_2d = best_match_24h[:, :, 1]
        best_match_24h_100u = best_match_24h[:, :, 2]
        best_match_24h_100v = best_match_24h[:, :, 3]
        best_match_24h_tp = best_match_24h[:, :, 4]
        best_match_24h_sp = best_match_24h[:, :, 5]
        

        ecmwf_ifs_1h_2t = ecmwf_ifs_1h[:, :, 0]
        ecmwf_ifs_1h_2d = ecmwf_ifs_1h[:, :, 1]
        ecmwf_ifs_1h_100u = ecmwf_ifs_1h[:, :, 2]
        ecmwf_ifs_1h_100v = ecmwf_ifs_1h[:, :, 3]
        ecmwf_ifs_1h_tp = ecmwf_ifs_1h[:, :, 4]
        ecmwf_ifs_1h_sp = ecmwf_ifs_1h[:, :, 5]
        
        ecmwf_ifs_2h_2t = ecmwf_ifs_2h[:, :, 0]
        ecmwf_ifs_2h_2d = ecmwf_ifs_2h[:, :, 1]
        ecmwf_ifs_2h_100u = ecmwf_ifs_2h[:, :, 2]
        ecmwf_ifs_2h_100v = ecmwf_ifs_2h[:, :, 3]
        ecmwf_ifs_2h_tp = ecmwf_ifs_2h[:, :, 4]
        ecmwf_ifs_2h_sp = ecmwf_ifs_2h[:, :, 5]
        
        ecmwf_ifs_3h_2t = ecmwf_ifs_3h[:, :, 0]
        ecmwf_ifs_3h_2d = ecmwf_ifs_3h[:, :, 1]
        ecmwf_ifs_3h_100u = ecmwf_ifs_3h[:, :, 2]
        ecmwf_ifs_3h_100v = ecmwf_ifs_3h[:, :, 3]
        ecmwf_ifs_3h_tp = ecmwf_ifs_3h[:, :, 4]
        ecmwf_ifs_3h_sp = ecmwf_ifs_3h[:, :, 5]
        
        ecmwf_ifs_4h_2t = ecmwf_ifs_4h[:, :, 0]
        ecmwf_ifs_4h_2d = ecmwf_ifs_4h[:, :, 1]
        ecmwf_ifs_4h_100u = ecmwf_ifs_4h[:, :, 2]
        ecmwf_ifs_4h_100v = ecmwf_ifs_4h[:, :, 3]
        ecmwf_ifs_4h_tp = ecmwf_ifs_4h[:, :, 4]
        ecmwf_ifs_4h_sp = ecmwf_ifs_4h[:, :, 5]
        
        ecmwf_ifs_5h_2t = ecmwf_ifs_5h[:, :, 0]
        ecmwf_ifs_5h_2d = ecmwf_ifs_5h[:, :, 1]
        ecmwf_ifs_5h_100u = ecmwf_ifs_5h[:, :, 2]
        ecmwf_ifs_5h_100v = ecmwf_ifs_5h[:, :, 3]
        ecmwf_ifs_5h_tp = ecmwf_ifs_5h[:, :, 4]
        ecmwf_ifs_5h_sp = ecmwf_ifs_5h[:, :, 5]

        ecmwf_ifs_6h_2t = ecmwf_ifs_6h[:, :, 0]
        ecmwf_ifs_6h_2d = ecmwf_ifs_6h[:, :, 1]
        ecmwf_ifs_6h_100u = ecmwf_ifs_6h[:, :, 2]
        ecmwf_ifs_6h_100v = ecmwf_ifs_6h[:, :, 3]
        ecmwf_ifs_6h_tp = ecmwf_ifs_6h[:, :, 4]
        ecmwf_ifs_6h_sp = ecmwf_ifs_6h[:, :, 5]
        
        ecmwf_ifs_7h_2t = ecmwf_ifs_7h[:, :, 0]
        ecmwf_ifs_7h_2d = ecmwf_ifs_7h[:, :, 1]
        ecmwf_ifs_7h_100u = ecmwf_ifs_7h[:, :, 2]
        ecmwf_ifs_7h_100v = ecmwf_ifs_7h[:, :, 3]
        ecmwf_ifs_7h_tp = ecmwf_ifs_7h[:, :, 4]
        ecmwf_ifs_7h_sp = ecmwf_ifs_7h[:, :, 5]
        
        ecmwf_ifs_8h_2t = ecmwf_ifs_8h[:, :, 0]
        ecmwf_ifs_8h_2d = ecmwf_ifs_8h[:, :, 1]
        ecmwf_ifs_8h_100u = ecmwf_ifs_8h[:, :, 2]
        ecmwf_ifs_8h_100v = ecmwf_ifs_8h[:, :, 3]
        ecmwf_ifs_8h_tp = ecmwf_ifs_8h[:, :, 4]
        ecmwf_ifs_8h_sp = ecmwf_ifs_8h[:, :, 5]
        
        ecmwf_ifs_9h_2t = ecmwf_ifs_9h[:, :, 0]
        ecmwf_ifs_9h_2d = ecmwf_ifs_9h[:, :, 1]
        ecmwf_ifs_9h_100u = ecmwf_ifs_9h[:, :, 2]
        ecmwf_ifs_9h_100v = ecmwf_ifs_9h[:, :, 3]
        ecmwf_ifs_9h_tp = ecmwf_ifs_9h[:, :, 4]
        ecmwf_ifs_9h_sp = ecmwf_ifs_9h[:, :, 5]
        
        ecmwf_ifs_10h_2t = ecmwf_ifs_10h[:, :, 0]
        ecmwf_ifs_10h_2d = ecmwf_ifs_10h[:, :, 1]
        ecmwf_ifs_10h_100u = ecmwf_ifs_10h[:, :, 2]
        ecmwf_ifs_10h_100v = ecmwf_ifs_10h[:, :, 3]
        ecmwf_ifs_10h_tp = ecmwf_ifs_10h[:, :, 4]
        ecmwf_ifs_10h_sp = ecmwf_ifs_10h[:, :, 5]
        
        ecmwf_ifs_11h_2t = ecmwf_ifs_11h[:, :, 0]
        ecmwf_ifs_11h_2d = ecmwf_ifs_11h[:, :, 1]
        ecmwf_ifs_11h_100u = ecmwf_ifs_11h[:, :, 2]
        ecmwf_ifs_11h_100v = ecmwf_ifs_11h[:, :, 3]
        ecmwf_ifs_11h_tp = ecmwf_ifs_11h[:, :, 4]
        ecmwf_ifs_11h_sp = ecmwf_ifs_11h[:, :, 5]
        
        ecmwf_ifs_12h_2t = ecmwf_ifs_12h[:, :, 0]
        ecmwf_ifs_12h_2d = ecmwf_ifs_12h[:, :, 1]
        ecmwf_ifs_12h_100u = ecmwf_ifs_12h[:, :, 2]
        ecmwf_ifs_12h_100v = ecmwf_ifs_12h[:, :, 3]
        ecmwf_ifs_12h_tp = ecmwf_ifs_12h[:, :, 4]
        ecmwf_ifs_12h_sp = ecmwf_ifs_12h[:, :, 5]
        
        ecmwf_ifs_13h_2t = ecmwf_ifs_13h[:, :, 0]
        ecmwf_ifs_13h_2d = ecmwf_ifs_13h[:, :, 1]
        ecmwf_ifs_13h_100u = ecmwf_ifs_13h[:, :, 2]
        ecmwf_ifs_13h_100v = ecmwf_ifs_13h[:, :, 3]
        ecmwf_ifs_13h_tp = ecmwf_ifs_13h[:, :, 4]
        ecmwf_ifs_13h_sp = ecmwf_ifs_13h[:, :, 5]
        
        ecmwf_ifs_14h_2t = ecmwf_ifs_14h[:, :, 0]
        ecmwf_ifs_14h_2d = ecmwf_ifs_14h[:, :, 1]
        ecmwf_ifs_14h_100u = ecmwf_ifs_14h[:, :, 2]
        ecmwf_ifs_14h_100v = ecmwf_ifs_14h[:, :, 3]
        ecmwf_ifs_14h_tp = ecmwf_ifs_14h[:, :, 4]
        ecmwf_ifs_14h_sp = ecmwf_ifs_14h[:, :, 5]
        
        ecmwf_ifs_15h_2t = ecmwf_ifs_15h[:, :, 0]
        ecmwf_ifs_15h_2d = ecmwf_ifs_15h[:, :, 1]
        ecmwf_ifs_15h_100u = ecmwf_ifs_15h[:, :, 2]
        ecmwf_ifs_15h_100v = ecmwf_ifs_15h[:, :, 3]
        ecmwf_ifs_15h_tp = ecmwf_ifs_15h[:, :, 4]
        ecmwf_ifs_15h_sp = ecmwf_ifs_15h[:, :, 5]
        
        ecmwf_ifs_16h_2t = ecmwf_ifs_16h[:, :, 0]
        ecmwf_ifs_16h_2d = ecmwf_ifs_16h[:, :, 1]
        ecmwf_ifs_16h_100u = ecmwf_ifs_16h[:, :, 2]
        ecmwf_ifs_16h_100v = ecmwf_ifs_16h[:, :, 3]
        ecmwf_ifs_16h_tp = ecmwf_ifs_16h[:, :, 4]
        ecmwf_ifs_16h_sp = ecmwf_ifs_16h[:, :, 5]

        ecmwf_ifs_17h_2t = ecmwf_ifs_17h[:, :, 0]
        ecmwf_ifs_17h_2d = ecmwf_ifs_17h[:, :, 1]
        ecmwf_ifs_17h_100u = ecmwf_ifs_17h[:, :, 2]
        ecmwf_ifs_17h_100v = ecmwf_ifs_17h[:, :, 3]
        ecmwf_ifs_17h_tp = ecmwf_ifs_17h[:, :, 4]
        ecmwf_ifs_17h_sp = ecmwf_ifs_17h[:, :, 5]
        
        ecmwf_ifs_18h_2t = ecmwf_ifs_18h[:, :, 0]
        ecmwf_ifs_18h_2d = ecmwf_ifs_18h[:, :, 1]
        ecmwf_ifs_18h_100u = ecmwf_ifs_18h[:, :, 2]
        ecmwf_ifs_18h_100v = ecmwf_ifs_18h[:, :, 3]
        ecmwf_ifs_18h_tp = ecmwf_ifs_18h[:, :, 4]
        ecmwf_ifs_18h_sp = ecmwf_ifs_18h[:, :, 5]
        
        ecmwf_ifs_19h_2t = ecmwf_ifs_19h[:, :, 0]
        ecmwf_ifs_19h_2d = ecmwf_ifs_19h[:, :, 1]
        ecmwf_ifs_19h_100u = ecmwf_ifs_19h[:, :, 2]
        ecmwf_ifs_19h_100v = ecmwf_ifs_19h[:, :, 3]
        ecmwf_ifs_19h_tp = ecmwf_ifs_19h[:, :, 4]
        ecmwf_ifs_19h_sp = ecmwf_ifs_19h[:, :, 5]
        
        ecmwf_ifs_20h_2t = ecmwf_ifs_20h[:, :, 0]
        ecmwf_ifs_20h_2d = ecmwf_ifs_20h[:, :, 1]
        ecmwf_ifs_20h_100u = ecmwf_ifs_20h[:, :, 2]
        ecmwf_ifs_20h_100v = ecmwf_ifs_20h[:, :, 3]
        ecmwf_ifs_20h_tp = ecmwf_ifs_20h[:, :, 4]
        ecmwf_ifs_20h_sp = ecmwf_ifs_20h[:, :, 5]

        ecmwf_ifs_21h_2t = ecmwf_ifs_21h[:, :, 0]
        ecmwf_ifs_21h_2d = ecmwf_ifs_21h[:, :, 1]
        ecmwf_ifs_21h_100u = ecmwf_ifs_21h[:, :, 2]
        ecmwf_ifs_21h_100v = ecmwf_ifs_21h[:, :, 3]
        ecmwf_ifs_21h_tp = ecmwf_ifs_21h[:, :, 4]
        ecmwf_ifs_21h_sp = ecmwf_ifs_21h[:, :, 5]
        
        ecmwf_ifs_22h_2t = ecmwf_ifs_22h[:, :, 0]
        ecmwf_ifs_22h_2d = ecmwf_ifs_22h[:, :, 1]
        ecmwf_ifs_22h_100u = ecmwf_ifs_22h[:, :, 2]
        ecmwf_ifs_22h_100v = ecmwf_ifs_22h[:, :, 3]
        ecmwf_ifs_22h_tp = ecmwf_ifs_22h[:, :, 4]
        ecmwf_ifs_22h_sp = ecmwf_ifs_22h[:, :, 5]
        
        ecmwf_ifs_23h_2t = ecmwf_ifs_23h[:, :, 0]
        ecmwf_ifs_23h_2d = ecmwf_ifs_23h[:, :, 1]
        ecmwf_ifs_23h_100u = ecmwf_ifs_23h[:, :, 2]
        ecmwf_ifs_23h_100v = ecmwf_ifs_23h[:, :, 3]
        ecmwf_ifs_23h_tp = ecmwf_ifs_23h[:, :, 4]
        ecmwf_ifs_23h_sp = ecmwf_ifs_23h[:, :, 5]
        
        ecmwf_ifs_24h_2t = ecmwf_ifs_24h[:, :, 0]
        ecmwf_ifs_24h_2d = ecmwf_ifs_24h[:, :, 1]
        ecmwf_ifs_24h_100u = ecmwf_ifs_24h[:, :, 2]
        ecmwf_ifs_24h_100v = ecmwf_ifs_24h[:, :, 3]
        ecmwf_ifs_24h_tp = ecmwf_ifs_24h[:, :, 4]
        ecmwf_ifs_24h_sp = ecmwf_ifs_24h[:, :, 5]
        
        gfs_global_1h_2t = gfs_global_1h[:, :, 0]
        gfs_global_1h_2d = gfs_global_1h[:, :, 1]
        gfs_global_1h_100u = gfs_global_1h[:, :, 2]
        gfs_global_1h_100v = gfs_global_1h[:, :, 3]
        gfs_global_1h_tp = gfs_global_1h[:, :, 4]
        gfs_global_1h_sp = gfs_global_1h[:, :, 5]
        
        gfs_global_2h_2t = gfs_global_2h[:, :, 0]
        gfs_global_2h_2d = gfs_global_2h[:, :, 1]
        gfs_global_2h_100u = gfs_global_2h[:, :, 2]
        gfs_global_2h_100v = gfs_global_2h[:, :, 3]
        gfs_global_2h_tp = gfs_global_2h[:, :, 4]
        gfs_global_2h_sp = gfs_global_2h[:, :, 5]
        
        gfs_global_3h_2t = gfs_global_3h[:, :, 0]
        gfs_global_3h_2d = gfs_global_3h[:, :, 1]
        gfs_global_3h_100u = gfs_global_3h[:, :, 2]
        gfs_global_3h_100v = gfs_global_3h[:, :, 3]
        gfs_global_3h_tp = gfs_global_3h[:, :, 4]
        gfs_global_3h_sp = gfs_global_3h[:, :, 5]
        
        gfs_global_4h_2t = gfs_global_4h[:, :, 0]
        gfs_global_4h_2d = gfs_global_4h[:, :, 1]
        gfs_global_4h_100u = gfs_global_4h[:, :, 2]
        gfs_global_4h_100v = gfs_global_4h[:, :, 3]
        gfs_global_4h_tp = gfs_global_4h[:, :, 4]
        gfs_global_4h_sp = gfs_global_4h[:, :, 5]
        
        gfs_global_5h_2t = gfs_global_5h[:, :, 0]
        gfs_global_5h_2d = gfs_global_5h[:, :, 1]
        gfs_global_5h_100u = gfs_global_5h[:, :, 2]
        gfs_global_5h_100v = gfs_global_5h[:, :, 3]
        gfs_global_5h_tp = gfs_global_5h[:, :, 4]
        gfs_global_5h_sp = gfs_global_5h[:, :, 5]

        gfs_global_6h_2t = gfs_global_6h[:, :, 0]
        gfs_global_6h_2d = gfs_global_6h[:, :, 1]
        gfs_global_6h_100u = gfs_global_6h[:, :, 2]
        gfs_global_6h_100v = gfs_global_6h[:, :, 3]
        gfs_global_6h_tp = gfs_global_6h[:, :, 4]
        gfs_global_6h_sp = gfs_global_6h[:, :, 5]

        gfs_global_7h_2t = gfs_global_7h[:, :, 0]
        gfs_global_7h_2d = gfs_global_7h[:, :, 1]
        gfs_global_7h_100u = gfs_global_7h[:, :, 2]
        gfs_global_7h_100v = gfs_global_7h[:, :, 3]
        gfs_global_7h_tp = gfs_global_7h[:, :, 4]
        gfs_global_7h_sp = gfs_global_7h[:, :, 5]
        
        gfs_global_8h_2t = gfs_global_8h[:, :, 0]
        gfs_global_8h_2d = gfs_global_8h[:, :, 1]
        gfs_global_8h_100u = gfs_global_8h[:, :, 2]
        gfs_global_8h_100v = gfs_global_8h[:, :, 3]
        gfs_global_8h_tp = gfs_global_8h[:, :, 4]
        gfs_global_8h_sp = gfs_global_8h[:, :, 5]
        
        gfs_global_9h_2t = gfs_global_9h[:, :, 0]
        gfs_global_9h_2d = gfs_global_9h[:, :, 1]
        gfs_global_9h_100u = gfs_global_9h[:, :, 2]
        gfs_global_9h_100v = gfs_global_9h[:, :, 3]
        gfs_global_9h_tp = gfs_global_9h[:, :, 4]
        gfs_global_9h_sp = gfs_global_9h[:, :, 5]
        
        gfs_global_10h_2t = gfs_global_10h[:, :, 0]
        gfs_global_10h_2d = gfs_global_10h[:, :, 1]
        gfs_global_10h_100u = gfs_global_10h[:, :, 2]
        gfs_global_10h_100v = gfs_global_10h[:, :, 3]
        gfs_global_10h_tp = gfs_global_10h[:, :, 4]
        gfs_global_10h_sp = gfs_global_10h[:, :, 5]
        
        gfs_global_11h_2t = gfs_global_11h[:, :, 0]
        gfs_global_11h_2d = gfs_global_11h[:, :, 1]
        gfs_global_11h_100u = gfs_global_11h[:, :, 2]
        gfs_global_11h_100v = gfs_global_11h[:, :, 3]
        gfs_global_11h_tp = gfs_global_11h[:, :, 4]
        gfs_global_11h_sp = gfs_global_11h[:, :, 5]
        
        gfs_global_12h_2t = gfs_global_12h[:, :, 0]
        gfs_global_12h_2d = gfs_global_12h[:, :, 1]
        gfs_global_12h_100u = gfs_global_12h[:, :, 2]
        gfs_global_12h_100v = gfs_global_12h[:, :, 3]
        gfs_global_12h_tp = gfs_global_12h[:, :, 4]
        gfs_global_12h_sp = gfs_global_12h[:, :, 5]
        
        gfs_global_13h_2t = gfs_global_13h[:, :, 0]
        gfs_global_13h_2d = gfs_global_13h[:, :, 1]
        gfs_global_13h_100u = gfs_global_13h[:, :, 2]
        gfs_global_13h_100v = gfs_global_13h[:, :, 3]
        gfs_global_13h_tp = gfs_global_13h[:, :, 4]
        gfs_global_13h_sp = gfs_global_13h[:, :, 5]
        
        gfs_global_14h_2t = gfs_global_14h[:, :, 0]
        gfs_global_14h_2d = gfs_global_14h[:, :, 1]
        gfs_global_14h_100u = gfs_global_14h[:, :, 2]
        gfs_global_14h_100v = gfs_global_14h[:, :, 3]
        gfs_global_14h_tp = gfs_global_14h[:, :, 4]
        gfs_global_14h_sp = gfs_global_14h[:, :, 5]
        
        gfs_global_15h_2t = gfs_global_15h[:, :, 0]
        gfs_global_15h_2d = gfs_global_15h[:, :, 1]
        gfs_global_15h_100u = gfs_global_15h[:, :, 2]
        gfs_global_15h_100v = gfs_global_15h[:, :, 3]
        gfs_global_15h_tp = gfs_global_15h[:, :, 4]
        gfs_global_15h_sp = gfs_global_15h[:, :, 5]
        
        gfs_global_16h_2t = gfs_global_16h[:, :, 0]
        gfs_global_16h_2d = gfs_global_16h[:, :, 1]
        gfs_global_16h_100u = gfs_global_16h[:, :, 2]
        gfs_global_16h_100v = gfs_global_16h[:, :, 3]
        gfs_global_16h_tp = gfs_global_16h[:, :, 4]
        gfs_global_16h_sp = gfs_global_16h[:, :, 5]
        
        gfs_global_17h_2t = gfs_global_17h[:, :, 0]
        gfs_global_17h_2d = gfs_global_17h[:, :, 1]
        gfs_global_17h_100u = gfs_global_17h[:, :, 2]
        gfs_global_17h_100v = gfs_global_17h[:, :, 3]
        gfs_global_17h_tp = gfs_global_17h[:, :, 4]
        gfs_global_17h_sp = gfs_global_17h[:, :, 5]
        
        gfs_global_18h_2t = gfs_global_18h[:, :, 0]
        gfs_global_18h_2d = gfs_global_18h[:, :, 1]
        gfs_global_18h_100u = gfs_global_18h[:, :, 2]
        gfs_global_18h_100v = gfs_global_18h[:, :, 3]
        gfs_global_18h_tp = gfs_global_18h[:, :, 4]
        gfs_global_18h_sp = gfs_global_18h[:, :, 5]
        
        gfs_global_19h_2t = gfs_global_19h[:, :, 0]
        gfs_global_19h_2d = gfs_global_19h[:, :, 1]
        gfs_global_19h_100u = gfs_global_19h[:, :, 2]
        gfs_global_19h_100v = gfs_global_19h[:, :, 3]
        gfs_global_19h_tp = gfs_global_19h[:, :, 4]
        gfs_global_19h_sp = gfs_global_19h[:, :, 5]
        
        gfs_global_20h_2t = gfs_global_20h[:, :, 0]
        gfs_global_20h_2d = gfs_global_20h[:, :, 1]
        gfs_global_20h_100u = gfs_global_20h[:, :, 2]
        gfs_global_20h_100v = gfs_global_20h[:, :, 3]
        gfs_global_20h_tp = gfs_global_20h[:, :, 4]
        gfs_global_20h_sp = gfs_global_20h[:, :, 5]
        
        gfs_global_21h_2t = gfs_global_21h[:, :, 0]
        gfs_global_21h_2d = gfs_global_21h[:, :, 1]
        gfs_global_21h_100u = gfs_global_21h[:, :, 2]
        gfs_global_21h_100v = gfs_global_21h[:, :, 3]
        gfs_global_21h_tp = gfs_global_21h[:, :, 4]
        gfs_global_21h_sp = gfs_global_21h[:, :, 5]
        
        gfs_global_22h_2t = gfs_global_22h[:, :, 0]
        gfs_global_22h_2d = gfs_global_22h[:, :, 1]
        gfs_global_22h_100u = gfs_global_22h[:, :, 2]
        gfs_global_22h_100v = gfs_global_22h[:, :, 3]
        gfs_global_22h_tp = gfs_global_22h[:, :, 4]
        gfs_global_22h_sp = gfs_global_22h[:, :, 5]
        
        gfs_global_23h_2t = gfs_global_23h[:, :, 0]
        gfs_global_23h_2d = gfs_global_23h[:, :, 1]
        gfs_global_23h_100u = gfs_global_23h[:, :, 2]
        gfs_global_23h_100v = gfs_global_23h[:, :, 3]
        gfs_global_23h_tp = gfs_global_23h[:, :, 4]
        gfs_global_23h_sp = gfs_global_23h[:, :, 5]
        
        gfs_global_24h_2t = gfs_global_24h[:, :, 0]
        gfs_global_24h_2d = gfs_global_24h[:, :, 1]
        gfs_global_24h_100u = gfs_global_24h[:, :, 2]
        gfs_global_24h_100v = gfs_global_24h[:, :, 3]
        gfs_global_24h_tp = gfs_global_24h[:, :, 4]
        gfs_global_24h_sp = gfs_global_24h[:, :, 5]
        
        AIFS_1h_2t = AIFS_1h[:, :, 0]
        AIFS_1h_2d = AIFS_1h[:, :, 1]
        AIFS_1h_100u = AIFS_1h[:, :, 2]
        AIFS_1h_100v = AIFS_1h[:, :, 3]
        AIFS_1h_tp = AIFS_1h[:, :, 4]
        AIFS_1h_sp = AIFS_1h[:, :, 5]
        
        AIFS_2h_2t = AIFS_2h[:, :, 0]
        AIFS_2h_2d = AIFS_2h[:, :, 1]
        AIFS_2h_100u = AIFS_2h[:, :, 2]
        AIFS_2h_100v = AIFS_2h[:, :, 3]
        AIFS_2h_tp = AIFS_2h[:, :, 4]
        AIFS_2h_sp = AIFS_2h[:, :, 5]
        
        AIFS_3h_2t = AIFS_3h[:, :, 0]
        AIFS_3h_2d = AIFS_3h[:, :, 1]
        AIFS_3h_100u = AIFS_3h[:, :, 2]
        AIFS_3h_100v = AIFS_3h[:, :, 3]
        AIFS_3h_tp = AIFS_3h[:, :, 4]
        AIFS_3h_sp = AIFS_3h[:, :, 5]
        
        AIFS_4h_2t = AIFS_4h[:, :, 0]
        AIFS_4h_2d = AIFS_4h[:, :, 1]
        AIFS_4h_100u = AIFS_4h[:, :, 2]
        AIFS_4h_100v = AIFS_4h[:, :, 3]
        AIFS_4h_tp = AIFS_4h[:, :, 4]
        AIFS_4h_sp = AIFS_4h[:, :, 5]
        
        AIFS_5h_2t = AIFS_5h[:, :, 0]
        AIFS_5h_2d = AIFS_5h[:, :, 1]
        AIFS_5h_100u = AIFS_5h[:, :, 2]
        AIFS_5h_100v = AIFS_5h[:, :, 3]
        AIFS_5h_tp = AIFS_5h[:, :, 4]
        AIFS_5h_sp = AIFS_5h[:, :, 5]
        
        AIFS_6h_2t = AIFS_6h[:, :, 0]
        AIFS_6h_2d = AIFS_6h[:, :, 1]
        AIFS_6h_100u = AIFS_6h[:, :, 2]
        AIFS_6h_100v = AIFS_6h[:, :, 3]
        AIFS_6h_tp = AIFS_6h[:, :, 4]
        AIFS_6h_sp = AIFS_6h[:, :, 5]
        
        AIFS_7h_2t = AIFS_7h[:, :, 0]
        AIFS_7h_2d = AIFS_7h[:, :, 1]
        AIFS_7h_100u = AIFS_7h[:, :, 2]
        AIFS_7h_100v = AIFS_7h[:, :, 3]
        AIFS_7h_tp = AIFS_7h[:, :, 4]
        AIFS_7h_sp = AIFS_7h[:, :, 5]
        
        AIFS_8h_2t = AIFS_8h[:, :, 0]
        AIFS_8h_2d = AIFS_8h[:, :, 1]
        AIFS_8h_100u = AIFS_8h[:, :, 2]
        AIFS_8h_100v = AIFS_8h[:, :, 3]
        AIFS_8h_tp = AIFS_8h[:, :, 4]
        AIFS_8h_sp = AIFS_8h[:, :, 5]
        
        AIFS_9h_2t = AIFS_9h[:, :, 0]
        AIFS_9h_2d = AIFS_9h[:, :, 1]
        AIFS_9h_100u = AIFS_9h[:, :, 2]
        AIFS_9h_100v = AIFS_9h[:, :, 3]
        AIFS_9h_tp = AIFS_9h[:, :, 4]
        AIFS_9h_sp = AIFS_9h[:, :, 5]
        
        AIFS_10h_2t = AIFS_10h[:, :, 0]
        AIFS_10h_2d = AIFS_10h[:, :, 1]
        AIFS_10h_100u = AIFS_10h[:, :, 2]
        AIFS_10h_100v = AIFS_10h[:, :, 3]
        AIFS_10h_tp = AIFS_10h[:, :, 4]
        AIFS_10h_sp = AIFS_10h[:, :, 5]

        AIFS_11h_2t = AIFS_11h[:, :, 0]
        AIFS_11h_2d = AIFS_11h[:, :, 1]
        AIFS_11h_100u = AIFS_11h[:, :, 2]
        AIFS_11h_100v = AIFS_11h[:, :, 3]
        AIFS_11h_tp = AIFS_11h[:, :, 4]
        AIFS_11h_sp = AIFS_11h[:, :, 5]
        
        AIFS_12h_2t = AIFS_12h[:, :, 0]
        AIFS_12h_2d = AIFS_12h[:, :, 1]
        AIFS_12h_100u = AIFS_12h[:, :, 2]
        AIFS_12h_100v = AIFS_12h[:, :, 3]
        AIFS_12h_tp = AIFS_12h[:, :, 4]
        AIFS_12h_sp = AIFS_12h[:, :, 5]
        
        AIFS_13h_2t = AIFS_13h[:, :, 0]
        AIFS_13h_2d = AIFS_13h[:, :, 1]
        AIFS_13h_100u = AIFS_13h[:, :, 2]
        AIFS_13h_100v = AIFS_13h[:, :, 3]
        AIFS_13h_tp = AIFS_13h[:, :, 4]
        AIFS_13h_sp = AIFS_13h[:, :, 5]
        
        AIFS_14h_2t = AIFS_14h[:, :, 0]
        AIFS_14h_2d = AIFS_14h[:, :, 1]
        AIFS_14h_100u = AIFS_14h[:, :, 2]
        AIFS_14h_100v = AIFS_14h[:, :, 3]
        AIFS_14h_tp = AIFS_14h[:, :, 4]
        AIFS_14h_sp = AIFS_14h[:, :, 5]

        AIFS_15h_2t = AIFS_15h[:, :, 0]
        AIFS_15h_2d = AIFS_15h[:, :, 1]
        AIFS_15h_100u = AIFS_15h[:, :, 2]
        AIFS_15h_100v = AIFS_15h[:, :, 3]
        AIFS_15h_tp = AIFS_15h[:, :, 4]
        AIFS_15h_sp = AIFS_15h[:, :, 5]
        

        AIFS_16h_2t = AIFS_16h[:, :, 0]
        AIFS_16h_2d = AIFS_16h[:, :, 1]
        AIFS_16h_100u = AIFS_16h[:, :, 2]
        AIFS_16h_100v = AIFS_16h[:, :, 3]
        AIFS_16h_tp = AIFS_16h[:, :, 4]
        AIFS_16h_sp = AIFS_16h[:, :, 5]
        
        AIFS_17h_2t = AIFS_17h[:, :, 0]
        AIFS_17h_2d = AIFS_17h[:, :, 1]
        AIFS_17h_100u = AIFS_17h[:, :, 2]
        AIFS_17h_100v = AIFS_17h[:, :, 3]
        AIFS_17h_tp = AIFS_17h[:, :, 4]
        AIFS_17h_sp = AIFS_17h[:, :, 5]
        
        AIFS_18h_2t = AIFS_18h[:, :, 0]
        AIFS_18h_2d = AIFS_18h[:, :, 1]
        AIFS_18h_100u = AIFS_18h[:, :, 2]
        AIFS_18h_100v = AIFS_18h[:, :, 3]
        AIFS_18h_tp = AIFS_18h[:, :, 4]
        AIFS_18h_sp = AIFS_18h[:, :, 5]
        
        AIFS_19h_2t = AIFS_19h[:, :, 0]
        AIFS_19h_2d = AIFS_19h[:, :, 1]
        AIFS_19h_100u = AIFS_19h[:, :, 2]
        AIFS_19h_100v = AIFS_19h[:, :, 3]
        AIFS_19h_tp = AIFS_19h[:, :, 4]
        AIFS_19h_sp = AIFS_19h[:, :, 5]
        
        AIFS_20h_2t = AIFS_20h[:, :, 0]
        AIFS_20h_2d = AIFS_20h[:, :, 1]
        AIFS_20h_100u = AIFS_20h[:, :, 2]
        AIFS_20h_100v = AIFS_20h[:, :, 3]
        AIFS_20h_tp = AIFS_20h[:, :, 4]
        AIFS_20h_sp = AIFS_20h[:, :, 5]

        AIFS_21h_2t = AIFS_21h[:, :, 0]
        AIFS_21h_2d = AIFS_21h[:, :, 1]
        AIFS_21h_100u = AIFS_21h[:, :, 2]
        AIFS_21h_100v = AIFS_21h[:, :, 3]
        AIFS_21h_tp = AIFS_21h[:, :, 4]
        AIFS_21h_sp = AIFS_21h[:, :, 5]
        
        AIFS_22h_2t = AIFS_22h[:, :, 0]
        AIFS_22h_2d = AIFS_22h[:, :, 1]
        AIFS_22h_100u = AIFS_22h[:, :, 2]
        AIFS_22h_100v = AIFS_22h[:, :, 3]
        AIFS_22h_tp = AIFS_22h[:, :, 4]
        AIFS_22h_sp = AIFS_22h[:, :, 5]
        
        AIFS_23h_2t = AIFS_23h[:, :, 0]
        AIFS_23h_2d = AIFS_23h[:, :, 1]
        AIFS_23h_100u = AIFS_23h[:, :, 2]
        AIFS_23h_100v = AIFS_23h[:, :, 3]
        AIFS_23h_tp = AIFS_23h[:, :, 4]
        AIFS_23h_sp = AIFS_23h[:, :, 5]
        
        AIFS_24h_2t = AIFS_24h[:, :, 0]
        AIFS_24h_2d = AIFS_24h[:, :, 1]
        AIFS_24h_100u = AIFS_24h[:, :, 2]
        AIFS_24h_100v = AIFS_24h[:, :, 3]
        AIFS_24h_tp = AIFS_24h[:, :, 4]
        AIFS_24h_sp = AIFS_24h[:, :, 5]
        
        CMA_1h_2t = CMA_1h[:,:,0]
        CMA_1h_2d = CMA_1h[:,:,1]
        CMA_1h_100u = CMA_1h[:,:,2]
        CMA_1h_100v = CMA_1h[:,:,3]
        CMA_1h_tp = CMA_1h[:,:,4]
        CMA_1h_sp = CMA_1h[:,:,5]
        
        CMA_2h_2t = CMA_2h[:,:,0]
        CMA_2h_2d = CMA_2h[:,:,1]
        CMA_2h_100u = CMA_2h[:,:,2]
        CMA_2h_100v = CMA_2h[:,:,3]  
        CMA_2h_tp = CMA_2h[:,:,4]
        CMA_2h_sp = CMA_2h[:,:,5]
        
        CMA_3h_2t = CMA_3h[:,:,0]
        CMA_3h_2d = CMA_3h[:,:,1]
        CMA_3h_100u = CMA_3h[:,:,2]
        CMA_3h_100v = CMA_3h[:,:,3]
        CMA_3h_tp = CMA_3h[:,:,4]
        CMA_3h_sp = CMA_3h[:,:,5]

        CMA_4h_2t = CMA_4h[:,:,0]
        CMA_4h_2d = CMA_4h[:,:,1]
        CMA_4h_100u = CMA_4h[:,:,2]
        CMA_4h_100v = CMA_4h[:,:,3]
        CMA_4h_tp = CMA_4h[:,:,4]
        CMA_4h_sp = CMA_4h[:,:,5]
        
        CMA_5h_2t = CMA_5h[:,:,0]
        CMA_5h_2d = CMA_5h[:,:,1]
        CMA_5h_100u = CMA_5h[:,:,2]
        CMA_5h_100v = CMA_5h[:,:,3]
        CMA_5h_tp = CMA_5h[:,:,4]
        CMA_5h_sp = CMA_5h[:,:,5]
        
        CMA_6h_2t = CMA_6h[:,:,0]
        CMA_6h_2d = CMA_6h[:,:,1]
        CMA_6h_100u = CMA_6h[:,:,2]
        CMA_6h_100v = CMA_6h[:,:,3]
        CMA_6h_tp = CMA_6h[:,:,4]
        CMA_6h_sp = CMA_6h[:,:,5]
        
        CMA_7h_2t = CMA_7h[:,:,0]
        CMA_7h_2d = CMA_7h[:,:,1]
        CMA_7h_100u = CMA_7h[:,:,2]
        CMA_7h_100v = CMA_7h[:,:,3]
        CMA_7h_tp = CMA_7h[:,:,4]
        CMA_7h_sp = CMA_7h[:,:,5]
        
        CMA_8h_2t = CMA_8h[:,:,0]
        CMA_8h_2d = CMA_8h[:,:,1]
        CMA_8h_100u = CMA_8h[:,:,2]
        CMA_8h_100v = CMA_8h[:,:,3]
        CMA_8h_tp = CMA_8h[:,:,4]
        CMA_8h_sp = CMA_8h[:,:,5]
        
        CMA_9h_2t = CMA_9h[:,:,0]
        CMA_9h_2d = CMA_9h[:,:,1]
        CMA_9h_100u = CMA_9h[:,:,2]
        CMA_9h_100v = CMA_9h[:,:,3]
        CMA_9h_tp = CMA_9h[:,:,4]
        CMA_9h_sp = CMA_9h[:,:,5]
        
        CMA_10h_2t = CMA_10h[:,:,0]
        CMA_10h_2d = CMA_10h[:,:,1]
        CMA_10h_100u = CMA_10h[:,:,2]
        CMA_10h_100v = CMA_10h[:,:,3]
        CMA_10h_tp = CMA_10h[:,:,4]
        CMA_10h_sp = CMA_10h[:,:,5]
        
        CMA_11h_2t = CMA_11h[:,:,0]
        CMA_11h_2d = CMA_11h[:,:,1]
        CMA_11h_100u = CMA_11h[:,:,2]
        CMA_11h_100v = CMA_11h[:,:,3]
        CMA_11h_tp = CMA_11h[:,:,4]
        CMA_11h_sp = CMA_11h[:,:,5]
        
        CMA_12h_2t = CMA_12h[:,:,0]
        CMA_12h_2d = CMA_12h[:,:,1]
        CMA_12h_100u = CMA_12h[:,:,2]
        CMA_12h_100v = CMA_12h[:,:,3]
        CMA_12h_tp = CMA_12h[:,:,4]
        CMA_12h_sp = CMA_12h[:,:,5]
        
        CMA_13h_2t = CMA_13h[:,:,0]
        CMA_13h_2d = CMA_13h[:,:,1]
        CMA_13h_100u = CMA_13h[:,:,2]
        CMA_13h_100v = CMA_13h[:,:,3]
        CMA_13h_tp = CMA_13h[:,:,4]
        CMA_13h_sp = CMA_13h[:,:,5]
        
        CMA_14h_2t = CMA_14h[:,:,0]
        CMA_14h_2d = CMA_14h[:,:,1]
        CMA_14h_100u = CMA_14h[:,:,2]
        CMA_14h_100v = CMA_14h[:,:,3]
        CMA_14h_tp = CMA_14h[:,:,4]
        CMA_14h_sp = CMA_14h[:,:,5]
        
        CMA_15h_2t = CMA_15h[:,:,0]
        CMA_15h_2d = CMA_15h[:,:,1]
        CMA_15h_100u = CMA_15h[:,:,2]
        CMA_15h_100v = CMA_15h[:,:,3]
        CMA_15h_tp = CMA_15h[:,:,4]
        CMA_15h_sp = CMA_15h[:,:,5]
        
        CMA_16h_2t = CMA_16h[:,:,0]
        CMA_16h_2d = CMA_16h[:,:,1]
        CMA_16h_100u = CMA_16h[:,:,2]
        CMA_16h_100v = CMA_16h[:,:,3]
        CMA_16h_tp = CMA_16h[:,:,4]
        CMA_16h_sp = CMA_16h[:,:,5]
        
        CMA_17h_2t = CMA_17h[:,:,0]
        CMA_17h_2d = CMA_17h[:,:,1]
        CMA_17h_100u = CMA_17h[:,:,2]
        CMA_17h_100v = CMA_17h[:,:,3]
        CMA_17h_tp = CMA_17h[:,:,4]
        CMA_17h_sp = CMA_17h[:,:,5]
        
        CMA_18h_2t = CMA_18h[:,:,0]
        CMA_18h_2d = CMA_18h[:,:,1]
        CMA_18h_100u = CMA_18h[:,:,2]
        CMA_18h_100v = CMA_18h[:,:,3]
        CMA_18h_tp = CMA_18h[:,:,4]
        CMA_18h_sp = CMA_18h[:,:,5]
        
        CMA_19h_2t = CMA_19h[:,:,0]
        CMA_19h_2d = CMA_19h[:,:,1]
        CMA_19h_100u = CMA_19h[:,:,2]
        CMA_19h_100v = CMA_19h[:,:,3]
        CMA_19h_tp = CMA_19h[:,:,4]
        CMA_19h_sp = CMA_19h[:,:,5]
        
        CMA_20h_2t = CMA_20h[:,:,0]
        CMA_20h_2d = CMA_20h[:,:,1]
        CMA_20h_100u = CMA_20h[:,:,2]
        CMA_20h_100v = CMA_20h[:,:,3]
        CMA_20h_tp = CMA_20h[:,:,4]
        CMA_20h_sp = CMA_20h[:,:,5]

        CMA_21h_2t = CMA_21h[:,:,0]
        CMA_21h_2d = CMA_21h[:,:,1]
        CMA_21h_100u = CMA_21h[:,:,2]
        CMA_21h_100v = CMA_21h[:,:,3]
        CMA_21h_tp = CMA_21h[:,:,4]
        CMA_21h_sp = CMA_21h[:,:,5]
        
        CMA_22h_2t = CMA_22h[:,:,0]
        CMA_22h_2d = CMA_22h[:,:,1]
        CMA_22h_100u = CMA_22h[:,:,2]
        CMA_22h_100v = CMA_22h[:,:,3]
        CMA_22h_tp = CMA_22h[:,:,4]
        CMA_22h_sp = CMA_22h[:,:,5]
        
        CMA_23h_2t = CMA_23h[:,:,0]
        CMA_23h_2d = CMA_23h[:,:,1]
        CMA_23h_100u = CMA_23h[:,:,2]
        CMA_23h_100v = CMA_23h[:,:,3]
        CMA_23h_tp = CMA_23h[:,:,4]
        CMA_23h_sp = CMA_23h[:,:,5]
        
        CMA_24h_2t = CMA_24h[:,:,0]
        CMA_24h_2d = CMA_24h[:,:,1]
        CMA_24h_100u = CMA_24h[:,:,2]
        CMA_24h_100v = CMA_24h[:,:,3]
        CMA_24h_tp = CMA_24h[:,:,4]
        CMA_24h_sp = CMA_24h[:,:,5]
        
        era5_1h_2t = era5_1h[:, :, 0]
        era5_1h_2d = era5_1h[:, :, 1]
        era5_1h_100u = era5_1h[:, :, 2]
        era5_1h_100v = era5_1h[:, :, 3]
        era5_1h_tp = era5_1h[:, :, 4]
        era5_1h_sp = era5_1h[:, :, 5]
        
        era5_2h_2t = era5_2h[:, :, 0]
        era5_2h_2d = era5_2h[:, :, 1]
        era5_2h_100u = era5_2h[:, :, 2]
        era5_2h_100v = era5_2h[:, :, 3]
        era5_2h_tp = era5_2h[:, :, 4]
        era5_2h_sp = era5_2h[:, :, 5]

        era5_3h_2t = era5_3h[:, :, 0]
        era5_3h_2d = era5_3h[:, :, 1]
        era5_3h_100u = era5_3h[:, :, 2]
        era5_3h_100v = era5_3h[:, :, 3]
        era5_3h_tp = era5_3h[:, :, 4]
        era5_3h_sp = era5_3h[:, :, 5]
        

        era5_4h_2t = era5_4h[:, :, 0]
        era5_4h_2d = era5_4h[:, :, 1]
        era5_4h_100u = era5_4h[:, :, 2]
        era5_4h_100v = era5_4h[:, :, 3]
        era5_4h_tp = era5_4h[:, :, 4]
        era5_4h_sp = era5_4h[:, :, 5]
        

        era5_5h_2t = era5_5h[:, :, 0]
        era5_5h_2d = era5_5h[:, :, 1]
        era5_5h_100u = era5_5h[:, :, 2]
        era5_5h_100v = era5_5h[:, :, 3]
        era5_5h_tp = era5_5h[:, :, 4]
        era5_5h_sp = era5_5h[:, :, 5]
        

        era5_6h_2t = era5_6h[:, :, 0]
        era5_6h_2d = era5_6h[:, :, 1]
        era5_6h_100u = era5_6h[:, :, 2]
        era5_6h_100v = era5_6h[:, :, 3]
        era5_6h_tp = era5_6h[:, :, 4]
        era5_6h_sp = era5_6h[:, :, 5]

        era5_7h_2t = era5_7h[:, :, 0]
        era5_7h_2d = era5_7h[:, :, 1]
        era5_7h_100u = era5_7h[:, :, 2]
        era5_7h_100v = era5_7h[:, :, 3]
        era5_7h_tp = era5_7h[:, :, 4]
        era5_7h_sp = era5_7h[:, :, 5]
        
        
        era5_8h_2t = era5_8h[:, :, 0]
        era5_8h_2d = era5_8h[:, :, 1]
        era5_8h_100u = era5_8h[:, :, 2]
        era5_8h_100v = era5_8h[:, :, 3]
        era5_8h_tp = era5_8h[:, :, 4]
        era5_8h_sp = era5_8h[:, :, 5]
        
        
        era5_9h_2t = era5_9h[:, :, 0]
        era5_9h_2d = era5_9h[:, :, 1]
        era5_9h_100u = era5_9h[:, :, 2]
        era5_9h_100v = era5_9h[:, :, 3]
        era5_9h_tp = era5_9h[:, :, 4]
        era5_9h_sp = era5_9h[:, :, 5]

        era5_10h_2t = era5_10h[:, :, 0]
        era5_10h_2d = era5_10h[:, :, 1]
        era5_10h_100u = era5_10h[:, :, 2]
        era5_10h_100v = era5_10h[:, :, 3]
        era5_10h_tp = era5_10h[:, :, 4]
        era5_10h_sp = era5_10h[:, :, 5]
        
        era5_11h_2t = era5_11h[:, :, 0]
        era5_11h_2d = era5_11h[:, :, 1]
        era5_11h_100u = era5_11h[:, :, 2]
        era5_11h_100v = era5_11h[:, :, 3]
        era5_11h_tp = era5_11h[:, :, 4]
        era5_11h_sp = era5_11h[:, :, 5]
        
        
        era5_12h_2t = era5_12h[:, :, 0]
        era5_12h_2d = era5_12h[:, :, 1]
        era5_12h_100u = era5_12h[:, :, 2]
        era5_12h_100v = era5_12h[:, :, 3]
        era5_12h_tp = era5_12h[:, :, 4]
        era5_12h_sp = era5_12h[:, :, 5]
        
        
        era5_13h_2t = era5_13h[:, :, 0]
        era5_13h_2d = era5_13h[:, :, 1]
        era5_13h_100u = era5_13h[:, :, 2]
        era5_13h_100v = era5_13h[:, :, 3]
        era5_13h_tp = era5_13h[:, :, 4]
        era5_13h_sp = era5_13h[:, :, 5]
        
        
        era5_14h_2t = era5_14h[:, :, 0]
        era5_14h_2d = era5_14h[:, :, 1]
        era5_14h_100u = era5_14h[:, :, 2]
        era5_14h_100v = era5_14h[:, :, 3]
        era5_14h_tp = era5_14h[:, :, 4]
        era5_14h_sp = era5_14h[:, :, 5]
        
        
        
        era5_15h_2t = era5_15h[:, :, 0]
        era5_15h_2d = era5_15h[:, :, 1]
        era5_15h_100u = era5_15h[:, :, 2]
        era5_15h_100v = era5_15h[:, :, 3]
        era5_15h_tp = era5_15h[:, :, 4]
        era5_15h_sp = era5_15h[:, :, 5]


        era5_16h_2t = era5_16h[:, :, 0]
        era5_16h_2d = era5_16h[:, :, 1]
        era5_16h_100u = era5_16h[:, :, 2]
        era5_16h_100v = era5_16h[:, :, 3]
        era5_16h_tp = era5_16h[:, :, 4]
        era5_16h_sp = era5_16h[:, :, 5]

        era5_17h_2t = era5_17h[:, :, 0]
        era5_17h_2d = era5_17h[:, :, 1]
        era5_17h_100u = era5_17h[:, :, 2]
        era5_17h_100v = era5_17h[:, :, 3]
        era5_17h_tp = era5_17h[:, :, 4]
        era5_17h_sp = era5_17h[:, :, 5]
        
        
        era5_18h_2t = era5_18h[:, :, 0]
        era5_18h_2d = era5_18h[:, :, 1]
        era5_18h_100u = era5_18h[:, :, 2]
        era5_18h_100v = era5_18h[:, :, 3]
        era5_18h_tp = era5_18h[:, :, 4]
        era5_18h_sp = era5_18h[:, :, 5]
        
        era5_19h_2t = era5_19h[:, :, 0]
        era5_19h_2d = era5_19h[:, :, 1]
        era5_19h_100u = era5_19h[:, :, 2]
        era5_19h_100v = era5_19h[:, :, 3]
        era5_19h_tp = era5_19h[:, :, 4]
        era5_19h_sp = era5_19h[:, :, 5]
        
        era5_20h_2t = era5_20h[:, :, 0]
        era5_20h_2d = era5_20h[:, :, 1]
        era5_20h_100u = era5_20h[:, :, 2]
        era5_20h_100v = era5_20h[:, :, 3]
        era5_20h_tp = era5_20h[:, :, 4]
        era5_20h_sp = era5_20h[:, :, 5]
        
        era5_21h_2t = era5_21h[:, :, 0]
        era5_21h_2d = era5_21h[:, :, 1]
        era5_21h_100u = era5_21h[:, :, 2]
        era5_21h_100v = era5_21h[:, :, 3]
        era5_21h_tp = era5_21h[:, :, 4]
        era5_21h_sp = era5_21h[:, :, 5]
        
        era5_22h_2t = era5_22h[:, :, 0]
        era5_22h_2d = era5_22h[:, :, 1]
        era5_22h_100u = era5_22h[:, :, 2]
        era5_22h_100v = era5_22h[:, :, 3]
        era5_22h_tp = era5_22h[:, :, 4]
        era5_22h_sp = era5_22h[:, :, 5]
        
        era5_23h_2t = era5_23h[:, :, 0]
        era5_23h_2d = era5_23h[:, :, 1]
        era5_23h_100u = era5_23h[:, :, 2]
        era5_23h_100v = era5_23h[:, :, 3]
        era5_23h_tp = era5_23h[:, :, 4]
        era5_23h_sp = era5_23h[:, :, 5]
        
        era5_24h_2t = era5_24h[:, :, 0]
        era5_24h_2d = era5_24h[:, :, 1]
        era5_24h_100u = era5_24h[:, :, 2]
        era5_24h_100v = era5_24h[:, :, 3]
        era5_24h_tp = era5_24h[:, :, 4]
        era5_24h_sp = era5_24h[:, :, 5]
        

        ens_aifs_1h_2t = ens_aifs_1h[:, :, 0]
        ens_aifs_1h_2d = ens_aifs_1h[:, :, 1]
        ens_aifs_1h_100u = ens_aifs_1h[:, :, 2]
        ens_aifs_1h_100v = ens_aifs_1h[:, :, 3]
        ens_aifs_1h_tp = ens_aifs_1h[:, :, 4]
        ens_aifs_1h_sp = ens_aifs_1h[:, :, 5]
        
        ens_aifs_2h_2t = ens_aifs_2h[:, :, 0]
        ens_aifs_2h_2d = ens_aifs_2h[:, :, 1]
        ens_aifs_2h_100u = ens_aifs_2h[:, :, 2]
        ens_aifs_2h_100v = ens_aifs_2h[:, :, 3]
        ens_aifs_2h_tp = ens_aifs_2h[:, :, 4]
        ens_aifs_2h_sp = ens_aifs_2h[:, :, 5]

        ens_aifs_3h_2t = ens_aifs_3h[:, :, 0]
        ens_aifs_3h_2d = ens_aifs_3h[:, :, 1]
        ens_aifs_3h_100u = ens_aifs_3h[:, :, 2]
        ens_aifs_3h_100v = ens_aifs_3h[:, :, 3]
        ens_aifs_3h_tp = ens_aifs_3h[:, :, 4]
        ens_aifs_3h_sp = ens_aifs_3h[:, :, 5]
        
        
        ens_aifs_4h_2t = ens_aifs_4h[:, :, 0]
        ens_aifs_4h_2d = ens_aifs_4h[:, :, 1]
        ens_aifs_4h_100u = ens_aifs_4h[:, :, 2]
        ens_aifs_4h_100v = ens_aifs_4h[:, :, 3]
        ens_aifs_4h_tp = ens_aifs_4h[:, :, 4]
        ens_aifs_4h_sp = ens_aifs_4h[:, :, 5]
        
        ens_aifs_5h_2t = ens_aifs_5h[:, :, 0]
        ens_aifs_5h_2d = ens_aifs_5h[:, :, 1]
        ens_aifs_5h_100u = ens_aifs_5h[:, :, 2]
        ens_aifs_5h_100v = ens_aifs_5h[:, :, 3]
        ens_aifs_5h_tp = ens_aifs_5h[:, :, 4]
        ens_aifs_5h_sp = ens_aifs_5h[:, :, 5]
        
        ens_aifs_6h_2t = ens_aifs_6h[:, :, 0]
        ens_aifs_6h_2d = ens_aifs_6h[:, :, 1]
        ens_aifs_6h_100u = ens_aifs_6h[:, :, 2]
        ens_aifs_6h_100v = ens_aifs_6h[:, :, 3]
        ens_aifs_6h_tp = ens_aifs_6h[:, :, 4]
        ens_aifs_6h_sp = ens_aifs_6h[:, :, 5]
        
        # Group features by hour and create one PNG per hour
        ens_aifs_7h_2t = ens_aifs_7h[:, :, 0]
        ens_aifs_7h_2d = ens_aifs_7h[:, :, 1]
        ens_aifs_7h_100u = ens_aifs_7h[:, :, 2]
        ens_aifs_7h_100v = ens_aifs_7h[:, :, 3]
        ens_aifs_7h_tp = ens_aifs_7h[:, :, 4]
        ens_aifs_7h_sp = ens_aifs_7h[:, :, 5]
        
        ens_aifs_8h_2t = ens_aifs_8h[:, :, 0]
        ens_aifs_8h_2d = ens_aifs_8h[:, :, 1]
        ens_aifs_8h_100u = ens_aifs_8h[:, :, 2]
        ens_aifs_8h_100v = ens_aifs_8h[:, :, 3]
        ens_aifs_8h_tp = ens_aifs_8h[:, :, 4]
        ens_aifs_8h_sp = ens_aifs_8h[:, :, 5]
        
        ens_aifs_9h_2t = ens_aifs_9h[:, :, 0]
        ens_aifs_9h_2d = ens_aifs_9h[:, :, 1]
        ens_aifs_9h_100u = ens_aifs_9h[:, :, 2]
        ens_aifs_9h_100v = ens_aifs_9h[:, :, 3]
        ens_aifs_9h_tp = ens_aifs_9h[:, :, 4]
        ens_aifs_9h_sp = ens_aifs_9h[:, :, 5]
        
        ens_aifs_10h_2t = ens_aifs_10h[:, :, 0]
        ens_aifs_10h_2d = ens_aifs_10h[:, :, 1]
        ens_aifs_10h_100u = ens_aifs_10h[:, :, 2]
        ens_aifs_10h_100v = ens_aifs_10h[:, :, 3]
        ens_aifs_10h_tp = ens_aifs_10h[:, :, 4]
        ens_aifs_10h_sp = ens_aifs_10h[:, :, 5]
        
        ens_aifs_11h_2t = ens_aifs_11h[:, :, 0]
        ens_aifs_11h_2d = ens_aifs_11h[:, :, 1]
        ens_aifs_11h_100u = ens_aifs_11h[:, :, 2]
        ens_aifs_11h_100v = ens_aifs_11h[:, :, 3]
        ens_aifs_11h_tp = ens_aifs_11h[:, :, 4]
        ens_aifs_11h_sp = ens_aifs_11h[:, :, 5]
        
        ens_aifs_12h_2t = ens_aifs_12h[:, :, 0]
        ens_aifs_12h_2d = ens_aifs_12h[:, :, 1]
        ens_aifs_12h_100u = ens_aifs_12h[:, :, 2]
        ens_aifs_12h_100v = ens_aifs_12h[:, :, 3]
        ens_aifs_12h_tp = ens_aifs_12h[:, :, 4]
        ens_aifs_12h_sp = ens_aifs_12h[:, :, 5]
        
        ens_aifs_13h_2t = ens_aifs_13h[:, :, 0]
        ens_aifs_13h_2d = ens_aifs_13h[:, :, 1]
        ens_aifs_13h_100u = ens_aifs_13h[:, :, 2]
        ens_aifs_13h_100v = ens_aifs_13h[:, :, 3]
        ens_aifs_13h_tp = ens_aifs_13h[:, :, 4]
        ens_aifs_13h_sp = ens_aifs_13h[:, :, 5]
        
        ens_aifs_14h_2t = ens_aifs_14h[:, :, 0]
        ens_aifs_14h_2d = ens_aifs_14h[:, :, 1]
        ens_aifs_14h_100u = ens_aifs_14h[:, :, 2]
        ens_aifs_14h_100v = ens_aifs_14h[:, :, 3]
        ens_aifs_14h_tp = ens_aifs_14h[:, :, 4]
        ens_aifs_14h_sp = ens_aifs_14h[:, :, 5]
        
        ens_aifs_15h_2t = ens_aifs_15h[:, :, 0]
        ens_aifs_15h_2d = ens_aifs_15h[:, :, 1]
        ens_aifs_15h_100u = ens_aifs_15h[:, :, 2]
        ens_aifs_15h_100v = ens_aifs_15h[:, :, 3]
        ens_aifs_15h_tp = ens_aifs_15h[:, :, 4]
        ens_aifs_15h_sp = ens_aifs_15h[:, :, 5]
        
        ens_aifs_16h_2t = ens_aifs_16h[:, :, 0]
        ens_aifs_16h_2d = ens_aifs_16h[:, :, 1]
        ens_aifs_16h_100u = ens_aifs_16h[:, :, 2]
        ens_aifs_16h_100v = ens_aifs_16h[:, :, 3]
        ens_aifs_16h_tp = ens_aifs_16h[:, :, 4]
        ens_aifs_16h_sp = ens_aifs_16h[:, :, 5]
        
        ens_aifs_17h_2t = ens_aifs_17h[:, :, 0]
        ens_aifs_17h_2d = ens_aifs_17h[:, :, 1]
        ens_aifs_17h_100u = ens_aifs_17h[:, :, 2]
        ens_aifs_17h_100v = ens_aifs_17h[:, :, 3]
        ens_aifs_17h_tp = ens_aifs_17h[:, :, 4]
        ens_aifs_17h_sp = ens_aifs_17h[:, :, 5]
        
        ens_aifs_18h_2t = ens_aifs_18h[:, :, 0]
        ens_aifs_18h_2d = ens_aifs_18h[:, :, 1]
        ens_aifs_18h_100u = ens_aifs_18h[:, :, 2]
        ens_aifs_18h_100v = ens_aifs_18h[:, :, 3]
        ens_aifs_18h_tp = ens_aifs_18h[:, :, 4]
        ens_aifs_18h_sp = ens_aifs_18h[:, :, 5]
        
        ens_aifs_19h_2t = ens_aifs_19h[:, :, 0]
        ens_aifs_19h_2d = ens_aifs_19h[:, :, 1]
        ens_aifs_19h_100u = ens_aifs_19h[:, :, 2]
        ens_aifs_19h_100v = ens_aifs_19h[:, :, 3]
        ens_aifs_19h_tp = ens_aifs_19h[:, :, 4]
        ens_aifs_19h_sp = ens_aifs_19h[:, :, 5]
        
        ens_aifs_20h_2t = ens_aifs_20h[:, :, 0]
        ens_aifs_20h_2d = ens_aifs_20h[:, :, 1]
        ens_aifs_20h_100u = ens_aifs_20h[:, :, 2]
        ens_aifs_20h_100v = ens_aifs_20h[:, :, 3]
        ens_aifs_20h_tp = ens_aifs_20h[:, :, 4]
        ens_aifs_20h_sp = ens_aifs_20h[:, :, 5]
        
        ens_aifs_21h_2t = ens_aifs_21h[:, :, 0]
        ens_aifs_21h_2d = ens_aifs_21h[:, :, 1]
        ens_aifs_21h_100u = ens_aifs_21h[:, :, 2]
        ens_aifs_21h_100v = ens_aifs_21h[:, :, 3]
        ens_aifs_21h_tp = ens_aifs_21h[:, :, 4]
        ens_aifs_21h_sp = ens_aifs_21h[:, :, 5]
        
        ens_aifs_22h_2t = ens_aifs_22h[:, :, 0]
        ens_aifs_22h_2d = ens_aifs_22h[:, :, 1]
        ens_aifs_22h_100u = ens_aifs_22h[:, :, 2]
        ens_aifs_22h_100v = ens_aifs_22h[:, :, 3]
        ens_aifs_22h_tp = ens_aifs_22h[:, :, 4]
        ens_aifs_22h_sp = ens_aifs_22h[:, :, 5]
        
        ens_aifs_23h_2t = ens_aifs_23h[:, :, 0]
        ens_aifs_23h_2d = ens_aifs_23h[:, :, 1]
        ens_aifs_23h_100u = ens_aifs_23h[:, :, 2]
        ens_aifs_23h_100v = ens_aifs_23h[:, :, 3]
        ens_aifs_23h_tp = ens_aifs_23h[:, :, 4]
        ens_aifs_23h_sp = ens_aifs_23h[:, :, 5]
        
        ens_aifs_24h_2t = ens_aifs_24h[:, :, 0]
        ens_aifs_24h_2d = ens_aifs_24h[:, :, 1]
        ens_aifs_24h_100u = ens_aifs_24h[:, :, 2]
        ens_aifs_24h_100v = ens_aifs_24h[:, :, 3]
        ens_aifs_24h_tp = ens_aifs_24h[:, :, 4]
        ens_aifs_24h_sp = ens_aifs_24h[:, :, 5]



        # Hours: 1h, 2h, 3h, 4h, 5h, 6h, 7h, 8h, 9h, 10h, 11h, 12h, 13h, 14h, 15h, 16h, 17h, 18h, 19h, 20h, 21h, 22h, 23h, 24h
        hours_data = {
            '1h': {
                'best_match': [best_match_1h_2t, best_match_1h_2d, best_match_1h_100u, best_match_1h_100v, best_match_1h_tp, best_match_1h_sp],
                'ecmwf_ifs': [ecmwf_ifs_1h_2t, ecmwf_ifs_1h_2d, ecmwf_ifs_1h_100u, ecmwf_ifs_1h_100v, ecmwf_ifs_1h_tp, ecmwf_ifs_1h_sp],
                'gfs_global': [gfs_global_1h_2t, gfs_global_1h_2d, gfs_global_1h_100u, gfs_global_1h_100v, gfs_global_1h_tp, gfs_global_1h_sp],
                'AIFS': [AIFS_1h_2t, AIFS_1h_2d, AIFS_1h_100u, AIFS_1h_100v, AIFS_1h_tp, AIFS_1h_sp],
                'CMA': [CMA_1h_2t, CMA_1h_2d, CMA_1h_100u, CMA_1h_100v, CMA_1h_tp, CMA_1h_sp],
                'era5': [era5_1h_2t, era5_1h_2d, era5_1h_100u, era5_1h_100v, era5_1h_tp, era5_1h_sp],
                'ens_aifs': [ens_aifs_1h_2t, ens_aifs_1h_2d, ens_aifs_1h_100u, ens_aifs_1h_100v, ens_aifs_1h_tp, ens_aifs_1h_sp],
                'feature_names': ['temperature_2t', 'dewpoint_2d', 'u100_100u', 'v100_100v', 'precipitation_tp', 'sp']
            },
            '2h': {
                'best_match': [best_match_2h_2t, best_match_2h_2d, best_match_2h_100u, best_match_2h_100v, best_match_2h_tp, best_match_2h_sp],
                'ecmwf_ifs': [ecmwf_ifs_2h_2t, ecmwf_ifs_2h_2d, ecmwf_ifs_2h_100u, ecmwf_ifs_2h_100v, ecmwf_ifs_2h_tp, ecmwf_ifs_2h_sp],
                'gfs_global': [gfs_global_2h_2t, gfs_global_2h_2d, gfs_global_2h_100u, gfs_global_2h_100v, gfs_global_2h_tp, gfs_global_2h_sp],
                'AIFS': [AIFS_2h_2t, AIFS_2h_2d, AIFS_2h_100u, AIFS_2h_100v, AIFS_2h_tp, AIFS_2h_sp],
                'CMA': [CMA_2h_2t, CMA_2h_2d, CMA_2h_100u, CMA_2h_100v, CMA_2h_tp, CMA_2h_sp],
                'era5': [era5_2h_2t, era5_2h_2d, era5_2h_100u, era5_2h_100v, era5_2h_tp, era5_2h_sp],
                'ens_aifs': [ens_aifs_2h_2t, ens_aifs_2h_2d, ens_aifs_2h_100u, ens_aifs_2h_100v, ens_aifs_2h_tp, ens_aifs_2h_sp],
                'feature_names': ['temperature_2t', 'dewpoint_2d', 'u100_100u', 'v100_100v', 'precipitation_tp', 'sp']
            },
            '3h': {
                'best_match': [best_match_3h_2t, best_match_3h_2d, best_match_3h_100u, best_match_3h_100v, best_match_3h_tp, best_match_3h_sp],
                'ecmwf_ifs': [ecmwf_ifs_3h_2t, ecmwf_ifs_3h_2d, ecmwf_ifs_3h_100u, ecmwf_ifs_3h_100v, ecmwf_ifs_3h_tp, ecmwf_ifs_3h_sp],
                'gfs_global': [gfs_global_3h_2t, gfs_global_3h_2d, gfs_global_3h_100u, gfs_global_3h_100v, gfs_global_3h_tp, gfs_global_3h_sp],
                'AIFS': [AIFS_3h_2t, AIFS_3h_2d, AIFS_3h_100u, AIFS_3h_100v, AIFS_3h_tp, AIFS_3h_sp],
                'CMA': [CMA_3h_2t, CMA_3h_2d, CMA_3h_100u, CMA_3h_100v, CMA_3h_tp, CMA_3h_sp],
                'era5': [era5_3h_2t, era5_3h_2d, era5_3h_100u, era5_3h_100v, era5_3h_tp, era5_3h_sp],
                'ens_aifs': [ens_aifs_3h_2t, ens_aifs_3h_2d, ens_aifs_3h_100u, ens_aifs_3h_100v, ens_aifs_3h_tp, ens_aifs_3h_sp],
                'feature_names': ['temperature_2t', 'dewpoint_2d', 'u100_100u', 'v100_100v', 'precipitation_tp', 'sp']
            },
            '4h': {
                'best_match': [best_match_4h_2t, best_match_4h_2d, best_match_4h_100u, best_match_4h_100v, best_match_4h_tp, best_match_4h_sp],
                'ecmwf_ifs': [ecmwf_ifs_4h_2t, ecmwf_ifs_4h_2d, ecmwf_ifs_4h_100u, ecmwf_ifs_4h_100v, ecmwf_ifs_4h_tp, ecmwf_ifs_4h_sp],
                'gfs_global': [gfs_global_4h_2t, gfs_global_4h_2d, gfs_global_4h_100u, gfs_global_4h_100v, gfs_global_4h_tp, gfs_global_4h_sp],
                'AIFS': [AIFS_4h_2t, AIFS_4h_2d, AIFS_4h_100u, AIFS_4h_100v, AIFS_4h_tp, AIFS_4h_sp],
                'CMA': [CMA_4h_2t, CMA_4h_2d, CMA_4h_100u, CMA_4h_100v, CMA_4h_tp, CMA_4h_sp],
                'era5': [era5_4h_2t, era5_4h_2d, era5_4h_100u, era5_4h_100v, era5_4h_tp, era5_4h_sp],
                'ens_aifs': [ens_aifs_4h_2t, ens_aifs_4h_2d, ens_aifs_4h_100u, ens_aifs_4h_100v, ens_aifs_4h_tp, ens_aifs_4h_sp],
                'feature_names': ['temperature_2t', 'dewpoint_2d', 'u100_100u', 'v100_100v', 'precipitation_tp', 'sp']
            },
            '5h': {
                'best_match': [best_match_5h_2t, best_match_5h_2d, best_match_5h_100u, best_match_5h_100v, best_match_5h_tp, best_match_5h_sp],
                'ecmwf_ifs': [ecmwf_ifs_5h_2t, ecmwf_ifs_5h_2d, ecmwf_ifs_5h_100u, ecmwf_ifs_5h_100v, ecmwf_ifs_5h_tp, ecmwf_ifs_5h_sp],
                'gfs_global': [gfs_global_5h_2t, gfs_global_5h_2d, gfs_global_5h_100u, gfs_global_5h_100v, gfs_global_5h_tp, gfs_global_5h_sp],
                'AIFS': [AIFS_5h_2t, AIFS_5h_2d, AIFS_5h_100u, AIFS_5h_100v, AIFS_5h_tp, AIFS_5h_sp],
                'CMA': [CMA_5h_2t, CMA_5h_2d, CMA_5h_100u, CMA_5h_100v, CMA_5h_tp, CMA_5h_sp],
                'era5': [era5_5h_2t, era5_5h_2d, era5_5h_100u, era5_5h_100v, era5_5h_tp, era5_5h_sp],
                'ens_aifs': [ens_aifs_5h_2t, ens_aifs_5h_2d, ens_aifs_5h_100u, ens_aifs_5h_100v, ens_aifs_5h_tp, ens_aifs_5h_sp],
                'feature_names': ['temperature_2t', 'dewpoint_2d', 'u100_100u', 'v100_100v', 'precipitation_tp', 'sp']
            },
            '6h': {
                'best_match': [best_match_6h_2t, best_match_6h_2d, best_match_6h_100u, best_match_6h_100v, best_match_6h_tp, best_match_6h_sp],
                'ecmwf_ifs': [ecmwf_ifs_6h_2t, ecmwf_ifs_6h_2d, ecmwf_ifs_6h_100u, ecmwf_ifs_6h_100v, ecmwf_ifs_6h_tp, ecmwf_ifs_6h_sp],
                'gfs_global': [gfs_global_6h_2t, gfs_global_6h_2d, gfs_global_6h_100u, gfs_global_6h_100v, gfs_global_6h_tp, gfs_global_6h_sp],
                'AIFS': [AIFS_6h_2t, AIFS_6h_2d, AIFS_6h_100u, AIFS_6h_100v, AIFS_6h_tp, AIFS_6h_sp],
                'CMA': [CMA_6h_2t, CMA_6h_2d, CMA_6h_100u, CMA_6h_100v, CMA_6h_tp, CMA_6h_sp],
                'era5': [era5_6h_2t, era5_6h_2d, era5_6h_100u, era5_6h_100v, era5_6h_tp, era5_6h_sp],
                'ens_aifs': [ens_aifs_6h_2t, ens_aifs_6h_2d, ens_aifs_6h_100u, ens_aifs_6h_100v, ens_aifs_6h_tp, ens_aifs_6h_sp],
                'feature_names': ['temperature_2t', 'dewpoint_2d', 'u100_100u', 'v100_100v', 'precipitation_tp', 'sp']
            },
            '7h': {
                'best_match': [best_match_7h_2t, best_match_7h_2d, best_match_7h_100u, best_match_7h_100v, best_match_7h_tp, best_match_7h_sp],
                'ecmwf_ifs': [ecmwf_ifs_7h_2t, ecmwf_ifs_7h_2d, ecmwf_ifs_7h_100u, ecmwf_ifs_7h_100v, ecmwf_ifs_7h_tp, ecmwf_ifs_7h_sp],
                'gfs_global': [gfs_global_7h_2t, gfs_global_7h_2d, gfs_global_7h_100u, gfs_global_7h_100v, gfs_global_7h_tp, gfs_global_7h_sp],
                'AIFS': [AIFS_7h_2t, AIFS_7h_2d, AIFS_7h_100u, AIFS_7h_100v, AIFS_7h_tp, AIFS_7h_sp],
                'CMA': [CMA_7h_2t, CMA_7h_2d, CMA_7h_100u, CMA_7h_100v, CMA_7h_tp, CMA_7h_sp],
                'era5': [era5_7h_2t, era5_7h_2d, era5_7h_100u, era5_7h_100v, era5_7h_tp, era5_7h_sp],
                'ens_aifs': [ens_aifs_7h_2t, ens_aifs_7h_2d, ens_aifs_7h_100u, ens_aifs_7h_100v, ens_aifs_7h_tp, ens_aifs_7h_sp],
                'feature_names': ['temperature_2t', 'dewpoint_2d', 'u100_100u', 'v100_100v', 'precipitation_tp', 'sp']
            },
            '8h': {
                'best_match': [best_match_8h_2t, best_match_8h_2d, best_match_8h_100u, best_match_8h_100v, best_match_8h_tp, best_match_8h_sp],
                'ecmwf_ifs': [ecmwf_ifs_8h_2t, ecmwf_ifs_8h_2d, ecmwf_ifs_8h_100u, ecmwf_ifs_8h_100v, ecmwf_ifs_8h_tp, ecmwf_ifs_8h_sp],
                'gfs_global': [gfs_global_8h_2t, gfs_global_8h_2d, gfs_global_8h_100u, gfs_global_8h_100v, gfs_global_8h_tp, gfs_global_8h_sp],
                'AIFS': [AIFS_8h_2t, AIFS_8h_2d, AIFS_8h_100u, AIFS_8h_100v, AIFS_8h_tp, AIFS_8h_sp],
                'CMA': [CMA_8h_2t, CMA_8h_2d, CMA_8h_100u, CMA_8h_100v, CMA_8h_tp, CMA_8h_sp],
                'era5': [era5_8h_2t, era5_8h_2d, era5_8h_100u, era5_8h_100v, era5_8h_tp, era5_8h_sp],
                'ens_aifs': [ens_aifs_8h_2t, ens_aifs_8h_2d, ens_aifs_8h_100u, ens_aifs_8h_100v, ens_aifs_8h_tp, ens_aifs_8h_sp],
                'feature_names': ['temperature_2t', 'dewpoint_2d', 'u100_100u', 'v100_100v', 'precipitation_tp', 'sp']
            },
            '9h': {
                'best_match': [best_match_9h_2t, best_match_9h_2d, best_match_9h_100u, best_match_9h_100v, best_match_9h_tp, best_match_9h_sp],
                'ecmwf_ifs': [ecmwf_ifs_9h_2t, ecmwf_ifs_9h_2d, ecmwf_ifs_9h_100u, ecmwf_ifs_9h_100v, ecmwf_ifs_9h_tp, ecmwf_ifs_9h_sp],
                'gfs_global': [gfs_global_9h_2t, gfs_global_9h_2d, gfs_global_9h_100u, gfs_global_9h_100v, gfs_global_9h_tp, gfs_global_9h_sp],
                'AIFS': [AIFS_9h_2t, AIFS_9h_2d, AIFS_9h_100u, AIFS_9h_100v, AIFS_9h_tp, AIFS_9h_sp],
                'CMA': [CMA_9h_2t, CMA_9h_2d, CMA_9h_100u, CMA_9h_100v, CMA_9h_tp, CMA_9h_sp],
                'era5': [era5_9h_2t, era5_9h_2d, era5_9h_100u, era5_9h_100v, era5_9h_tp, era5_9h_sp],
                'ens_aifs': [ens_aifs_9h_2t, ens_aifs_9h_2d, ens_aifs_9h_100u, ens_aifs_9h_100v, ens_aifs_9h_tp, ens_aifs_9h_sp],
                'feature_names': ['temperature_2t', 'dewpoint_2d', 'u100_100u', 'v100_100v', 'precipitation_tp', 'sp']
            },
            '10h': {
                'best_match': [best_match_10h_2t, best_match_10h_2d, best_match_10h_100u, best_match_10h_100v, best_match_10h_tp, best_match_10h_sp],
                'ecmwf_ifs': [ecmwf_ifs_10h_2t, ecmwf_ifs_10h_2d, ecmwf_ifs_10h_100u, ecmwf_ifs_10h_100v, ecmwf_ifs_10h_tp, ecmwf_ifs_10h_sp],
                'gfs_global': [gfs_global_10h_2t, gfs_global_10h_2d, gfs_global_10h_100u, gfs_global_10h_100v, gfs_global_10h_tp, gfs_global_10h_sp],
                'AIFS': [AIFS_10h_2t, AIFS_10h_2d, AIFS_10h_100u, AIFS_10h_100v, AIFS_10h_tp, AIFS_10h_sp],
                'CMA': [CMA_10h_2t, CMA_10h_2d, CMA_10h_100u, CMA_10h_100v, CMA_10h_tp, CMA_10h_sp],
                'era5': [era5_10h_2t, era5_10h_2d, era5_10h_100u, era5_10h_100v, era5_10h_tp, era5_10h_sp],
                'ens_aifs': [ens_aifs_10h_2t, ens_aifs_10h_2d, ens_aifs_10h_100u, ens_aifs_10h_100v, ens_aifs_10h_tp, ens_aifs_10h_sp],
                'feature_names': ['temperature_2t', 'dewpoint_2d', 'u100_100u', 'v100_100v', 'precipitation_tp', 'sp']
            },
            '11h': {
                'best_match': [best_match_11h_2t, best_match_11h_2d, best_match_11h_100u, best_match_11h_100v, best_match_11h_tp, best_match_11h_sp],
                'ecmwf_ifs': [ecmwf_ifs_11h_2t, ecmwf_ifs_11h_2d, ecmwf_ifs_11h_100u, ecmwf_ifs_11h_100v, ecmwf_ifs_11h_tp, ecmwf_ifs_11h_sp],
                'gfs_global': [gfs_global_11h_2t, gfs_global_11h_2d, gfs_global_11h_100u, gfs_global_11h_100v, gfs_global_11h_tp, gfs_global_11h_sp],
                'AIFS': [AIFS_11h_2t, AIFS_11h_2d, AIFS_11h_100u, AIFS_11h_100v, AIFS_11h_tp, AIFS_11h_sp],
                'CMA': [CMA_11h_2t, CMA_11h_2d, CMA_11h_100u, CMA_11h_100v, CMA_11h_tp, CMA_11h_sp],
                'era5': [era5_11h_2t, era5_11h_2d, era5_11h_100u, era5_11h_100v, era5_11h_tp, era5_11h_sp],
                'ens_aifs': [ens_aifs_11h_2t, ens_aifs_11h_2d, ens_aifs_11h_100u, ens_aifs_11h_100v, ens_aifs_11h_tp, ens_aifs_11h_sp],
                'feature_names': ['temperature_2t', 'dewpoint_2d', 'u100_100u', 'v100_100v', 'precipitation_tp', 'sp']
            },
            '12h': {
                'best_match': [best_match_12h_2t, best_match_12h_2d, best_match_12h_100u, best_match_12h_100v, best_match_12h_tp, best_match_12h_sp],
                'ecmwf_ifs': [ecmwf_ifs_12h_2t, ecmwf_ifs_12h_2d, ecmwf_ifs_12h_100u, ecmwf_ifs_12h_100v, ecmwf_ifs_12h_tp, ecmwf_ifs_12h_sp],
                'gfs_global': [gfs_global_12h_2t, gfs_global_12h_2d, gfs_global_12h_100u, gfs_global_12h_100v, gfs_global_12h_tp, gfs_global_12h_sp],
                'AIFS': [AIFS_12h_2t, AIFS_12h_2d, AIFS_12h_100u, AIFS_12h_100v, AIFS_12h_tp, AIFS_12h_sp],
                'CMA': [CMA_12h_2t, CMA_12h_2d, CMA_12h_100u, CMA_12h_100v, CMA_12h_tp, CMA_12h_sp],
                'era5': [era5_12h_2t, era5_12h_2d, era5_12h_100u, era5_12h_100v, era5_12h_tp, era5_12h_sp],
                'ens_aifs': [ens_aifs_12h_2t, ens_aifs_12h_2d, ens_aifs_12h_100u, ens_aifs_12h_100v, ens_aifs_12h_tp, ens_aifs_12h_sp],
                'feature_names': ['temperature_2t', 'dewpoint_2d', 'u100_100u', 'v100_100v', 'precipitation_tp', 'sp']
            },
            '13h': {
                'best_match': [best_match_13h_2t, best_match_13h_2d, best_match_13h_100u, best_match_13h_100v, best_match_13h_tp, best_match_13h_sp],
                'ecmwf_ifs': [ecmwf_ifs_13h_2t, ecmwf_ifs_13h_2d, ecmwf_ifs_13h_100u, ecmwf_ifs_13h_100v, ecmwf_ifs_13h_tp, ecmwf_ifs_13h_sp],
                'gfs_global': [gfs_global_13h_2t, gfs_global_13h_2d, gfs_global_13h_100u, gfs_global_13h_100v, gfs_global_13h_tp, gfs_global_13h_sp],
                'AIFS': [AIFS_13h_2t, AIFS_13h_2d, AIFS_13h_100u, AIFS_13h_100v, AIFS_13h_tp, AIFS_13h_sp],
                'CMA': [CMA_13h_2t, CMA_13h_2d, CMA_13h_100u, CMA_13h_100v, CMA_13h_tp, CMA_13h_sp],
                'era5': [era5_13h_2t, era5_13h_2d, era5_13h_100u, era5_13h_100v, era5_13h_tp, era5_13h_sp],
                'ens_aifs': [ens_aifs_13h_2t, ens_aifs_13h_2d, ens_aifs_13h_100u, ens_aifs_13h_100v, ens_aifs_13h_tp, ens_aifs_13h_sp],
                'feature_names': ['temperature_2t', 'dewpoint_2d', 'u100_100u', 'v100_100v', 'precipitation_tp', 'sp']
            },
            '14h': {
                'best_match': [best_match_14h_2t, best_match_14h_2d, best_match_14h_100u, best_match_14h_100v, best_match_14h_tp, best_match_14h_sp],
                'ecmwf_ifs': [ecmwf_ifs_14h_2t, ecmwf_ifs_14h_2d, ecmwf_ifs_14h_100u, ecmwf_ifs_14h_100v, ecmwf_ifs_14h_tp, ecmwf_ifs_14h_sp],
                'gfs_global': [gfs_global_14h_2t, gfs_global_14h_2d, gfs_global_14h_100u, gfs_global_14h_100v, gfs_global_14h_tp, gfs_global_14h_sp],
                'AIFS': [AIFS_14h_2t, AIFS_14h_2d, AIFS_14h_100u, AIFS_14h_100v, AIFS_14h_tp, AIFS_14h_sp],
                'CMA': [CMA_14h_2t, CMA_14h_2d, CMA_14h_100u, CMA_14h_100v, CMA_14h_tp, CMA_14h_sp],
                'era5': [era5_14h_2t, era5_14h_2d, era5_14h_100u, era5_14h_100v, era5_14h_tp, era5_14h_sp],
                'ens_aifs': [ens_aifs_14h_2t, ens_aifs_14h_2d, ens_aifs_14h_100u, ens_aifs_14h_100v, ens_aifs_14h_tp, ens_aifs_14h_sp],
                'feature_names': ['temperature_2t', 'dewpoint_2d', 'u100_100u', 'v100_100v', 'precipitation_tp', 'sp']
            },
            '15h': {
                'best_match': [best_match_15h_2t, best_match_15h_2d, best_match_15h_100u, best_match_15h_100v, best_match_15h_tp, best_match_15h_sp],
                'ecmwf_ifs': [ecmwf_ifs_15h_2t, ecmwf_ifs_15h_2d, ecmwf_ifs_15h_100u, ecmwf_ifs_15h_100v, ecmwf_ifs_15h_tp, ecmwf_ifs_15h_sp],
                'gfs_global': [gfs_global_15h_2t, gfs_global_15h_2d, gfs_global_15h_100u, gfs_global_15h_100v, gfs_global_15h_tp, gfs_global_15h_sp],
                'AIFS': [AIFS_15h_2t, AIFS_15h_2d, AIFS_15h_100u, AIFS_15h_100v, AIFS_15h_tp, AIFS_15h_sp],
                'CMA': [CMA_15h_2t, CMA_15h_2d, CMA_15h_100u, CMA_15h_100v, CMA_15h_tp, CMA_15h_sp],
                'era5': [era5_15h_2t, era5_15h_2d, era5_15h_100u, era5_15h_100v, era5_15h_tp, era5_15h_sp],
                'ens_aifs': [ens_aifs_15h_2t, ens_aifs_15h_2d, ens_aifs_15h_100u, ens_aifs_15h_100v, ens_aifs_15h_tp, ens_aifs_15h_sp],
                'feature_names': ['temperature_2t', 'dewpoint_2d', 'u100_100u', 'v100_100v', 'precipitation_tp', 'sp']
            },
            '16h': {
                'best_match': [best_match_16h_2t, best_match_16h_2d, best_match_16h_100u, best_match_16h_100v, best_match_16h_tp, best_match_16h_sp],
                'ecmwf_ifs': [ecmwf_ifs_16h_2t, ecmwf_ifs_16h_2d, ecmwf_ifs_16h_100u, ecmwf_ifs_16h_100v, ecmwf_ifs_16h_tp, ecmwf_ifs_16h_sp],
                'gfs_global': [gfs_global_16h_2t, gfs_global_16h_2d, gfs_global_16h_100u, gfs_global_16h_100v, gfs_global_16h_tp, gfs_global_16h_sp],
                'AIFS': [AIFS_16h_2t, AIFS_16h_2d, AIFS_16h_100u, AIFS_16h_100v, AIFS_16h_tp, AIFS_16h_sp],
                'CMA': [CMA_16h_2t, CMA_16h_2d, CMA_16h_100u, CMA_16h_100v, CMA_16h_tp, CMA_16h_sp],
                'era5': [era5_16h_2t, era5_16h_2d, era5_16h_100u, era5_16h_100v, era5_16h_tp, era5_16h_sp],
                'ens_aifs': [ens_aifs_16h_2t, ens_aifs_16h_2d, ens_aifs_16h_100u, ens_aifs_16h_100v, ens_aifs_16h_tp, ens_aifs_16h_sp],
                'feature_names': ['temperature_2t', 'dewpoint_2d', 'u100_100u', 'v100_100v', 'precipitation_tp', 'sp']
            },
            '17h': {
                'best_match': [best_match_17h_2t, best_match_17h_2d, best_match_17h_100u, best_match_17h_100v, best_match_17h_tp, best_match_17h_sp],
                'ecmwf_ifs': [ecmwf_ifs_17h_2t, ecmwf_ifs_17h_2d, ecmwf_ifs_17h_100u, ecmwf_ifs_17h_100v, ecmwf_ifs_17h_tp, ecmwf_ifs_17h_sp],
                'gfs_global': [gfs_global_17h_2t, gfs_global_17h_2d, gfs_global_17h_100u, gfs_global_17h_100v, gfs_global_17h_tp, gfs_global_17h_sp],
                'AIFS': [AIFS_17h_2t, AIFS_17h_2d, AIFS_17h_100u, AIFS_17h_100v, AIFS_17h_tp, AIFS_17h_sp],
                'CMA': [CMA_17h_2t, CMA_17h_2d, CMA_17h_100u, CMA_17h_100v, CMA_17h_tp, CMA_17h_sp],
                'era5': [era5_17h_2t, era5_17h_2d, era5_17h_100u, era5_17h_100v, era5_17h_tp, era5_17h_sp],
                'ens_aifs': [ens_aifs_17h_2t, ens_aifs_17h_2d, ens_aifs_17h_100u, ens_aifs_17h_100v, ens_aifs_17h_tp, ens_aifs_17h_sp],
                'feature_names': ['temperature_2t', 'dewpoint_2d', 'u100_100u', 'v100_100v', 'precipitation_tp', 'sp']
            },
            '18h': {
                'best_match': [best_match_18h_2t, best_match_18h_2d, best_match_18h_100u, best_match_18h_100v, best_match_18h_tp, best_match_18h_sp],
                'ecmwf_ifs': [ecmwf_ifs_18h_2t, ecmwf_ifs_18h_2d, ecmwf_ifs_18h_100u, ecmwf_ifs_18h_100v, ecmwf_ifs_18h_tp, ecmwf_ifs_18h_sp],
                'gfs_global': [gfs_global_18h_2t, gfs_global_18h_2d, gfs_global_18h_100u, gfs_global_18h_100v, gfs_global_18h_tp, gfs_global_18h_sp],
                'AIFS': [AIFS_18h_2t, AIFS_18h_2d, AIFS_18h_100u, AIFS_18h_100v, AIFS_18h_tp, AIFS_18h_sp],
                'CMA': [CMA_18h_2t, CMA_18h_2d, CMA_18h_100u, CMA_18h_100v, CMA_18h_tp, CMA_18h_sp],
                'era5': [era5_18h_2t, era5_18h_2d, era5_18h_100u, era5_18h_100v, era5_18h_tp, era5_18h_sp],
                'ens_aifs': [ens_aifs_18h_2t, ens_aifs_18h_2d, ens_aifs_18h_100u, ens_aifs_18h_100v, ens_aifs_18h_tp, ens_aifs_18h_sp],
                'feature_names': ['temperature_2t', 'dewpoint_2d', 'u100_100u', 'v100_100v', 'precipitation_tp', 'sp']
            },
            '19h': {
                'best_match': [best_match_19h_2t, best_match_19h_2d, best_match_19h_100u, best_match_19h_100v, best_match_19h_tp, best_match_19h_sp],
                'ecmwf_ifs': [ecmwf_ifs_19h_2t, ecmwf_ifs_19h_2d, ecmwf_ifs_19h_100u, ecmwf_ifs_19h_100v, ecmwf_ifs_19h_tp, ecmwf_ifs_19h_sp],
                'gfs_global': [gfs_global_19h_2t, gfs_global_19h_2d, gfs_global_19h_100u, gfs_global_19h_100v, gfs_global_19h_tp, gfs_global_19h_sp],
                'AIFS': [AIFS_19h_2t, AIFS_19h_2d, AIFS_19h_100u, AIFS_19h_100v, AIFS_19h_tp, AIFS_19h_sp],
                'CMA': [CMA_19h_2t, CMA_19h_2d, CMA_19h_100u, CMA_19h_100v, CMA_19h_tp, CMA_19h_sp],
                'era5': [era5_19h_2t, era5_19h_2d, era5_19h_100u, era5_19h_100v, era5_19h_tp, era5_19h_sp],
                'ens_aifs': [ens_aifs_19h_2t, ens_aifs_19h_2d, ens_aifs_19h_100u, ens_aifs_19h_100v, ens_aifs_19h_tp, ens_aifs_19h_sp],
                'feature_names': ['temperature_2t', 'dewpoint_2d', 'u100_100u', 'v100_100v', 'precipitation_tp', 'sp']
            },
            '20h': {
                'best_match': [best_match_20h_2t, best_match_20h_2d, best_match_20h_100u, best_match_20h_100v, best_match_20h_tp, best_match_20h_sp],
                'ecmwf_ifs': [ecmwf_ifs_20h_2t, ecmwf_ifs_20h_2d, ecmwf_ifs_20h_100u, ecmwf_ifs_20h_100v, ecmwf_ifs_20h_tp, ecmwf_ifs_20h_sp],
                'gfs_global': [gfs_global_20h_2t, gfs_global_20h_2d, gfs_global_20h_100u, gfs_global_20h_100v, gfs_global_20h_tp, gfs_global_20h_sp],
                'AIFS': [AIFS_20h_2t, AIFS_20h_2d, AIFS_20h_100u, AIFS_20h_100v, AIFS_20h_tp, AIFS_20h_sp],
                'CMA': [CMA_20h_2t, CMA_20h_2d, CMA_20h_100u, CMA_20h_100v, CMA_20h_tp, CMA_20h_sp],
                'era5': [era5_20h_2t, era5_20h_2d, era5_20h_100u, era5_20h_100v, era5_20h_tp, era5_20h_sp],
                'ens_aifs': [ens_aifs_20h_2t, ens_aifs_20h_2d, ens_aifs_20h_100u, ens_aifs_20h_100v, ens_aifs_20h_tp, ens_aifs_20h_sp],
                'feature_names': ['temperature_2t', 'dewpoint_2d', 'u100_100u', 'v100_100v', 'precipitation_tp', 'sp']
            },
            '21h': {
                'best_match': [best_match_21h_2t, best_match_21h_2d, best_match_21h_100u, best_match_21h_100v, best_match_21h_tp, best_match_21h_sp],
                'ecmwf_ifs': [ecmwf_ifs_21h_2t, ecmwf_ifs_21h_2d, ecmwf_ifs_21h_100u, ecmwf_ifs_21h_100v, ecmwf_ifs_21h_tp, ecmwf_ifs_21h_sp],
                'gfs_global': [gfs_global_21h_2t, gfs_global_21h_2d, gfs_global_21h_100u, gfs_global_21h_100v, gfs_global_21h_tp, gfs_global_21h_sp],
                'AIFS': [AIFS_21h_2t, AIFS_21h_2d, AIFS_21h_100u, AIFS_21h_100v, AIFS_21h_tp, AIFS_21h_sp],
                'CMA': [CMA_21h_2t, CMA_21h_2d, CMA_21h_100u, CMA_21h_100v, CMA_21h_tp, CMA_21h_sp],
                'era5': [era5_21h_2t, era5_21h_2d, era5_21h_100u, era5_21h_100v, era5_21h_tp, era5_21h_sp],
                'ens_aifs': [ens_aifs_21h_2t, ens_aifs_21h_2d, ens_aifs_21h_100u, ens_aifs_21h_100v, ens_aifs_21h_tp, ens_aifs_21h_sp],
                'feature_names': ['temperature_2t', 'dewpoint_2d', 'u100_100u', 'v100_100v', 'precipitation_tp', 'sp']
            },
            '22h': {
                'best_match': [best_match_22h_2t, best_match_22h_2d, best_match_22h_100u, best_match_22h_100v, best_match_22h_tp, best_match_22h_sp],
                'ecmwf_ifs': [ecmwf_ifs_22h_2t, ecmwf_ifs_22h_2d, ecmwf_ifs_22h_100u, ecmwf_ifs_22h_100v, ecmwf_ifs_22h_tp, ecmwf_ifs_22h_sp],
                'gfs_global': [gfs_global_22h_2t, gfs_global_22h_2d, gfs_global_22h_100u, gfs_global_22h_100v, gfs_global_22h_tp, gfs_global_22h_sp],
                'AIFS': [AIFS_22h_2t, AIFS_22h_2d, AIFS_22h_100u, AIFS_22h_100v, AIFS_22h_tp, AIFS_22h_sp],
                'CMA': [CMA_22h_2t, CMA_22h_2d, CMA_22h_100u, CMA_22h_100v, CMA_22h_tp, CMA_22h_sp],
                'era5': [era5_22h_2t, era5_22h_2d, era5_22h_100u, era5_22h_100v, era5_22h_tp, era5_22h_sp],
                'ens_aifs': [ens_aifs_22h_2t, ens_aifs_22h_2d, ens_aifs_22h_100u, ens_aifs_22h_100v, ens_aifs_22h_tp, ens_aifs_22h_sp],
                'feature_names': ['temperature_2t', 'dewpoint_2d', 'u100_100u', 'v100_100v', 'precipitation_tp', 'sp']
            },
            '23h': {
                'best_match': [best_match_23h_2t, best_match_23h_2d, best_match_23h_100u, best_match_23h_100v, best_match_23h_tp, best_match_23h_sp],
                'ecmwf_ifs': [ecmwf_ifs_23h_2t, ecmwf_ifs_23h_2d, ecmwf_ifs_23h_100u, ecmwf_ifs_23h_100v, ecmwf_ifs_23h_tp, ecmwf_ifs_23h_sp],
                'gfs_global': [gfs_global_23h_2t, gfs_global_23h_2d, gfs_global_23h_100u, gfs_global_23h_100v, gfs_global_23h_tp, gfs_global_23h_sp],
                'AIFS': [AIFS_23h_2t, AIFS_23h_2d, AIFS_23h_100u, AIFS_23h_100v, AIFS_23h_tp, AIFS_23h_sp],
                'CMA': [CMA_23h_2t, CMA_23h_2d, CMA_23h_100u, CMA_23h_100v, CMA_23h_tp, CMA_23h_sp],
                'era5': [era5_23h_2t, era5_23h_2d, era5_23h_100u, era5_23h_100v, era5_23h_tp, era5_23h_sp],
                'ens_aifs': [ens_aifs_23h_2t, ens_aifs_23h_2d, ens_aifs_23h_100u, ens_aifs_23h_100v, ens_aifs_23h_tp, ens_aifs_23h_sp],
                'feature_names': ['temperature_2t', 'dewpoint_2d', 'u100_100u', 'v100_100v', 'precipitation_tp', 'sp']
            },
            '24h': {
                'best_match': [best_match_24h_2t, best_match_24h_2d, best_match_24h_100u, best_match_24h_100v, best_match_24h_tp, best_match_24h_sp],
                'ecmwf_ifs': [ecmwf_ifs_24h_2t, ecmwf_ifs_24h_2d, ecmwf_ifs_24h_100u, ecmwf_ifs_24h_100v, ecmwf_ifs_24h_tp, ecmwf_ifs_24h_sp],
                'gfs_global': [gfs_global_24h_2t, gfs_global_24h_2d, gfs_global_24h_100u, gfs_global_24h_100v, gfs_global_24h_tp, gfs_global_24h_sp],
                'AIFS': [AIFS_24h_2t, AIFS_24h_2d, AIFS_24h_100u, AIFS_24h_100v, AIFS_24h_tp, AIFS_24h_sp],
                'CMA': [CMA_24h_2t, CMA_24h_2d, CMA_24h_100u, CMA_24h_100v, CMA_24h_tp, CMA_24h_sp],
                'era5': [era5_24h_2t, era5_24h_2d, era5_24h_100u, era5_24h_100v, era5_24h_tp, era5_24h_sp],
                'ens_aifs': [ens_aifs_24h_2t, ens_aifs_24h_2d, ens_aifs_24h_100u, ens_aifs_24h_100v, ens_aifs_24h_tp, ens_aifs_24h_sp],
                'feature_names': ['temperature_2t', 'dewpoint_2d', 'u100_100u', 'v100_100v', 'precipitation_tp', 'sp']
            }
        }

        
        # Use diverging colormap for better contrast
        cmap = 'RdBu_r'  # or 'coolwarm', 'seismic', 'bwr'
        
        # Process each hour separately
        try:
            for hour in ['1h', '2h', '3h', '4h', '5h', '6h', '7h', '8h', '9h', '10h', '11h', '12h', '13h', '14h', '15h', '16h', '17h', '18h', '19h', '20h', '21h', '22h', '23h', '24h']:
                hour_data = hours_data[hour]
                
                # Check if PNG already exists for this hour
                output_path = os.path.join(rar_output_dir, f'{rar_basename}_{hour}.png')
                if os.path.exists(output_path):
                    print(f"PNG file already exists: {output_path}, skipping...")
                    continue
                
                # Create figure with 6 rows (features) and 5 columns (models)
                fig, axes = plt.subplots(6, 6, figsize=(18, 36))
                
                # Process each feature for this hour
                for row_idx in range(6):
                    best_match_data = hour_data['best_match'][row_idx]
                    ecmwf_ifs_data = hour_data['ecmwf_ifs'][row_idx]
                    gfs_global_data = hour_data['gfs_global'][row_idx]
                    AIFS_data = hour_data['AIFS'][row_idx]
                    CMA_data = hour_data['CMA'][row_idx]
                    era5_data = hour_data['era5'][row_idx]
                    ens_aifs_data = hour_data['ens_aifs'][row_idx]
                    feature_name = hour_data['feature_names'][row_idx]
                    
                    # Calculate RMSE for each model
                    rmse_best_match = np.flip(np.abs(best_match_data - era5_data), axis=0)
                    rmse_ecmwf_ifs = np.flip(np.abs(ecmwf_ifs_data - era5_data), axis=0)
                    rmse_gfs_global = np.flip(np.abs(gfs_global_data - era5_data), axis=0)
                    rmse_AIFS = np.flip(np.abs(AIFS_data - era5_data), axis=0)
                    rmse_CMA = np.flip(np.abs(CMA_data - era5_data), axis=0)
                    rmse_ens_aifs = np.flip(np.abs(ens_aifs_data - era5_data), axis=0)
                    
                    global_rmse_ecmwf_ifs = np.sqrt(np.mean(rmse_ecmwf_ifs ** 2))
                    global_rmse_best_match = np.sqrt(np.mean(rmse_best_match ** 2)) - global_rmse_ecmwf_ifs
                    global_rmse_gfs_global = np.sqrt(np.mean(rmse_gfs_global ** 2)) - global_rmse_ecmwf_ifs
                    global_rmse_AIFS = np.sqrt(np.mean(rmse_AIFS ** 2)) - global_rmse_ecmwf_ifs
                    global_rmse_CMA = np.sqrt(np.mean(rmse_CMA ** 2)) - global_rmse_ecmwf_ifs
                    global_rmse_ens_aifs = np.sqrt(np.mean(rmse_ens_aifs ** 2)) - global_rmse_ecmwf_ifs
                    
                    # Calculate differences relative to ECMWF
                    data1 = rmse_best_match - rmse_ecmwf_ifs
                    data3 = rmse_gfs_global - rmse_ecmwf_ifs
                    data4 = rmse_AIFS - rmse_ecmwf_ifs
                    data5 = rmse_CMA - rmse_ecmwf_ifs
                    data6 = rmse_ens_aifs - rmse_ecmwf_ifs
                    
                    # Find global min/max for symmetric color scaling
                    vmax = max(np.abs(data1).max(), np.abs(data3).max(), np.abs(data4).max(), np.abs(data5).max(), np.abs(data6).max())
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
                    axes[row_idx, 3].set_title(f'{feature_name} ({hour})\nAIFS - ECMWF\n(relative to ERA5)')
                    axes[row_idx, 3].axis('off')
                    axes[row_idx, 3].text(0.5, -0.08, f'Global RMSE: {global_rmse_AIFS:.4f}', 
                                          transform=axes[row_idx, 3].transAxes, ha='center', va='top', fontsize=9)
                    
                    im4 = axes[row_idx, 4].imshow(data5, cmap=cmap, vmin=vmin, vmax=vmax)
                    axes[row_idx, 4].set_title(f'{feature_name} ({hour})\nCMA - ECMWF\n(relative to ERA5)')
                    axes[row_idx, 4].axis('off')
                    axes[row_idx, 4].text(0.5, -0.08, f'Global RMSE: {global_rmse_CMA:.4f}', 
                                          transform=axes[row_idx, 4].transAxes, ha='center', va='top', fontsize=9)
                    im5 = axes[row_idx, 5].imshow(data6, cmap=cmap, vmin=vmin, vmax=vmax)
                    axes[row_idx, 5].set_title(f'{feature_name} ({hour})\nENS AIFS - ECMWF\n(relative to ERA5)')
                    axes[row_idx, 5].axis('off')
                    axes[row_idx, 5].text(0.5, -0.08, f'Global RMSE: {global_rmse_ens_aifs:.4f}', 
                                          transform=axes[row_idx, 5].transAxes, ha='center', va='top', fontsize=9)

                    # Add colorbars
                    fig.colorbar(im0, ax=axes[row_idx, 0], fraction=0.046, pad=0.04)
                    fig.colorbar(im1, ax=axes[row_idx, 1], fraction=0.046, pad=0.04)
                    fig.colorbar(im2, ax=axes[row_idx, 2], fraction=0.046, pad=0.04)
                    fig.colorbar(im3, ax=axes[row_idx, 3], fraction=0.046, pad=0.04)
                    fig.colorbar(im4, ax=axes[row_idx, 4], fraction=0.046, pad=0.04)
                    fig.colorbar(im5, ax=axes[row_idx, 5], fraction=0.046, pad=0.04)
                
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

