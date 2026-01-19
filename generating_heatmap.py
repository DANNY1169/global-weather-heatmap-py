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
        
        best_match, ecmwf_ifs, gfs_global, graphcast, aifs = np.split(data_3, 5, axis = -1)
        best_match = best_match.squeeze(axis = -1)
        ecmwf_ifs = ecmwf_ifs.squeeze(axis = -1)
        gfs_global = gfs_global.squeeze(axis = -1)
        graphcast = graphcast.squeeze(axis = -1)
        aifs = aifs.squeeze(axis = -1)

        best_match_1d = best_match[:,:,0:4,:]
        best_match_2d = best_match[:,:,4:8,:]
        best_match_3d = best_match[:,:,8:12,:]
        best_match_4d = best_match[:,:,12:16,:]
        best_match_5d = best_match[:,:,16:20,:]

        ecmwf_ifs_1d = ecmwf_ifs[:,:,0:4,:]
        ecmwf_ifs_2d = ecmwf_ifs[:,:,4:8,:]
        ecmwf_ifs_3d = ecmwf_ifs[:,:,8:12,:]
        ecmwf_ifs_4d = ecmwf_ifs[:,:,12:16,:]
        ecmwf_ifs_5d = ecmwf_ifs[:,:,16:20,:]

        gfs_global_1d = gfs_global[:,:,0:4,:]
        gfs_global_2d = gfs_global[:,:,4:8,:]
        gfs_global_3d = gfs_global[:,:,8:12,:]
        gfs_global_4d = gfs_global[:,:,12:16,:]
        gfs_global_5d = gfs_global[:,:,16:20,:]
        
        graphcast_1d = graphcast[:,:,0:4,:]
        graphcast_2d = graphcast[:,:,4:8,:]
        graphcast_3d = graphcast[:,:,8:12,:]
        graphcast_4d = graphcast[:,:,12:16,:]
        graphcast_5d = graphcast[:,:,16:20,:]

        aifs_1d = aifs[:,:,0:4,:]
        aifs_2d = aifs[:,:,4:8,:]
        aifs_3d = aifs[:,:,8:12,:]
        aifs_4d = aifs[:,:,12:16,:]
        aifs_5d = aifs[:,:,16:20,:]

        # best_match = best_match.reshape(720, 1440, 5, 4, 6)
        # ecmwf_ifs = ecmwf_ifs.reshape(720, 1440, 5, 4, 6)
        # gfs_global = gfs_global.reshape(720, 1440, 5, 4, 6)
        # graphcast = graphcast.reshape(720, 1440, 5, 4, 6)
        # aifs = aifs.reshape(720, 1440, 5, 4, 6)

        # best_match_1d, best_match_2d, best_match_3d, best_match_4d, best_match_5d = np.split(best_match, 5, axis = 2) 
        # ecmwf_ifs_1d, ecmwf_ifs_2d, ecmwf_ifs_3d, ecmwf_ifs_4d, ecmwf_ifs_5d = np.split(ecmwf_ifs, 5, axis = 2)
        # gfs_global_1d, gfs_global_2d, gfs_global_3d, gfs_global_4d, gfs_global_5d = np.split(gfs_global, 5, axis = 2)
        # graphcast_1d, graphcast_2d, graphcast_3d, graphcast_4d, graphcast_5d = np.split(graphcast, 5, axis = 2)
        # aifs_1d, aifs_2d, aifs_3d, aifs_4d, aifs_5d = np.split(aifs, 5, axis = 2)

        # best_match_1d = best_match_1d.squeeze(axis = 2)
        # best_match_2d = best_match_2d.squeeze(axis = 2)
        # best_match_3d = best_match_3d.squeeze(axis = 2)
        # best_match_4d = best_match_4d.squeeze(axis = 2)
        # best_match_5d = best_match_5d.squeeze(axis = 2)
        # ecmwf_ifs_1d = ecmwf_ifs_1d.squeeze(axis = 2)
        # ecmwf_ifs_2d = ecmwf_ifs_2d.squeeze(axis = 2)
        # ecmwf_ifs_3d = ecmwf_ifs_3d.squeeze(axis = 2)
        # ecmwf_ifs_4d = ecmwf_ifs_4d.squeeze(axis = 2)
        # ecmwf_ifs_5d = ecmwf_ifs_5d.squeeze(axis = 2)
        # gfs_global_1d = gfs_global_1d.squeeze(axis = 2)
        # gfs_global_2d = gfs_global_2d.squeeze(axis = 2)
        # gfs_global_3d = gfs_global_3d.squeeze(axis = 2)
        # gfs_global_4d = gfs_global_4d.squeeze(axis = 2)
        # gfs_global_5d = gfs_global_5d.squeeze(axis = 2)
        # graphcast_1d = graphcast_1d.squeeze(axis = 2)
        # graphcast_2d = graphcast_2d.squeeze(axis = 2)
        # graphcast_3d = graphcast_3d.squeeze(axis = 2)
        # graphcast_4d = graphcast_4d.squeeze(axis = 2)
        # graphcast_5d = graphcast_5d.squeeze(axis = 2)
        # aifs_1d = aifs_1d.squeeze(axis = 2)
        # aifs_2d = aifs_2d.squeeze(axis = 2)
        # aifs_3d = aifs_3d.squeeze(axis = 2)
        # aifs_4d = aifs_4d.squeeze(axis = 2)
        # aifs_5d = aifs_5d.squeeze(axis = 2)

        try:
            with open(y_path, 'rb') as f:
                era5 = pickle.load(f)
            era5 = np.array(era5)
        except Exception as e:
            print(f"Error loading {y_path}: {e}")
            cleanup_extracted_files()
            return False
        

        era5_1d = era5[:,:,0:4,:]
        era5_2d = era5[:,:,4:8,:]
        era5_3d = era5[:,:,8:12,:]
        era5_4d = era5[:,:,12:16,:]
        era5_5d = era5[:,:,16:20,:]

        # era5 = era5.reshape(720, 1440, 5, 4, 6)
        # era5_1d, era5_2d, era5_3d, era5_4d, era5_5d = np.split(era5, 5, axis = 2)
        # era5_1d = era5_1d.squeeze(axis = 2)
        # era5_2d = era5_2d.squeeze(axis = 2)
        # era5_3d = era5_3d.squeeze(axis = 2)
        # era5_4d = era5_4d.squeeze(axis = 2)
        # era5_5d = era5_5d.squeeze(axis = 2)

        best_match_1d_2t, best_match_1d_2d, best_match_1d_100u, best_match_1d_100v, best_match_1d_tp, best_match_1d_sp = np.split(best_match_1d, 6, axis = -1)
        best_match_2d_2t, best_match_2d_2d, best_match_2d_100u, best_match_2d_100v, best_match_2d_tp, best_match_2d_sp = np.split(best_match_2d, 6, axis = -1)
        best_match_3d_2t, best_match_3d_2d, best_match_3d_100u, best_match_3d_100v, best_match_3d_tp, best_match_3d_sp = np.split(best_match_3d, 6, axis = -1)
        best_match_4d_2t, best_match_4d_2d, best_match_4d_100u, best_match_4d_100v, best_match_4d_tp, best_match_4d_sp = np.split(best_match_4d, 6, axis = -1)
        best_match_5d_2t, best_match_5d_2d, best_match_5d_100u, best_match_5d_100v, best_match_5d_tp, best_match_5d_sp = np.split(best_match_5d, 6, axis = -1)

        best_match_1d_2t = best_match_1d_2t.squeeze(axis = -1)
        best_match_1d_2d = best_match_1d_2d.squeeze(axis = -1)
        best_match_1d_100u = best_match_1d_100u.squeeze(axis = -1)/3.6
        best_match_1d_100v = best_match_1d_100v.squeeze(axis = -1)/3.6
        best_match_1d_tp = best_match_1d_tp.squeeze(axis = -1)
        best_match_1d_sp = best_match_1d_sp.squeeze(axis = -1)

        best_match_2d_2t = best_match_2d_2t.squeeze(axis = -1)
        best_match_2d_2d = best_match_2d_2d.squeeze(axis = -1)
        best_match_2d_100u = best_match_2d_100u.squeeze(axis = -1)/3.6
        best_match_2d_100v = best_match_2d_100v.squeeze(axis = -1)/3.6
        best_match_2d_tp = best_match_2d_tp.squeeze(axis = -1)
        best_match_2d_sp = best_match_2d_sp.squeeze(axis = -1)

        best_match_3d_2t = best_match_3d_2t.squeeze(axis = -1)
        best_match_3d_2d = best_match_3d_2d.squeeze(axis = -1)
        best_match_3d_100u = best_match_3d_100u.squeeze(axis = -1)/3.6
        best_match_3d_100v = best_match_3d_100v.squeeze(axis = -1)/3.6
        best_match_3d_tp = best_match_3d_tp.squeeze(axis = -1)
        best_match_3d_sp = best_match_3d_sp.squeeze(axis = -1)

        best_match_4d_2t = best_match_4d_2t.squeeze(axis = -1)
        best_match_4d_2d = best_match_4d_2d.squeeze(axis = -1)
        best_match_4d_100u = best_match_4d_100u.squeeze(axis = -1)/3.6
        best_match_4d_100v = best_match_4d_100v.squeeze(axis = -1)/3.6
        best_match_4d_tp = best_match_4d_tp.squeeze(axis = -1)
        best_match_4d_sp = best_match_4d_sp.squeeze(axis = -1)

        best_match_5d_2t = best_match_5d_2t.squeeze(axis = -1)
        best_match_5d_2d = best_match_5d_2d.squeeze(axis = -1)
        best_match_5d_100u = best_match_5d_100u.squeeze(axis = -1)/3.6
        best_match_5d_100v = best_match_5d_100v.squeeze(axis = -1)/3.6
        best_match_5d_tp = best_match_5d_tp.squeeze(axis = -1)
        best_match_5d_sp = best_match_5d_sp.squeeze(axis = -1)

        ecmwf_ifs_1d_2t, ecmwf_ifs_1d_2d, ecmwf_ifs_1d_100u, ecmwf_ifs_1d_100v, ecmwf_ifs_1d_tp, ecmwf_ifs_1d_sp = np.split(ecmwf_ifs_1d, 6, axis = -1)
        ecmwf_ifs_2d_2t, ecmwf_ifs_2d_2d, ecmwf_ifs_2d_100u, ecmwf_ifs_2d_100v, ecmwf_ifs_2d_tp, ecmwf_ifs_2d_sp = np.split(ecmwf_ifs_2d, 6, axis = -1)
        ecmwf_ifs_3d_2t, ecmwf_ifs_3d_2d, ecmwf_ifs_3d_100u, ecmwf_ifs_3d_100v, ecmwf_ifs_3d_tp, ecmwf_ifs_3d_sp = np.split(ecmwf_ifs_3d, 6, axis = -1)
        ecmwf_ifs_4d_2t, ecmwf_ifs_4d_2d, ecmwf_ifs_4d_100u, ecmwf_ifs_4d_100v, ecmwf_ifs_4d_tp, ecmwf_ifs_4d_sp = np.split(ecmwf_ifs_4d, 6, axis = -1)
        ecmwf_ifs_5d_2t, ecmwf_ifs_5d_2d, ecmwf_ifs_5d_100u, ecmwf_ifs_5d_100v, ecmwf_ifs_5d_tp, ecmwf_ifs_5d_sp = np.split(ecmwf_ifs_5d, 6, axis = -1)

        ecmwf_ifs_1d_2t = ecmwf_ifs_1d_2t.squeeze(axis = -1)
        ecmwf_ifs_1d_2d = ecmwf_ifs_1d_2d.squeeze(axis = -1)
        ecmwf_ifs_1d_100u = ecmwf_ifs_1d_100u.squeeze(axis = -1)/3.6
        ecmwf_ifs_1d_100v = ecmwf_ifs_1d_100v.squeeze(axis = -1)/3.6
        ecmwf_ifs_1d_tp = ecmwf_ifs_1d_tp.squeeze(axis = -1)
        ecmwf_ifs_1d_sp = ecmwf_ifs_1d_sp.squeeze(axis = -1)

        ecmwf_ifs_2d_2t = ecmwf_ifs_2d_2t.squeeze(axis = -1)
        ecmwf_ifs_2d_2d = ecmwf_ifs_2d_2d.squeeze(axis = -1)
        ecmwf_ifs_2d_100u = ecmwf_ifs_2d_100u.squeeze(axis = -1)/3.6
        ecmwf_ifs_2d_100v = ecmwf_ifs_2d_100v.squeeze(axis = -1)/3.6
        ecmwf_ifs_2d_tp = ecmwf_ifs_2d_tp.squeeze(axis = -1)
        ecmwf_ifs_2d_sp = ecmwf_ifs_2d_sp.squeeze(axis = -1)

        ecmwf_ifs_3d_2t = ecmwf_ifs_3d_2t.squeeze(axis = -1)
        ecmwf_ifs_3d_2d = ecmwf_ifs_3d_2d.squeeze(axis = -1)
        ecmwf_ifs_3d_100u = ecmwf_ifs_3d_100u.squeeze(axis = -1)/3.6
        ecmwf_ifs_3d_100v = ecmwf_ifs_3d_100v.squeeze(axis = -1)/3.6
        ecmwf_ifs_3d_tp = ecmwf_ifs_3d_tp.squeeze(axis = -1)
        ecmwf_ifs_3d_sp = ecmwf_ifs_3d_sp.squeeze(axis = -1)

        ecmwf_ifs_4d_2t = ecmwf_ifs_4d_2t.squeeze(axis = -1)
        ecmwf_ifs_4d_2d = ecmwf_ifs_4d_2d.squeeze(axis = -1)
        ecmwf_ifs_4d_100u = ecmwf_ifs_4d_100u.squeeze(axis = -1)/3.6
        ecmwf_ifs_4d_100v = ecmwf_ifs_4d_100v.squeeze(axis = -1)/3.6
        ecmwf_ifs_4d_tp = ecmwf_ifs_4d_tp.squeeze(axis = -1)
        ecmwf_ifs_4d_sp = ecmwf_ifs_4d_sp.squeeze(axis = -1)

        ecmwf_ifs_5d_2t = ecmwf_ifs_5d_2t.squeeze(axis = -1)
        ecmwf_ifs_5d_2d = ecmwf_ifs_5d_2d.squeeze(axis = -1)
        ecmwf_ifs_5d_100u = ecmwf_ifs_5d_100u.squeeze(axis = -1)/3.6
        ecmwf_ifs_5d_100v = ecmwf_ifs_5d_100v.squeeze(axis = -1)/3.6
        ecmwf_ifs_5d_tp = ecmwf_ifs_5d_tp.squeeze(axis = -1)
        ecmwf_ifs_5d_sp = ecmwf_ifs_5d_sp.squeeze(axis = -1)

        gfs_global_1d_2t, gfs_global_1d_2d, gfs_global_1d_100u, gfs_global_1d_100v, gfs_global_1d_tp, gfs_global_1d_sp = np.split(gfs_global_1d, 6, axis = -1)
        gfs_global_2d_2t, gfs_global_2d_2d, gfs_global_2d_100u, gfs_global_2d_100v, gfs_global_2d_tp, gfs_global_2d_sp = np.split(gfs_global_2d, 6, axis = -1)
        gfs_global_3d_2t, gfs_global_3d_2d, gfs_global_3d_100u, gfs_global_3d_100v, gfs_global_3d_tp, gfs_global_3d_sp = np.split(gfs_global_3d, 6, axis = -1)
        gfs_global_4d_2t, gfs_global_4d_2d, gfs_global_4d_100u, gfs_global_4d_100v, gfs_global_4d_tp, gfs_global_4d_sp = np.split(gfs_global_4d, 6, axis = -1)
        gfs_global_5d_2t, gfs_global_5d_2d, gfs_global_5d_100u, gfs_global_5d_100v, gfs_global_5d_tp, gfs_global_5d_sp = np.split(gfs_global_5d, 6, axis = -1)

        gfs_global_1d_2t = gfs_global_1d_2t.squeeze(axis = -1)
        gfs_global_1d_2d = gfs_global_1d_2d.squeeze(axis = -1)
        gfs_global_1d_100u = gfs_global_1d_100u.squeeze(axis = -1)/3.6
        gfs_global_1d_100v = gfs_global_1d_100v.squeeze(axis = -1)/3.6
        gfs_global_1d_tp = gfs_global_1d_tp.squeeze(axis = -1)
        gfs_global_1d_sp = gfs_global_1d_sp.squeeze(axis = -1)

        gfs_global_2d_2t = gfs_global_2d_2t.squeeze(axis = -1)
        gfs_global_2d_2d = gfs_global_2d_2d.squeeze(axis = -1)
        gfs_global_2d_100u = gfs_global_2d_100u.squeeze(axis = -1)/3.6
        gfs_global_2d_100v = gfs_global_2d_100v.squeeze(axis = -1)/3.6
        gfs_global_2d_tp = gfs_global_2d_tp.squeeze(axis = -1)
        gfs_global_2d_sp = gfs_global_2d_sp.squeeze(axis = -1)

        gfs_global_3d_2t = gfs_global_3d_2t.squeeze(axis = -1)
        gfs_global_3d_2d = gfs_global_3d_2d.squeeze(axis = -1)
        gfs_global_3d_100u = gfs_global_3d_100u.squeeze(axis = -1)/3.6
        gfs_global_3d_100v = gfs_global_3d_100v.squeeze(axis = -1)/3.6
        gfs_global_3d_tp = gfs_global_3d_tp.squeeze(axis = -1)
        gfs_global_3d_sp = gfs_global_3d_sp.squeeze(axis = -1)

        gfs_global_4d_2t = gfs_global_4d_2t.squeeze(axis = -1)
        gfs_global_4d_2d = gfs_global_4d_2d.squeeze(axis = -1)
        gfs_global_4d_100u = gfs_global_4d_100u.squeeze(axis = -1)/3.6
        gfs_global_4d_100v = gfs_global_4d_100v.squeeze(axis = -1)/3.6
        gfs_global_4d_tp = gfs_global_4d_tp.squeeze(axis = -1)
        gfs_global_4d_sp = gfs_global_4d_sp.squeeze(axis = -1)

        gfs_global_5d_2t = gfs_global_5d_2t.squeeze(axis = -1)
        gfs_global_5d_2d = gfs_global_5d_2d.squeeze(axis = -1)
        gfs_global_5d_100u = gfs_global_5d_100u.squeeze(axis = -1)/3.6
        gfs_global_5d_100v = gfs_global_5d_100v.squeeze(axis = -1)/3.6
        gfs_global_5d_tp = gfs_global_5d_tp.squeeze(axis = -1)
        gfs_global_5d_sp = gfs_global_5d_sp.squeeze(axis = -1)

        graphcast_1d_2t, graphcast_1d_2d, graphcast_1d_100u, graphcast_1d_100v, graphcast_1d_tp, graphcast_1d_sp = np.split(graphcast_1d, 6, axis = -1)
        graphcast_2d_2t, graphcast_2d_2d, graphcast_2d_100u, graphcast_2d_100v, graphcast_2d_tp, graphcast_2d_sp = np.split(graphcast_2d, 6, axis = -1)
        graphcast_3d_2t, graphcast_3d_2d, graphcast_3d_100u, graphcast_3d_100v, graphcast_3d_tp, graphcast_3d_sp = np.split(graphcast_3d, 6, axis = -1)
        graphcast_4d_2t, graphcast_4d_2d, graphcast_4d_100u, graphcast_4d_100v, graphcast_4d_tp, graphcast_4d_sp = np.split(graphcast_4d, 6, axis = -1)
        graphcast_5d_2t, graphcast_5d_2d, graphcast_5d_100u, graphcast_5d_100v, graphcast_5d_tp, graphcast_5d_sp = np.split(graphcast_5d, 6, axis = -1)

        graphcast_1d_2t = graphcast_1d_2t.squeeze(axis = -1)
        graphcast_1d_2d = graphcast_1d_2d.squeeze(axis = -1)
        graphcast_1d_100u = graphcast_1d_100u.squeeze(axis = -1)/3.6
        graphcast_1d_100v = graphcast_1d_100v.squeeze(axis = -1)/3.6
        graphcast_1d_tp = graphcast_1d_tp.squeeze(axis = -1)
        graphcast_1d_sp = graphcast_1d_sp.squeeze(axis = -1)

        graphcast_2d_2t = graphcast_2d_2t.squeeze(axis = -1)
        graphcast_2d_2d = graphcast_2d_2d.squeeze(axis = -1)
        graphcast_2d_100u = graphcast_2d_100u.squeeze(axis = -1)/3.6
        graphcast_2d_100v = graphcast_2d_100v.squeeze(axis = -1)/3.6
        graphcast_2d_tp = graphcast_2d_tp.squeeze(axis = -1)
        graphcast_2d_sp = graphcast_2d_sp.squeeze(axis = -1)

        graphcast_3d_2t = graphcast_3d_2t.squeeze(axis = -1)
        graphcast_3d_2d = graphcast_3d_2d.squeeze(axis = -1)
        graphcast_3d_100u = graphcast_3d_100u.squeeze(axis = -1)/3.6
        graphcast_3d_100v = graphcast_3d_100v.squeeze(axis = -1)/3.6
        graphcast_3d_tp = graphcast_3d_tp.squeeze(axis = -1)
        graphcast_3d_sp = graphcast_3d_sp.squeeze(axis = -1)

        graphcast_4d_2t = graphcast_4d_2t.squeeze(axis = -1)
        graphcast_4d_2d = graphcast_4d_2d.squeeze(axis = -1)
        graphcast_4d_100u = graphcast_4d_100u.squeeze(axis = -1)/3.6
        graphcast_4d_100v = graphcast_4d_100v.squeeze(axis = -1)/3.6
        graphcast_4d_tp = graphcast_4d_tp.squeeze(axis = -1)
        graphcast_4d_sp = graphcast_4d_sp.squeeze(axis = -1)

        graphcast_5d_2t = graphcast_5d_2t.squeeze(axis = -1)
        graphcast_5d_2d = graphcast_5d_2d.squeeze(axis = -1)
        graphcast_5d_100u = graphcast_5d_100u.squeeze(axis = -1)/3.6
        graphcast_5d_100v = graphcast_5d_100v.squeeze(axis = -1)/3.6
        graphcast_5d_tp = graphcast_5d_tp.squeeze(axis = -1)
        graphcast_5d_sp = graphcast_5d_sp.squeeze(axis = -1)

        aifs_1d_2t, aifs_1d_2d, aifs_1d_100u, aifs_1d_100v, aifs_1d_tp, aifs_1d_sp = np.split(aifs_1d, 6, axis = -1)
        aifs_2d_2t, aifs_2d_2d, aifs_2d_100u, aifs_2d_100v, aifs_2d_tp, aifs_2d_sp = np.split(aifs_2d, 6, axis = -1)
        aifs_3d_2t, aifs_3d_2d, aifs_3d_100u, aifs_3d_100v, aifs_3d_tp, aifs_3d_sp = np.split(aifs_3d, 6, axis = -1)
        aifs_4d_2t, aifs_4d_2d, aifs_4d_100u, aifs_4d_100v, aifs_4d_tp, aifs_4d_sp = np.split(aifs_4d, 6, axis = -1)
        aifs_5d_2t, aifs_5d_2d, aifs_5d_100u, aifs_5d_100v, aifs_5d_tp, aifs_5d_sp = np.split(aifs_5d, 6, axis = -1)

        aifs_1d_2t = aifs_1d_2t.squeeze(axis = -1)
        aifs_1d_2d = aifs_1d_2d.squeeze(axis = -1)
        aifs_1d_100u = aifs_1d_100u.squeeze(axis = -1)/3.6
        aifs_1d_100v = aifs_1d_100v.squeeze(axis = -1)/3.6
        aifs_1d_tp = aifs_1d_tp.squeeze(axis = -1)
        aifs_1d_sp = aifs_1d_sp.squeeze(axis = -1)

        aifs_2d_2t = aifs_2d_2t.squeeze(axis = -1)
        aifs_2d_2d = aifs_2d_2d.squeeze(axis = -1)
        aifs_2d_100u = aifs_2d_100u.squeeze(axis = -1)/3.6
        aifs_2d_100v = aifs_2d_100v.squeeze(axis = -1)/3.6
        aifs_2d_tp = aifs_2d_tp.squeeze(axis = -1)
        aifs_2d_sp = aifs_2d_sp.squeeze(axis = -1)

        aifs_3d_2t = aifs_3d_2t.squeeze(axis = -1)
        aifs_3d_2d = aifs_3d_2d.squeeze(axis = -1)
        aifs_3d_100u = aifs_3d_100u.squeeze(axis = -1)/3.6
        aifs_3d_100v = aifs_3d_100v.squeeze(axis = -1)/3.6
        aifs_3d_tp = aifs_3d_tp.squeeze(axis = -1)
        aifs_3d_sp = aifs_3d_sp.squeeze(axis = -1)

        aifs_4d_2t = aifs_4d_2t.squeeze(axis = -1)
        aifs_4d_2d = aifs_4d_2d.squeeze(axis = -1)
        aifs_4d_100u = aifs_4d_100u.squeeze(axis = -1)/3.6
        aifs_4d_100v = aifs_4d_100v.squeeze(axis = -1)/3.6
        aifs_4d_tp = aifs_4d_tp.squeeze(axis = -1)
        aifs_4d_sp = aifs_4d_sp.squeeze(axis = -1)

        aifs_5d_2t = aifs_5d_2t.squeeze(axis = -1)
        aifs_5d_2d = aifs_5d_2d.squeeze(axis = -1)
        aifs_5d_100u = aifs_5d_100u.squeeze(axis = -1)/3.6
        aifs_5d_100v = aifs_5d_100v.squeeze(axis = -1)/3.6
        aifs_5d_tp = aifs_5d_tp.squeeze(axis = -1)
        aifs_5d_sp = aifs_5d_sp.squeeze(axis = -1)

        era5_1d_2t, era5_1d_2d, era5_1d_100u, era5_1d_100v, era5_1d_tp, era5_1d_sp = np.split(era5_1d, 6, axis = -1)
        era5_2d_2t, era5_2d_2d, era5_2d_100u, era5_2d_100v, era5_2d_tp, era5_2d_sp = np.split(era5_2d, 6, axis = -1)
        era5_3d_2t, era5_3d_2d, era5_3d_100u, era5_3d_100v, era5_3d_tp, era5_3d_sp = np.split(era5_3d, 6, axis = -1)
        era5_4d_2t, era5_4d_2d, era5_4d_100u, era5_4d_100v, era5_4d_tp, era5_4d_sp = np.split(era5_4d, 6, axis = -1)
        era5_5d_2t, era5_5d_2d, era5_5d_100u, era5_5d_100v, era5_5d_tp, era5_5d_sp = np.split(era5_5d, 6, axis = -1)

        era5_1d_2t = era5_1d_2t.squeeze(axis = -1)
        era5_1d_2d = era5_1d_2d.squeeze(axis = -1)
        era5_1d_100u = era5_1d_100u.squeeze(axis = -1)/3.6
        era5_1d_100v = era5_1d_100v.squeeze(axis = -1)/3.6
        era5_1d_tp = era5_1d_tp.squeeze(axis = -1)
        era5_1d_sp = era5_1d_sp.squeeze(axis = -1)

        era5_2d_2t = era5_2d_2t.squeeze(axis = -1)
        era5_2d_2d = era5_2d_2d.squeeze(axis = -1)
        era5_2d_100u = era5_2d_100u.squeeze(axis = -1)/3.6
        era5_2d_100v = era5_2d_100v.squeeze(axis = -1)/3.6
        era5_2d_tp = era5_2d_tp.squeeze(axis = -1)
        era5_2d_sp = era5_2d_sp.squeeze(axis = -1)

        era5_3d_2t = era5_3d_2t.squeeze(axis = -1)
        era5_3d_2d = era5_3d_2d.squeeze(axis = -1)
        era5_3d_100u = era5_3d_100u.squeeze(axis = -1)/3.6
        era5_3d_100v = era5_3d_100v.squeeze(axis = -1)/3.6
        era5_3d_tp = era5_3d_tp.squeeze(axis = -1)
        era5_3d_sp = era5_3d_sp.squeeze(axis = -1)

        era5_4d_2t = era5_4d_2t.squeeze(axis = -1)
        era5_4d_2d = era5_4d_2d.squeeze(axis = -1)
        era5_4d_100u = era5_4d_100u.squeeze(axis = -1)/3.6
        era5_4d_100v = era5_4d_100v.squeeze(axis = -1)/3.6
        era5_4d_tp = era5_4d_tp.squeeze(axis = -1)
        era5_4d_sp = era5_4d_sp.squeeze(axis = -1)

        era5_5d_2t = era5_5d_2t.squeeze(axis = -1)
        era5_5d_2d = era5_5d_2d.squeeze(axis = -1)
        era5_5d_100u = era5_5d_100u.squeeze(axis = -1)/3.6
        era5_5d_100v = era5_5d_100v.squeeze(axis = -1)/3.6
        era5_5d_tp = era5_5d_tp.squeeze(axis = -1)
        era5_5d_sp = era5_5d_sp.squeeze(axis = -1)

        # Group features by day and create one PNG per day
        # Days: 1d, 2d, 3d, 4d, 5d
        days_data = {
            '1d': {
                'best_match': [best_match_1d_2t, best_match_1d_2d, best_match_1d_100u, best_match_1d_100v, best_match_1d_tp, best_match_1d_sp],
                'ecmwf_ifs': [ecmwf_ifs_1d_2t, ecmwf_ifs_1d_2d, ecmwf_ifs_1d_100u, ecmwf_ifs_1d_100v, ecmwf_ifs_1d_tp, ecmwf_ifs_1d_sp],
                'gfs_global': [gfs_global_1d_2t, gfs_global_1d_2d, gfs_global_1d_100u, gfs_global_1d_100v, gfs_global_1d_tp, gfs_global_1d_sp],
                'graphcast': [graphcast_1d_2t, graphcast_1d_2d, graphcast_1d_100u, graphcast_1d_100v, graphcast_1d_tp, graphcast_1d_sp],
                'aifs': [aifs_1d_2t, aifs_1d_2d, aifs_1d_100u, aifs_1d_100v, aifs_1d_tp, aifs_1d_sp],
                'era5': [era5_1d_2t, era5_1d_2d, era5_1d_100u, era5_1d_100v, era5_1d_tp, era5_1d_sp],
                'feature_names': ['temperature_2t', 'dewpoint_2d', 'u100_100u', 'v100_100v', 'precipitation_tp', 'sp']
            },
            '2d': {
                'best_match': [best_match_2d_2t, best_match_2d_2d, best_match_2d_100u, best_match_2d_100v, best_match_2d_tp, best_match_2d_sp],
                'ecmwf_ifs': [ecmwf_ifs_2d_2t, ecmwf_ifs_2d_2d, ecmwf_ifs_2d_100u, ecmwf_ifs_2d_100v, ecmwf_ifs_2d_tp, ecmwf_ifs_2d_sp],
                'gfs_global': [gfs_global_2d_2t, gfs_global_2d_2d, gfs_global_2d_100u, gfs_global_2d_100v, gfs_global_2d_tp, gfs_global_2d_sp],
                'graphcast': [graphcast_2d_2t, graphcast_2d_2d, graphcast_2d_100u, graphcast_2d_100v, graphcast_2d_tp, graphcast_2d_sp],
                'aifs': [aifs_2d_2t, aifs_2d_2d, aifs_2d_100u, aifs_2d_100v, aifs_2d_tp, aifs_2d_sp],
                'era5': [era5_2d_2t, era5_2d_2d, era5_2d_100u, era5_2d_100v, era5_2d_tp, era5_2d_sp],
                'feature_names': ['temperature_2t', 'dewpoint_2d', 'u100_100u', 'v100_100v', 'precipitation_tp', 'sp']
            },
            '3d': {
                'best_match': [best_match_3d_2t, best_match_3d_2d, best_match_3d_100u, best_match_3d_100v, best_match_3d_tp, best_match_3d_sp],
                'ecmwf_ifs': [ecmwf_ifs_3d_2t, ecmwf_ifs_3d_2d, ecmwf_ifs_3d_100u, ecmwf_ifs_3d_100v, ecmwf_ifs_3d_tp, ecmwf_ifs_3d_sp],
                'gfs_global': [gfs_global_3d_2t, gfs_global_3d_2d, gfs_global_3d_100u, gfs_global_3d_100v, gfs_global_3d_tp, gfs_global_3d_sp],
                'graphcast': [graphcast_3d_2t, graphcast_3d_2d, graphcast_3d_100u, graphcast_3d_100v, graphcast_3d_tp, graphcast_3d_sp],
                'aifs': [aifs_3d_2t, aifs_3d_2d, aifs_3d_100u, aifs_3d_100v, aifs_3d_tp, aifs_3d_sp],
                'era5': [era5_3d_2t, era5_3d_2d, era5_3d_100u, era5_3d_100v, era5_3d_tp, era5_3d_sp],
                'feature_names': ['temperature_2t', 'dewpoint_2d', 'u100_100u', 'v100_100v', 'precipitation_tp', 'sp']
            },
            '4d': {
                'best_match': [best_match_4d_2t, best_match_4d_2d, best_match_4d_100u, best_match_4d_100v, best_match_4d_tp, best_match_4d_sp],
                'ecmwf_ifs': [ecmwf_ifs_4d_2t, ecmwf_ifs_4d_2d, ecmwf_ifs_4d_100u, ecmwf_ifs_4d_100v, ecmwf_ifs_4d_tp, ecmwf_ifs_4d_sp],
                'gfs_global': [gfs_global_4d_2t, gfs_global_4d_2d, gfs_global_4d_100u, gfs_global_4d_100v, gfs_global_4d_tp, gfs_global_4d_sp],
                'graphcast': [graphcast_4d_2t, graphcast_4d_2d, graphcast_4d_100u, graphcast_4d_100v, graphcast_4d_tp, graphcast_4d_sp],
                'aifs': [aifs_4d_2t, aifs_4d_2d, aifs_4d_100u, aifs_4d_100v, aifs_4d_tp, aifs_4d_sp],
                'era5': [era5_4d_2t, era5_4d_2d, era5_4d_100u, era5_4d_100v, era5_4d_tp, era5_4d_sp],
                'feature_names': ['temperature_2t', 'dewpoint_2d', 'u100_100u', 'v100_100v', 'precipitation_tp', 'sp']
            },
            '5d': {
                'best_match': [best_match_5d_2t, best_match_5d_2d, best_match_5d_100u, best_match_5d_100v, best_match_5d_tp, best_match_5d_sp],
                'ecmwf_ifs': [ecmwf_ifs_5d_2t, ecmwf_ifs_5d_2d, ecmwf_ifs_5d_100u, ecmwf_ifs_5d_100v, ecmwf_ifs_5d_tp, ecmwf_ifs_5d_sp],
                'gfs_global': [gfs_global_5d_2t, gfs_global_5d_2d, gfs_global_5d_100u, gfs_global_5d_100v, gfs_global_5d_tp, gfs_global_5d_sp],
                'graphcast': [graphcast_5d_2t, graphcast_5d_2d, graphcast_5d_100u, graphcast_5d_100v, graphcast_5d_tp, graphcast_5d_sp],
                'aifs': [aifs_5d_2t, aifs_5d_2d, aifs_5d_100u, aifs_5d_100v, aifs_5d_tp, aifs_5d_sp],
                'era5': [era5_5d_2t, era5_5d_2d, era5_5d_100u, era5_5d_100v, era5_5d_tp, era5_5d_sp],
                'feature_names': ['temperature_2t', 'dewpoint_2d', 'u100_100u', 'v100_100v', 'precipitation_tp', 'sp']
            }
        }
        
        # Use diverging colormap for better contrast
        cmap = 'RdBu_r'  # or 'coolwarm', 'seismic', 'bwr'
        
        # Process each day separately
        try:
            for day in ['1d', '2d', '3d', '4d', '5d']:
                day_data = days_data[day]
                
                # Check if PNG already exists for this day
                output_path = os.path.join(rar_output_dir, f'{rar_basename}_{day}.png')
                if os.path.exists(output_path):
                    print(f"PNG file already exists: {output_path}, skipping...")
                    continue
                
                # Create figure with 6 rows (features) and 5 columns (models)
                fig, axes = plt.subplots(6, 5, figsize=(18, 30))
                
                # Process each feature for this day
                for row_idx in range(6):
                    best_match_data = day_data['best_match'][row_idx]
                    ecmwf_ifs_data = day_data['ecmwf_ifs'][row_idx]
                    gfs_global_data = day_data['gfs_global'][row_idx]
                    graphcast_data = day_data['graphcast'][row_idx]
                    aifs_data = day_data['aifs'][row_idx]
                    era5_data = day_data['era5'][row_idx]
                    feature_name = day_data['feature_names'][row_idx]
                    
                    # Calculate RMSE for each model
                    rmse_best_match = np.flip(np.sqrt(np.mean((best_match_data - era5_data) ** 2, axis=2)), axis=0)
                    rmse_ecmwf_ifs = np.flip(np.sqrt(np.mean((ecmwf_ifs_data - era5_data) ** 2, axis=2)), axis=0)
                    rmse_gfs_global = np.flip(np.sqrt(np.mean((gfs_global_data - era5_data) ** 2, axis=2)), axis=0)
                    rmse_graphcast = np.flip(np.sqrt(np.mean((graphcast_data - era5_data) ** 2, axis=2)), axis=0)
                    rmse_aifs = np.flip(np.sqrt(np.mean((aifs_data - era5_data) ** 2, axis=2)), axis=0)
                    
                    global_rmse_ecmwf_ifs = rmse_ecmwf_ifs.mean()
                    global_rmse_best_match = rmse_best_match.mean() - global_rmse_ecmwf_ifs
                    global_rmse_gfs_global = rmse_gfs_global.mean() - global_rmse_ecmwf_ifs
                    global_rmse_graphcast = rmse_graphcast.mean() - global_rmse_ecmwf_ifs
                    global_rmse_aifs = rmse_aifs.mean() - global_rmse_ecmwf_ifs

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
                    axes[row_idx, 0].set_title(f'{feature_name} ({day})\nBest Match - ECMWF\n(relative to ERA5)')
                    axes[row_idx, 0].axis('off')
                    axes[row_idx, 0].text(0.5, -0.08, f'Global RMSE: {global_rmse_best_match:.4f}', 
                                          transform=axes[row_idx, 0].transAxes, ha='center', va='top', fontsize=9)
                    
                    # Second plot shows absolute ECMWF RMSE
                    im1 = axes[row_idx, 1].imshow(rmse_ecmwf_ifs, cmap='viridis')
                    axes[row_idx, 1].set_title(f'{feature_name} ({day})\nECMWF RMSE\n(absolute values)')
                    axes[row_idx, 1].axis('off')
                    axes[row_idx, 1].text(0.5, -0.08, f'Global RMSE: {global_rmse_ecmwf_ifs:.4f}', 
                                          transform=axes[row_idx, 1].transAxes, ha='center', va='top', fontsize=9)
                    
                    im2 = axes[row_idx, 2].imshow(data3, cmap=cmap, vmin=vmin, vmax=vmax)
                    axes[row_idx, 2].set_title(f'{feature_name} ({day})\nGFS Global - ECMWF\n(relative to ERA5)')
                    axes[row_idx, 2].axis('off')
                    axes[row_idx, 2].text(0.5, -0.08, f'Global RMSE: {global_rmse_gfs_global:.4f}', 
                                          transform=axes[row_idx, 2].transAxes, ha='center', va='top', fontsize=9)
                    
                    im3 = axes[row_idx, 3].imshow(data4, cmap=cmap, vmin=vmin, vmax=vmax)
                    axes[row_idx, 3].set_title(f'{feature_name} ({day})\nGraphcast - ECMWF\n(relative to ERA5)')
                    axes[row_idx, 3].axis('off')
                    axes[row_idx, 3].text(0.5, -0.08, f'Global RMSE: {global_rmse_graphcast:.4f}', 
                                          transform=axes[row_idx, 3].transAxes, ha='center', va='top', fontsize=9)
                    
                    im4 = axes[row_idx, 4].imshow(data5, cmap=cmap, vmin=vmin, vmax=vmax)
                    axes[row_idx, 4].set_title(f'{feature_name} ({day})\nAIFS - ECMWF\n(relative to ERA5)')
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
                
                # Save the PNG file for this day
                try:
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

