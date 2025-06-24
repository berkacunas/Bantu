import os
import sqlite3

def get_connection(sqlite_path: str): 
    
    try:
        dirname = os.path.dirname(sqlite_path)
        
        if not os.path.exists(dirname):
            create_dir_if_not_exists(dirname)
            
        if os.path.exists(dirname):
            conn = sqlite3.connect(sqlite_path)
            return conn
    except:
        raise
    

def create_dir_if_not_exists(dirname: str) -> str:
    
    if not os.path.exists(dirname):
        try:
            os.makedirs(dirname)
            print(f"Nested directories '{dirname}' created successfully.")
            return dirname
        except FileExistsError:
            print(f"One or more directories in '{dirname}' already exist.")
        except PermissionError:
            print(f"Permission denied: Unable to create '{dirname}'.")
        except Exception as e:
            print(f"An error occurred: {e}")
