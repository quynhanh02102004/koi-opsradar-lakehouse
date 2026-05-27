import subprocess
import os
import shutil
import sys

def execute_scraper(queries_path, output_filename):
    shared_queries = "/shared_data/queries.txt"
    shared_output = f"/shared_data/{output_filename}"

    shutil.copy(queries_path, shared_queries)
    
    cmd = [
        "docker", "run", "--rm",
        "-v", "opsradar_shared:/shared_data",
        "gosom/google-maps-scraper",
        "-input", "/shared_data/queries.txt",
        "-json",
        "-results", f"/shared_data/{output_filename}",
        "-extra-reviews",
        "-depth", "1",
        "-lang", "en",
        "-exit-on-inactivity", "2m"
    ]
    
    try:
        print("Run Docker Scraper")
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"error Scraper: {e}")
        sys.exit(1)


# def execute_scraper(queries_path, output_path):
#     is_running_in_container = os.path.exists('/.dockerenv')

#     if is_running_in_container:
#         shared_queries = "/shared_data/queries.txt"
#         output_filename = os.path.basename(output_path)
        
        
#         shutil.copy(queries_path, shared_queries)
        
#         cmd = [
#             "docker", "run", "--rm",
#             "-v", "opsradar_shared:/shared_data",
#             "gosom/google-maps-scraper",
#             "-input", "/shared_data/queries.txt",
#             "-json",
#             "-results", f"/shared_data/{output_filename}",
#             "-extra-reviews", "-depth", "1", "-lang", "en", "-exit-on-inactivity", "2m"
#         ]
#     else:
#        
        
#         abs_queries_path = os.path.abspath(queries_path)
#         abs_output_dir = os.path.abspath(os.path.dirname(output_path))
#         output_filename = os.path.basename(output_path)

#         os.makedirs(abs_output_dir, exist_ok=True)

#         cmd = [
#             "docker", "run", "--rm",
#             "-v", f"{abs_queries_path}:/queries.txt:ro",  
#             "-v", f"{abs_output_dir}:/out",               
#             "gosom/google-maps-scraper",
#             "-input", "/queries.txt",
#             "-json",
#             "-results", f"/out/{output_filename}",        
#             "-extra-reviews", "-depth", "1", "-lang", "en", "-exit-on-inactivity", "2m"
#         ]
    
#     try:
#         subprocess.run(cmd, check=True)
#         print("crawl raw data done")
#     except subprocess.CalledProcessError as e:
#         print(f"error scraper {e}")
#         sys.exit(1)