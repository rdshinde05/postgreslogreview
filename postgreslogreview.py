
import re
import csv
import glob
import os

def parse_postgres_logs(log_file_paths, output_csv_path):
    # Define regex patterns for the log and the errors you're searching for
    log_pattern = re.compile(r"(\d+) \[([a-f0-9-]+)\] - (.*)")
    hostname_pattern = re.compile(r"hostname\s*:\s*(\S+)")
    
    # Pattern for the first error
    error_assign_profile_pattern = re.compile(r"ERROR:\s*function\s*public\.assign_profile\(unknown,\s*unknown\)\s*does\s*not\s*exist")
    
    # Pattern for the second error
    error_permission_denied_pattern = re.compile(r"ERROR:\s*permission\s*denied\s*to\s*create\s*role")
    
    hostname = None
    error_logs = []
    
    # Open the CSV file for writing (removed newline='' for Python <3.6)
    with open(output_csv_path, 'w') as csvfile:
        csv_writer = csv.writer(csvfile)
        # Write the header
        csv_writer.writerow(['Filename', 'Timestamp', 'UUID', 'Log Message', 'Hostname'])
        
        for log_file_path in log_file_paths:
            with open(log_file_path, 'r') as log_file:
                for line in log_file:
                    match = log_pattern.match(line.strip())
                    
                    if match:
                        timestamp = match.group(1)
                        uuid = match.group(2)
                        log_message = match.group(3)
                        
                        # Capture the hostname
                        hostname_match = hostname_pattern.search(log_message)
                        if hostname_match:
                            hostname = hostname_match.group(1)
                        
                        # Debug output to check each log line
                        print("Log message:", log_message)

                        # Check for both error patterns
                        if error_assign_profile_pattern.search(log_message):
                            print("Matched error: assign_profile")
                        elif error_permission_denied_pattern.search(log_message):
                            print("Matched error: permission denied to create role")

                        # Capture both types of error logs
                        if error_assign_profile_pattern.search(log_message) or error_permission_denied_pattern.search(log_message):
                            error_logs.append([os.path.basename(log_file_path), timestamp, uuid, log_message, hostname])
                            # Write to CSV
                            csv_writer.writerow([os.path.basename(log_file_path), timestamp, uuid, log_message, hostname])
    
    if error_logs:
        print("Errors found and logged in CSV with hostname: {}".format(hostname))
    else:
        print("No errors found.")

if __name__ == "__main__":
    # Directory where log files are stored (can be changed to your log directory)
    log_directory = '/pathtolog/postgres/'  # Change this to the directory where your logs are stored
    # Pattern for log files (change as needed, e.g., '*.log' to match log files)
    log_file_pattern = '*.log'
    
    # Find all log files in the directory matching the pattern
    log_file_paths = glob.glob(os.path.join(log_directory, log_file_pattern))
    
    if not log_file_paths:
        print("No log files found matching the pattern.")
    else:
        # Output CSV file path
        output_csv_path = '/tmp/postgres_parsed_logs.csv'  # Output CSV file path
        # Parse logs and save to CSV
        parse_postgres_logs(log_file_paths, output_csv_path)
