#!/bin/bash
set -e

# Directory where the .csv files are located
CSV="/root/projects/PropertyScraper/data/raw"
LOG="/root/projects/PropertyScraper/logs"

# Function to find and delete files older than 1 day
delete_files() {
    find "$1" -name "$2" -mtime +0 -print -exec rm {} \;
}

# Delete .csv files and show the ones that have been removed
echo "Deleting .csv files older than 1 day:"
delete_files "$CSV" '*.csv' > ../logs/cleaning_csv.log
# > /dev/null

# Delete .log files and show the ones that have been removed
echo "Deleting .log files older than 1 day:"
delete_files "$LOG" '*.log' > ../logs/cleaning_log.log

trap 'echo "An error occurred. Exiting the program.."; exit 1' ERR
