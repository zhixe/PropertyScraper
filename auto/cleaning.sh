#!/bin/bash
set -e

# Directory where the .csv files are located
CSV="/root/projects/PropertyScraper/data/raw"
LOG="/root/projects/PropertyScraper/logs"

# Find and delete .csv files older than 2 days
find "$CSV" -name '*.csv' -mtime +1 -exec rm {} \;
find "$LOG" -name '*.log' -mtime +1 -exec rm {} \;
trap 'echo "An error occurred. Exiting the program.."; exit 1' ERR
