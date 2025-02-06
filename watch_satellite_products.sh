#!/bin/bash

# conda activate NBSsync


while true; do
    start_date=$(date -d "6 days ago" +"%Y%m%d")
    end_date=$(date +"%Y%m%d")

    echo "$(date) - Running sync_and_store.py with start_date=$start_date and end_date=$end_date"
    python3 sync_and_store.py --start_date="$start_date" --end_date="$end_date" --sat="S1"

    echo "$(date) - Waiting for 10 minutes before the next run..."
    sleep 600  # Wait 10 minutes
done
# need to test S1, see if memmory issues are solved running on computing nodes