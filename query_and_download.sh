#!/bin/bash

conda activate base

# Define Python script
python_script="./query_and_download.py"

# Calculate dynamic dates. Data will be processed for all dates inbetween and inclusive of the start and end date.
end_date="20210701"
start_date="20210701"
sat="S1"

# Execute the Python script with arguments
$python_script --start_date="$start_date" --end_date="$end_date" --sat="$sat"
