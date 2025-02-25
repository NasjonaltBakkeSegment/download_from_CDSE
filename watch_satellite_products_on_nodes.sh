#!/bin/bash -f                      
#$ -N test_sat_sync                     # Set the job name
#$ -l h_rt=02:00:00                 # Set a hard runtime limit (hh:mm:ss)
#$ -S /bin/bash                     
#$ -pe shmem-1 1                    
#$ -l h_rss=32G,mem_free=32G,h_data=32G # Request memory 
#$ -q research-r8.q                  # Submit job to the 'research-r8.q' queue
#$ -j y                              # Merge standard output and error streams into a single file
#$ -m ba                             
#$ -o /lustre/storeB/users/alessioc/node_output/OUT_$JOB_NAME.$JOB_ID  # Standard output log file
#$ -e /lustre/storeB/users/alessioc/node_output/OUT_$JOB_NAME.$JOB_ID  # Standard error log file (merged with -j y)
#$ -R y                              
#$ -r y                              

source /modules/rhel8/conda/install/etc/profile.d/conda.sh
conda activate production-08-2024

# Define Python script
python_script="/lustre/storeB/users/alessioc/download_from_CDSE/sync_and_store.py"

# end_date="20250113"
# start_date="20250106"
sat="all"

# # Execute the Python script with arguments
# python3 $python_script --start_date="$start_date" --end_date="$end_date" --sat="$sat"

while true; do
    start_date=$(date -d "1 days ago" +"%Y%m%d")
    end_date=$(date +"%Y%m%d")

    python3 $python_script --start_date="$start_date" --end_date="$end_date" --sat="$sat"

    sleep 600  # Wait 10 minutes
done