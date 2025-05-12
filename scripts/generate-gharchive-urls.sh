#!/bin/bash

# Usage: ./generate-gharchive-urls.sh 2018 2022 /path/to/output/dir
start_year=$1
end_year=$2
output_dir=$3

if [[ -z "$start_year" || -z "$end_year" || -z "$output_dir" ]]; then
    echo "Usage: $0 <start_year> <end_year> <output_dir>"
    exit 1
fi

# Create the output directory if it doesn't exist
mkdir -p "$output_dir"

for year in $(seq $start_year $end_year); do
    output_file="${output_dir}/download-gharchive-${year}.txt"
    >"$output_file" # Clear file if it already exists

    for month in $(seq -w 1 12); do
        # Get number of days in month
        days_in_month=$(cal $month $year | awk 'NF {DAYS = $NF}; END {print DAYS}')
        for day in $(seq -w 1 $days_in_month); do
            for hour in $(seq -w 0 23); do
                echo "https://data.gharchive.org/${year}-${month}-${day}-${hour}.json.gz" >>"$output_file"
            done
        done
    done

    echo "URL list for $year saved to $output_file"
done
