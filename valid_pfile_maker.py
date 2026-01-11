#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Filter endpoint_pfile CSV to only include valid direct addresses.

This script reads the validation results from output.csv and filters
the original endpoint_pfile to only include rows where ValidDirect=1.
"""

import sys
import csv
import os

__author__ = "Fred Trotter"


def load_valid_endpoints(*, output_csv_filepath):
    """
    Load the set of valid direct addresses from output.csv.
    Returns a set of (NPI, Endpoint) tuples where ValidDirect=1.
    """
    if not os.path.exists(output_csv_filepath):
        print(f"Error: Validation file {output_csv_filepath} does not exist.")
        print("Please run process_nppes.py first to generate the validation results.")
        sys.exit(1)
    
    valid_endpoints = set()
    
    with open(output_csv_filepath, 'r') as fh:
        reader = csv.DictReader(fh)
        
        for row in reader:
            # Only include rows where ValidDirect equals "1"
            if row.get('ValidDirect') == '1':
                npi = row['NPI']
                endpoint = row['Endpoint']
                valid_endpoints.add((npi, endpoint))
    
    print(f"Loaded {len(valid_endpoints)} valid direct addresses from {output_csv_filepath}")
    return valid_endpoints


def filter_endpoint_pfile(*, input_csv_filepath, output_csv_filepath, valid_endpoints):
    """
    Filter the endpoint_pfile CSV to only include valid direct addresses.
    
    Args:
        input_csv_filepath: Path to the original endpoint_pfile CSV
        output_csv_filepath: Path where the filtered CSV will be written
        valid_endpoints: Set of (NPI, Endpoint) tuples that are valid
    """
    if not os.path.exists(input_csv_filepath):
        print(f"Error: Input file {input_csv_filepath} does not exist.")
        sys.exit(1)
    
    rows_read = 0
    rows_written = 0
    
    with open(input_csv_filepath, 'r') as input_fh, \
         open(output_csv_filepath, 'w', newline='') as output_fh:
        
        reader = csv.reader(input_fh)
        writer = csv.writer(output_fh)
        
        # Read and write the header
        header = next(reader)
        writer.writerow(header)
        
        # Find the column indices for NPI and Endpoint
        npi_index = header.index('NPI')
        endpoint_index = header.index('Endpoint')
        
        # Process each row
        for row in reader:
            rows_read += 1
            
            npi = row[npi_index]
            endpoint = row[endpoint_index]
            
            # Check if this endpoint is valid
            if (npi, endpoint) in valid_endpoints:
                writer.writerow(row)
                rows_written += 1
            
            # Progress indicator every 10000 rows
            if rows_read % 10000 == 0:
                print(f"Processed {rows_read} rows, wrote {rows_written} valid rows...")
    
    print(f"\nFiltering complete!")
    print(f"Total rows read: {rows_read}")
    print(f"Valid direct address rows written: {rows_written}")
    print(f"Output written to: {output_csv_filepath}")


def create_valid_pfile(*, input_csv_filepath, validation_csv_filepath, output_csv_filepath):
    """
    Main function to create a filtered endpoint_pfile with only valid direct addresses.
    
    Args:
        input_csv_filepath: Path to the original endpoint_pfile CSV
        validation_csv_filepath: Path to the validation results CSV (output.csv)
        output_csv_filepath: Path where the filtered CSV will be written
    """
    print("Step 1: Loading valid direct addresses from validation file...")
    valid_endpoints = load_valid_endpoints(output_csv_filepath=validation_csv_filepath)
    
    print("\nStep 2: Filtering endpoint_pfile to only include valid direct addresses...")
    filter_endpoint_pfile(
        input_csv_filepath=input_csv_filepath,
        output_csv_filepath=output_csv_filepath,
        valid_endpoints=valid_endpoints
    )


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Create a filtered endpoint_pfile containing only valid direct addresses.")
        print("\nUsage: valid_pfile_maker.py [input_endpoint_pfile] [validation_csv] [output_filtered_pfile]")
        print("\nExample:")
        print("  python valid_pfile_maker.py data/endpoint_pfile_20050523-20250810.csv data/output.csv data/valid_endpoint_pfile.csv")
        sys.exit(1)
    
    input_file = sys.argv[1]
    validation_file = sys.argv[2]
    output_file = sys.argv[3]
    
    create_valid_pfile(
        input_csv_filepath=input_file,
        validation_csv_filepath=validation_file,
        output_csv_filepath=output_file
    )
