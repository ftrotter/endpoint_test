#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: ai ts=4 sts=4 et sw=4
# Written by Alan Viars
import sys
import csv
import os
from validate_email import validate_email
from gdc.get_direct_certificate import DCert

__author__ = "Alan Viars"

# Flush output every N rows for safety
FLUSH_INTERVAL = 50


def count_output_rows(*, output_csv_filepath):
    """
    Count the number of data rows in the output CSV file (excluding header).
    Returns 0 if file doesn't exist or only has header.
    """
    if not os.path.exists(output_csv_filepath):
        return 0
    
    try:
        with open(output_csv_filepath, 'r') as fh:
            reader = csv.reader(fh)
            # Skip header if present
            try:
                next(reader)
            except StopIteration:
                return 0
            
            # Count remaining rows
            row_count = sum(1 for _ in reader)
            return row_count
    except Exception as e:
        print(f"Warning: Could not read output file {output_csv_filepath}: {e}")
        return 0


def process_endpoint_csv(input_csv_filepath,
                         output_csv_filepath="output.csv"):
    """
    Process NPPES endpoint CSV with resume capability.
    If output file exists, will resume from the last processed row.
    """
    output_fieldnames = [
        "NPI",
        "EndpointType",
        "Endpoint",
        "ValidEmail",
        "ValidDirect",
        "cert_protocol"]
    
    # Check if we're resuming from a previous run
    existing_rows = count_output_rows(output_csv_filepath=output_csv_filepath)
    
    if existing_rows > 0:
        print(f"Found existing output with {existing_rows} rows. Resuming from row {existing_rows + 1}...")
        output_mode = 'a'  # Append mode
        write_header = False
    else:
        print("Starting fresh processing...")
        output_mode = 'w'  # Write mode
        write_header = True
    
    # Open files for streaming
    input_fh = open(input_csv_filepath, 'r')
    output_fh = open(output_csv_filepath, output_mode)
    
    input_csv = csv.reader(input_fh, delimiter=',')
    writer = csv.DictWriter(output_fh, fieldnames=output_fieldnames)
    
    # Write header only if starting fresh
    if write_header:
        writer.writeheader()
    
    # Skip the header row in input
    next(input_csv)
    
    # Skip already processed rows if resuming
    if existing_rows > 0:
        print(f"Skipping {existing_rows} already processed rows...")
        for i in range(existing_rows):
            try:
                next(input_csv)
            except StopIteration:
                print(f"Warning: Input file has fewer rows than output. Starting from end of input.")
                break
    
    # Process remaining rows with periodic flushing
    rows_processed = 0
    total_rows_in_output = existing_rows
    
    try:
        for row in input_csv:
            total_rows_in_output += 1
            
            outrow = {}
            outrow['NPI'] = row[0]
            outrow['EndpointType'] = row[1]
            outrow['Endpoint'] = row[3]
            outrow['ValidEmail'] = ""
            outrow['ValidDirect'] = ""
            outrow['cert_protocol'] = ""
            
            # Determine status message for this row
            status_message = ""
            
            # Handle non-DIRECT/EMAIL endpoints
            if outrow['EndpointType'] not in ("DIRECT", "EMAIL"):
                status_message = "Not a Direct endpoint.. skipping"
                print(f"Processing {outrow['Endpoint']} for NPI {outrow['NPI']}: {status_message}")
                writer.writerow(outrow)
                rows_processed += 1
                continue
            
            # Process DIRECT or EMAIL endpoints
            outrow["ValidEmail"] = validate_email(outrow['Endpoint'])
            
            if outrow['EndpointType'] == "DIRECT":
                dc = DCert(outrow['Endpoint'])
                dc.validate_certificate(False)

                if dc.result['is_found']:
                    outrow['ValidDirect'] = "1"
                    # Determine if LDAP or DNS based on the certificate retrieval method
                    method = dc.result.get('method', 'DNS')
                    if isinstance(method, str) and method.upper() == 'LDAP':
                        outrow['cert_protocol'] = "ldap"
                        status_message = "Success: LDAP Certificate retrieved"
                    else:
                        outrow['cert_protocol'] = "dns"
                        status_message = "Success: DNS Certificate retrieved"
                else:
                    outrow['ValidDirect'] = "0"
                    # Certificate download failed, but if email is valid
                    if outrow['ValidEmail']:
                        status_message = "Success: Email data is a properly formed email address"
                    else:
                        status_message = "Failed: Could not retrieve certificate"
            else:
                # EndpointType is EMAIL (not DIRECT)
                if outrow['ValidEmail']:
                    status_message = "Success: Email data is a properly formed email address"
                else:
                    status_message = "Failed: Invalid email format"

            # Print single status line for this row
            print(f"Processing {outrow['Endpoint']} for NPI {outrow['NPI']}: {status_message}")
            
            writer.writerow(outrow)
            
            rows_processed += 1
            
            # Periodic flush for safety
            if rows_processed % FLUSH_INTERVAL == 0:
                output_fh.flush()
                print(f"Progress: {rows_processed} rows processed in this session, {total_rows_in_output} total rows in output")
    
    except KeyboardInterrupt:
        print(f"\nProcessing interrupted by user. Progress saved: {total_rows_in_output} rows in output.")
        print(f"Run the script again to resume from row {total_rows_in_output + 1}")
    except Exception as e:
        print(f"\nError during processing at row {total_rows_in_output}: {e}")
        print(f"Progress saved: {total_rows_in_output} rows in output.")
        print(f"Run the script again to resume from row {total_rows_in_output + 1}")
        raise
    finally:
        # Ensure final flush and close files
        output_fh.flush()
        input_fh.close()
        output_fh.close()
    
    print(f"\nProcessing complete!")
    print(f"Total rows written to {output_csv_filepath}: {total_rows_in_output}")
    if existing_rows > 0:
        print(f"(Resumed from row {existing_rows + 1}, processed {rows_processed} new rows in this session)")

if __name__ == "__main__":

    # Get the file from the command line
    if len(sys.argv) not in (2, 3):
        print("You must supply an NPPES endpoint input file. If the destination file is omitted, the default is output.csv")
        print(
            "Usage: process_nppes_endpoint_file.py [nppes_endpoint_file] <nppes_output_file>")
        sys.exit(1)
    else:
        if len(sys.argv) == 2:
            process_endpoint_csv(sys.argv[1])
        else:
            process_endpoint_csv(sys.argv[1], sys.argv[2])
