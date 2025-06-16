# File: combine_jsonl.py

import pathlib

# The local directory containing your 6 partition files.
# This script expects this folder to be in the same directory you run it from.
local_source_dir = "intermediate_json_output"

# The name of the final, single JSON Lines file you want to create.
output_file_name = "combined_data.jsonl"

print(f"Starting to combine files from local directory: '{local_source_dir}'")

# Find all files starting with 'part-' in the source directory.
# We sort them to ensure they are processed in order.
source_files = sorted(pathlib.Path(local_source_dir).glob('part-*.json'))

if not source_files:
    print(f"Error: No 'part-*.json' files were found in the directory '{local_source_dir}'.")
    print("Please make sure the directory exists and contains your files.")
else:
    # Open the final output file in 'write' mode
    with open(output_file_name, 'w') as f_out:
        # Loop through each source file found
        for file_path in source_files:
            print(f"  -> Appending content from: {file_path.name}")
            # Open the current part-file in 'read' mode
            with open(file_path, 'r') as f_in:
                # Read the content of the part-file and write it to the output file
                f_out.write(f_in.read())

    print(f"\nSuccess! All part-files have been combined into: {output_file_name}")