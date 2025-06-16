# File: stitch_json.py (Modified for Local Filesystem)

import pathlib

# The local directory containing the JSON part-files.
# This script expects this folder to be in the same directory you run it from.
local_source_dir = "intermediate_json_output"

# The name of the final, local JSON file you want to create
local_output_file = "real_covid_data_local.json"

print(f"Starting to stitch files from local directory: '{local_source_dir}'")

# --- Find all the partition files ---
# Use pathlib to find all files starting with 'part-' in the source directory.
# We sort them to ensure a consistent order every time you run the script.
source_files = sorted(pathlib.Path(local_source_dir).glob('part-*.json'))

# Check if any files were found before proceeding
if not source_files:
    print(f"Error: No 'part-*.json' files found in the directory '{local_source_dir}'.")
    print("Please make sure the directory exists and contains your partition files.")
else:
    # --- Stitch the files together ---
    with open(local_output_file, 'w') as f_out:
        # Write the opening bracket for the JSON array
        f_out.write('[\n')

        # This flag helps us correctly place commas between JSON objects
        is_first_object = True

        for file_path in source_files:
            print(f"  -> Processing file: {file_path.name}")
            with open(file_path, 'r') as f_in:
                for line in f_in:
                    # Skip any blank lines
                    if not line.strip():
                        continue

                    # If this is not the very first object, write a comma and a newline first
                    if not is_first_object:
                        f_out.write(',\n')

                    # Write the actual JSON object (the line from the file)
                    f_out.write(line.strip())
                    
                    # From now on, every object will need a comma before it
                    is_first_object = False

        # Write the closing bracket for the JSON array
        f_out.write('\n]\n')

    print(f"\nSuccess! Final JSON array created at: {local_output_file}")