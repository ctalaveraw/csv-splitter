"""
Authors:
https://github.com/ctalaveraw

CSVSplitter
This script uses the "Dask" and "Pandas" libraries to load and process
large data sets into memory via chunks.

**Before running the script**
- Define the schema using the 'CSV_SCHEMA' global variable
- Define the output path using the 'OUTPUT_PATH' global variable 
- import the `dask` and `pandas` libraries 
    - Do this by running this shell command: 'python3 -m pip install dask pandas'
"""

import dask.dataframe as dd

# Insert your own CSV schema here
CSV_SCHEMA = {
    'FIELD_1': str,
    'FIELD_2': str,
    'FIELD_3': str,
    'FIELD_4': str,
    'FIELD_5': str,
    'FIELD_6': str,
    'FIELD_7': str,
    'FIELD_8': str
}
OUTPUT_PATH = 'result/output-*.csv'


def program_start():
    print('\nWelcome!')
    print('\nThis program accepts a large CSV file as input and outputs them into smaller CSVs.')
    print('Note: The input CSV must be in the working directory of this script!')
    continue_message = str('\nTap any key!\n')
    input(continue_message)


# Using Dask library to split CSV:
def split_files(ofn, fs):
    ddf = dd.read_csv(ofn, blocksize=fs, dtype=CSV_SCHEMA)
    ddf.to_csv(OUTPUT_PATH)


def main():
    program_start()
    input_csv_name = input(
        '\nPlease enter the filename without extension:\n(Example is entering "test" for "test.csv")\n')
    file_size = ((input('\nPlease enter the amount of MB for each file:\n')) + 'MB')
    output_file_name = f'{input_csv_name}.csv'
    print(f'\nThe file to be split is "{output_file_name}".')
    print(f'NOTE: Larger files might take a few minutes. so please be patient!')
    split_files(output_file_name, file_size)
    print(f'\nThe resulting files are now outputted in "./{OUTPUT_PATH}".')


main()
