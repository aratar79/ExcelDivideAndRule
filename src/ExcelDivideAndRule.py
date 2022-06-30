import argparse
import sys
import pandas as pd
import random as rnd
import datetime
import sys
import os
import shutil

from csv import excel
from operator import index
from queue import PriorityQueue
from tqdm import tqdm
from tabulate import tabulate


def exists_files(file_name):

    if file_name.startswith(".\\"):
        prefix = os.path.splitext(file_name)[0].split("\\")[1]
    else:
        prefix = os.path.splitext(file_name)[0]

    result = 0
    
    files = [
        filename
        for filename in os.listdir(os.getcwd())
        if filename.startswith(f"{prefix}_output_")
    ]

    if files:

        names = [os.path.splitext(base)[0] for base in files]
        if names:
            end_names = [name.split("_")[-1] for name in names]
            nums = [int(num) for num in end_names if num.isdigit()]
            if nums:
                result = max(nums) + 1
    
    return result


def run_process(file_name, sheet_name, split_parts, init_row):

    outfile_num = exists_files(file_name)
    head_rows = init_row - 1
    print("Loading main file, may take a few minutes....")
    main_file = pd.read_excel(file_name, sheet_name)
    print("\n")
    main_headers = main_file.keys()
    main_num_row = (len(main_file.index) + 1) - head_rows
    headers = [head for head in main_headers]
    columns = len(headers)
    books = []

    tab_file_headers = ["File name", "Rows", "Cols", "File parts"]
    tab_part_headers = ["Index", "File name", "Rows"]

    tab_file_data = [file_name, main_num_row, columns, split_parts]

    data_file = [tab_file_data]
    data_parts = []
    data_part = []

    print(
        tabulate(
            data_file,
            headers=tab_file_headers,
            showindex="never",
            tablefmt="simple",
        )
    )
    print("\n")

    parts = [main_num_row // split_parts] * split_parts
    remainder = main_num_row % split_parts

    for i in range(remainder):
        parts[i] += 1

    for i in range(split_parts):
        new_file = pd.DataFrame(columns=range(len(headers)))
        new_file.columns = [head for head in headers]
        name_file = f"{os.path.splitext(file_name)[0]}_output_{i + outfile_num}.xlsx"
        writer = pd.ExcelWriter(name_file, engine="xlsxwriter")
        books.append(name_file)
        new_file.to_excel(writer, sheet_name, index=False)
        writer.save()

    if not len(books) == len(parts) == split_parts:
        sys.exit()

    for i in tqdm(range(len(books)), ascii=True, desc="Split file process.", leave=True):
        rows = parts[i]
        output_file = pd.read_excel(books[i])
        data_part = [books[i], rows]
        values_to_add = {}
        if i == 0:
            start_row = head_rows - 1
        end_row = start_row + rows
        for row in tqdm(range(start_row, end_row, 1), ascii=True, desc=books[i], leave=False):
            for column in range(columns):
                values_to_add[headers[column]] = main_file[headers[column]].values[row]
            row_to_add = pd.DataFrame(values_to_add, index=[row])
            output_file = pd.concat([output_file, row_to_add])
        output_file.to_excel(books[i], sheet_name, index=False)
        start_row = end_row
        data_parts.append(data_part)

    print("\n")
    print(
        tabulate(
            data_parts,
            headers=tab_part_headers,
            showindex="always",
            tablefmt="simple",
        )
    )


def main(argv):
    try:
        parser = argv

        parser.add_argument("-f", "--file", help="Name of the file to be processed.")
        parser.add_argument(
            "-s",
            "--sheet-name",
            help="File sheet to be processed.",
            type=str,
            default="Hoja1",
        )
        parser.add_argument(
            "-p",
            "--parts",
            help="The number of parts into which you will split the file.",
            type=int,
            default=0,
        )
        parser.add_argument(
            "-i",
            "--init-row",
            help="Row number at which to start processing.",
            type=int,
            default=1,
        )

        arguments = parser.parse_args()

        if arguments.file:
            run_process(
                arguments.file,
                arguments.sheet_name,
                arguments.parts,
                arguments.init_row,
            )

        else:
            parser.print_help()
    except Exception as e:
        print(f"ERROR: {e}")


if __name__ == "__main__":
    print(f'\nDirectory: {os.getcwd()}\n')
    main(
        argparse.ArgumentParser(description="Splits an excel .xlsx file into N files.")
    )
