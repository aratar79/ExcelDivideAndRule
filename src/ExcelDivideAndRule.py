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


def run_process(file_name, sheet_name, split_parts, init_row):

    head_rows = init_row - 1
    main_file = pd.read_excel(file_name, sheet_name)
    main_headers = main_file.keys()
    main_num_row = (len(main_file.index) + 1) - head_rows
    headers = [head for head in main_headers]
    books = []

    parts = [main_num_row // split_parts] * split_parts
    remainder = main_num_row % split_parts

    for i in range(remainder):
        parts[i] += 1

    for i in range(split_parts):
        new_file = pd.DataFrame(columns=range(len(headers)))
        new_file.columns = [head for head in headers]
        name_file = f"{file_name}_output_{i}.xlsx"
        writer = pd.ExcelWriter(name_file, engine="xlsxwriter")
        books.append(name_file)
        new_file.to_excel(writer, sheet_name, index=False)
        writer.save()

    if not len(books) == len(parts) == split_parts:
        sys.exit()

    columns = len(headers)

    for i in tqdm(range(len(books)), ascii=True, desc="Split file process."):
        rows = parts[i]
        output_file = pd.read_excel(books[i])
        values_to_add = {}
        if i == 0:
            start_row = head_rows - 1
        end_row = start_row + rows
        for row in range(start_row, end_row, 1):
            for column in range(columns):
                values_to_add[headers[column]] = main_file[headers[column]].values[row]
            row_to_add = pd.DataFrame(values_to_add, index=[row])
            output_file = pd.concat([output_file, row_to_add])
        output_file.to_excel(books[i], sheet_name, index=False)
        start_row = end_row


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
    main(
        argparse.ArgumentParser(description="Splits an excel .xlsx file into N files.")
    )
