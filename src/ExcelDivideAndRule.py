from csv import excel
from queue import PriorityQueue
import sys
import pandas as pd
import random as rnd
import datetime


def main():

    init_row = 3 #parameter initial row
    head_rows = (init_row - 1)
    main_file = pd.read_excel('libro33.xlsx')
    main_headers = main_file.keys()
    main_num_row = (len(main_file.index) + 1) - head_rows
    headers = [head for head in main_headers]
    books = []

    divider = 4

    parts = [ main_num_row//divider ] * divider    
    remainder = main_num_row % divider

    for i in range(remainder):
      parts[i]+=1

    for i in range(divider):
        new_file = pd.DataFrame(columns = range(len(headers)))
        new_file.columns = [head for head in headers]
        name_file = f'output_file_{i}.xlsx'
        writer = pd.ExcelWriter(name_file, engine='xlsxwriter')
        books.append(name_file)
        new_file.to_excel(writer, sheet_name='hoja1', index=False)
        writer.save()

    if not len(books) == len(parts) == divider:
        sys.exit()
    
    
    columns = len(headers)
    
    for i in range(len(books)):
        rows = parts[i]
        output_file = pd.read_excel(books[i])
        values_to_add = {}
        if i == 0 :
            start_row = head_rows - 1
        end_row = start_row + rows
        for row in range(start_row, end_row, 1):
            for column in range(columns):
                values_to_add[headers[column]] = main_file[headers[column]].values[row]
            row_to_add = pd.Series(values_to_add)
            output_file = output_file.append(row_to_add, ignore_index=True)
        output_file.to_excel(books[i], sheet_name='Hoja1', index=False)
        start_row = end_row

if __name__ == '__main__':
    main()