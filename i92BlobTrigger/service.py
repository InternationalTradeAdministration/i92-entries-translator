# -*- coding: utf-8 -*-
import os
import datetime as dt
import csv
import re
import xlrd
from azure.storage.blob.blockblobservice import BlockBlobService
import tempfile


HEADER_ROW = ["Date", "Type", "US Port", "Region", "Country Name", "Foreign Port", "Citizens Total", "Citizens Pct",
              "Aliens Total", "Aliens Pct", "US Flag Total", "US Flag Pct", "Foreign Flag Total", "Foreign Flag Pct",
              "Scheduled Flights Total", "Scheduled Flights Pct", "Chartered Flights Total", "Chartered Flights Pct",
              "Passenger Total"]
VALUES_AT = [2, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 18]

conn_str = os.environ["AzureWebJobsStorage"]
acct_name = re.search('AccountName=(.+?);', conn_str).group(1)
acct_key = re.search('AccountKey=(.+?);', conn_str).group(1)
container_name = os.environ["ContainerName"]

def handler(blob):
    all_rows = [HEADER_ROW]
    key = blob.name.replace(container_name+"/", "")
    event_type = "Arrival"
    if "Depart" in key: event_type = "Departure"
    date = normalize_date(' '.join(key.split()[0:2]))
    outputfile = tempfile.NamedTemporaryFile(mode='r+', delete=False)
    excel2csv(blob, outputfile)
    rows = rowify(outputfile, date, event_type)
    print("Processed file {} with {} entries".format(key, len(rows)))
    all_rows.extend(rows)
    csv_rows = [",".join(row) for row in all_rows]

    file_extension = re.search('.+\.(.+?)$', key).group(1)
    new_file_name = 'translated/' + key.replace(file_extension, 'csv')
    block_blob_service = BlockBlobService(account_name=acct_name, account_key=acct_key)
    
    upload_data = ''
    for r in csv_rows:
        upload_data = upload_data+r+'\n'
    block_blob_service.create_blob_from_text(container_name=container_name, blob_name=new_file_name, text=str(upload_data))


def excel2csv(blob_in, outputfile):
    workbook = xlrd.open_workbook(file_contents=blob_in.read())
    output_csv_file = open(outputfile.name, 'w+')
    csv_writer = csv.writer(output_csv_file)
    sheet = workbook.sheet_by_index(0)
    for rownum in range(sheet.nrows):
        csv_writer.writerow(sheet.row_values(rownum))


def rowify(outputfile, date, event_type):
    rows = []
    with open(outputfile.name, 'r+') as csvfile:
        csvreader = csv.reader(csvfile)
        content_reached = False
        curr_us_port = curr_foreign_port = curr_region = curr_country = None
        for csv_row in csvreader:
            location = csv_row[0]
            if not content_reached and location.startswith('**'):
                content_reached = True
            else:
                if location.startswith('     '):
                    curr_foreign_port = quote(location)
                    row = [date, event_type, curr_us_port, curr_region, curr_country, curr_foreign_port]
                    row.extend(normalize_row(csv_row))
                    rows.append(row)
                elif location.startswith('   '):
                    curr_country = location.strip()
                elif location.startswith('  '):
                    curr_region = location.strip()
                elif location.startswith('*'):
                    us_port = location.replace('*', '').replace(" Totals", '')
                    curr_us_port = quote(us_port)

    return rows


def quote(str):
    return "\"{}\"".format(str.strip())


def normalize_row(csv_row):
    values = [csv_row[index] for index in VALUES_AT]
    return [item.replace(',', '') if isinstance(item, str) else item for item in values]


def normalize_date(entry_date):
    entry_date = entry_date.replace('Sept ', 'September ').replace('Jan ', 'January ')
    return dt.datetime.strptime(entry_date, '%B %Y').strftime("%Y-%m")
