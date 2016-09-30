# -*- coding: utf-8 -*-
import boto3
import datetime as dt
import csv
import uuid
import xlrd

s3 = boto3.resource('s3')

HEADER_ROW = ["Date", "Type", "US Port", "Region", "Country Name", "Foreign Port", "Citizens Total", "Citizens Pct",
              "Aliens Total", "Aliens Pct", "US Flag Total", "US Flag Pct", "Foreign Flag Total", "Foreign Flag Pct",
              "Scheduled Flights Total", "Scheduled Flights Pct", "Chartered Flights Total", "Chartered Flights Pct",
              "Passenger Total"]
VALUES_AT = [2, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 18]


def handler(event, context):
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    bucket = s3.Bucket(bucket_name)
    all_rows = [HEADER_ROW]
    for obj in bucket.objects.all():
        key = obj.key
        download_path = '/tmp/{}{}'.format(uuid.uuid4(), key)
        event_type = "Arrival"
        if "Depart" in key: event_type = "Departure"
        date = normalize_date(' '.join(key.split()[0:2]))
        outputfile_path = '/tmp/{}-{}.csv'.format(date, event_type)
        bucket.download_file(key, download_path)
        excel2csv(download_path, outputfile_path)
        rows = rowify(outputfile_path, date, event_type)
        print "Processed file {} with {} entries".format(key, len(rows))
        all_rows.extend(rows)
    csv_rows = [",".join(row) for row in all_rows]
    s3.Object('i92', 'entries.csv').put(Body="\n".join(csv_rows), ContentType='application/csv')
    return "Uploaded entries.csv file with %i entries" % (len(csv_rows) - 1)


def excel2csv(download_path, outputfile_path):
    workbook = xlrd.open_workbook(download_path)
    output_csv_file = open(outputfile_path, 'wb')
    csv_writer = csv.writer(output_csv_file)
    sheet = workbook.sheet_by_index(0)
    for rownum in xrange(sheet.nrows):
        csv_writer.writerow(sheet.row_values(rownum))
    output_csv_file.close()


def rowify(outputfile_path, date, event_type):
    rows = []
    with open(outputfile_path, 'rb') as csvfile:
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
