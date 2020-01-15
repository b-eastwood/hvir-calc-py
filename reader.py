import sys
from datetime import datetime
import os
import csv
import json
import logging
from data_processor import create_typer



def get_data(params):
    if not sys.stdin.isatty():
        logging.debug('Reading stdin')
        data_stream = sys.stdin
        csv_reader = load_stdin(data_stream)
        header, raw_data = read_file(csv_reader)
    else:
        try:
            logging.debug('Reading from file')
            csv_file, data_stream = load_csv(params['filepath'])
            header, raw_data = read_file(data_stream)
            csv_file.close()
        except FileNotFoundError:
            logging.critical("Input file Not Found: %s",params['filepath'])
            exit(1)
    return header, raw_data


def load_csv(file_location):
    if not file_location.lower().endswith('.csv'):
        raise ValueError('File: {file_location} is not a csv'.format(file_location=file_location))
    if not os.path.exists(file_location):
        raise FileNotFoundError('{f} Not Found'.format(f=file_location))

    csv_file = open(file_location, encoding='UTF-8'.lower())
    csv_reader = csv.reader(csv_file, delimiter=',')
    return csv_file, csv_reader


def load_stdin(std):
    csv_reader = csv.reader(std, delimiter=',')
    return csv_reader


def read_file(csv_reader):
    raw_data = []
    line_count = 0
    header = []
    for row in csv_reader:
        if line_count == 0:
            if any(cell.isdigit() for cell in row):  # Top row is not header
                raw_data.append(row.strip('\n'))
            else:
                header = row
                try:
                    header = [h.lower() for h in header]
                except TypeError:
                    logging.warning("Couldn't parse header correctly")
            line_count += 1
        else:
            raw_data.append(row)
            line_count += 1

    logging.debug('Read %s lines.' % str(line_count))
    return header, raw_data


def get_data_settings(config_file):
    with open(config_file) as json_file:
        settings = json.load(json_file)
        type_dict = {key: settings['datatypes'][key]['type'] for key in settings['datatypes']}
    for k in type_dict:
        if type_dict[k] == 'datetime':
            type_dict[k] = 'str'
    return settings, type_dict


def validate_data_format(settings, header):
    converters = {}
    header_error = False
    bad_headers = []

    type_selector = create_typer(settings['datetime_format'])
    for key in settings['datatypes']:
        converters[key.lower()] = type_selector[settings['datatypes'][key.lower()]['type']]
    for key in header:
        if key not in converters:
            header_error = True
            bad_headers.append(key)
    if header_error:
        raise KeyError("One or more of the header names:\n <%s> \nin the csv does not match the converter list:\n %s" % (
        str(bad_headers), str(list(converters.keys()))))

    return type_selector, converters
