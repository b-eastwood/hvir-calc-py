import sys
import csv
import logging
from datetime import datetime
from datetime import date as date_obj

def append_id(filename,id):
    return "{0}_{2}.{1}".format(*filename.rsplit('.', 1) + [id])

def financial_year_formatter(date):
    if date.date() > date_obj(date.year, 7, 1):
        return str(date.year) +'-'+ str((date.year+1))[2:]
    elif date.date() < date_obj(date.year, 7, 1):
        return str(date.year-1) +'-'+ str((date.year))[2:]

def write_log(lines,filename,out_header):
    new_lines = []
    for l in lines:
        l_dict = {}
        i = 0
        for v in l:
            l_dict[out_header[i]] = v
            i += 1
        new_lines.append(l_dict)
    lines = new_lines
    if not sys.stdout.isatty():
        # print('Writing to stdout')
        writer = csv.DictWriter(sys.stdout, fieldnames=out_header, lineterminator='\n')
        writer.writeheader()

        for l in lines:
            writer.writerow(l)
    else:
        logging.debug('Writing to location: %s' % filename)
        try:
            with open(filename, mode='w', newline='') as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=out_header)
                writer.writeheader()
                for l in lines:
                    writer.writerow(l)
                logging.info('Completed, outfile is here: %s' % filename)
        except PermissionError:
            logging.CRITICAL("Failed to write to disk, do you have the output file open?")
            exit(13)
        except FileNotFoundError:
            logging.critical("Couldn't write to output directory not found %s" % filename)
            exit(1)

def write_data(surveys, out_header, params,rounding=9,sub_file=None):
    if not sys.stdout.isatty():
        # print('Writing to stdout')
        writer = csv.DictWriter(sys.stdout, fieldnames=out_header, lineterminator='\n')
        writer.writeheader()

        for s in surveys:
            ws = {}
            for k in s.keys():
                if k in out_header:
                    if type(s[k]) == type(0.1):
                        ws[k] = round(s[k],rounding)
                    elif type(s[k]) == datetime:
                        if  'fin_year' in k:
                            ws[k] = financial_year_formatter(s[k])
                        else:
                            ws[k] = s[k].strftime("%Y%m%d")
                    else:
                        ws[k] = s[k]
            writer.writerow(ws)
    else:
        if sub_file is not None:
            filename = append_id(params['outfile'],sub_file)
        else:
            filename = params['outfile']
        logging.debug('Writing to location: %s' % filename)
        try:
            with open(filename, mode='w', newline='') as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=out_header)
                writer.writeheader()
                for s in surveys:
                    ws = {}
                    for k in s.keys():
                        if k in out_header:
                            if type(s[k]) == type(0.1):
                                ws[k] = round(s[k], rounding)
                            elif type(s[k]) == datetime:
                                if  'fin_year' in k:
                                    ws[k] = financial_year_formatter(s[k])
                                else:
                                    ws[k] = s[k].strftime("%Y%m%d")
                            else:
                                ws[k] = s[k]
                    writer.writerow(ws)
                logging.info('Completed, outfile is here: %s' % params['outfile'])

        except PermissionError:
            logging.critical("Failed to write to disk, do you have the output file open?")
            exit(13)
        except FileNotFoundError:
            logging.critical("Couldn't write to output directory not found %s" % params['outfile'])
            exit(13)