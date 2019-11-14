import sys
import csv
import logging
def append_id(filename,id):
    return "{0}_{2}.{1}".format(*filename.rsplit('.', 1) + [id])

def write_data(surveys, out_header, params,rounding=4,sub_file=None):
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
                    else:
                        ws[k] = s[k]
            writer.writerow(ws)
    else:
        if sub_file is not None:
            filename = append_id(params['outfile'],sub_file)
        else:
            filename = params['outfile']
        logging.debug('Writing to location: %s' % filename)

        with open(filename, mode='w', newline='') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=out_header)
            writer.writeheader()
            for s in surveys:
                ws = {}
                for k in s.keys():
                    if k in out_header:
                        if type(s[k]) == type(0.1):
                            ws[k] = round(s[k], rounding)
                        else:
                            ws[k] = s[k]
                writer.writerow(ws)
