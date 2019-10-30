import sys
import csv


def write_data(surveys, out_header, params,rounding=4):
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
        # print('Writing to file')
        with open(params['outfile'], mode='w', newline='') as csv_file:
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
