import getopt
import sys
import reader
import writer
import data_processor
import datetime
import logging

def get_params(argv):
    arg_keys = {'f': 'filepath',
                'a': 'a_method',
                'r': 'r_method',
                'w': 'w_method',
                'o': 'outfile',
                'l': 'logfile',
                'c': 'config_file',
                'd': 'debug'}

    # Specify some default paramaters
    params = {'config_file': 'config/settings.config',
              'quality_config_file': 'config/quality.config',
              'a_method': 'limits',
              'r_method': 'hati'}
    try:
        # Define the getopt parameters
        opts, args = getopt.getopt(argv, 'f:a:r:w:o:l:c:d:')
        if len(opts) > 0:
            keys, vals = zip(*opts)
            for k, key in enumerate(keys):
                params[arg_keys[key.strip('-')]] = vals[k]

    except getopt.GetoptError:
        # Print something useful
        print('Must supply a filepath or stdin, methods specs or config settings')
        sys.exit(2)
    return params


def write_log(params,key_fails,raw_data,failed_rows,meta):
    with open(params['logfile'], 'w') as logfile:
        now = datetime.datetime.now()
        logfile.writelines(['Completed: ' + now.strftime("%B %d, %Y") + '\n'])
        logfile.writelines(['Total rows in dataset: %s\n' % str(len(raw_data))])
        logfile.writelines(['Completeness: %s percent, %s BLANK attributes(s)\n' % ( round(len(key_fails) / (len(raw_data) * len(raw_data[0]))),len(key_fails) )])
        logfile.writelines(['Accuracy: %s percent, %s INVALID attributes(s) %s attributes(s) OUT OF RANGE\n' % (round(meta['accuracy'],2),meta['num_invalid'], meta['num_ranged'])])
        logfile.writelines(['Total of %s rows were not completed' % len(failed_rows) + '\n'])
        logfile.writelines(['Timeliness: Condition data from %s to %s\n' % (meta['min_date'].strftime('%d/%m/%Y'), meta['max_date'].strftime('%d/%m/%Y'))])
        # for key in key_fails.keys():
        #    logfile.writelines([str(key_fails[key]) + ' ' + str(key) + ' key(s) could not be read' + '\n'])
        # logfile.writelines(['%s surveys read, %s surveys failed' % (len(raw_data), len(failed_rows)) + '\n'])
        logfile.writelines(['%s percent success rate for HVIR' % str(
            round((len(raw_data) - len(failed_rows)) / len(raw_data) * 100, 2)) + '\n'])

def main():
    # Get the arguments from the command-line except the filename
    argv = sys.argv[1:]
    params = get_params(argv)
    print('Program started, input file: %s' % params['filepath'])

    if 'debug' in params.keys():
        if params['debug'] == '1':
            print('setting debug level')
            logging.getLogger().setLevel(logging.DEBUG)
        elif params['debug'] == '2':
            logging.getLogger().setLevel(logging.WARNING)
        else:
            logging.getLogger().setLevel(logging.CRITICAL)
    else:
        logging.getLogger().setLevel(logging.CRITICAL)

    params['data_params'], type_dict = reader.get_data_settings(params['config_file'])
    header, raw_data = reader.get_data(params)
    type_selector, converters = reader.validate_data_format(params['data_params'], header)
    key_fails, failed_rows, surveys,quality_assessment, out_keys,meta = data_processor.process_rows(raw_data, header, params, converters)
    if 'logfile' in params:
       write_log(params,key_fails,raw_data,failed_rows,meta)

    out_header = header + out_keys
    writer.write_data(surveys, out_header, params)
    writer.write_data(quality_assessment, quality_assessment[0].keys(), params,sub_file='quality')
    print('Completed, outfile is here: %s' % params['outfile'])

if __name__ == "__main__":
    main()
