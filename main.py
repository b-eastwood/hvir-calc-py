import getopt
import sys
import reader
import writer
import data_processor
import datetime


def get_params(argv):
    arg_keys = {'f': 'filepath',
                'a': 'a_method',
                'r': 'r_method',
                'w': 'w_method',
                'o': 'outfile',
                'l': 'logfile',
                'c': 'config_file'}

    # Specify some default paramaters
    params = {'config_file': 'config/settings.config',
              'a_method': 'limits',
              'r_method': 'iri'}
    try:
        # Define the getopt parameters
        opts, args = getopt.getopt(argv, 'f:a:r:w:o:l:')
        if len(opts) > 0:
            keys, vals = zip(*opts)
            for k, key in enumerate(keys):
                params[arg_keys[key.strip('-')]] = vals[k]

    except getopt.GetoptError:
        # Print something useful
        print('Must supply a filepath or stdin, methods specs or config settings')
        sys.exit(2)
    return params


def main():
    # Get the arguments from the command-line except the filename
    argv = sys.argv[1:]
    params = get_params(argv)
    params['data_params'], type_dict = reader.get_data_settings(params['config_file'])
    header, raw_data = reader.get_data(params)
    type_selector, converters = reader.validate_data_format(params['data_params'], header)
    key_fails, failed_rows, surveys, out_keys = data_processor.process_rows(raw_data, header, params, converters)
    if 'logfile' in params:
        with open(params['logfile'], 'w') as logfile:
            now = datetime.datetime.now()
            logfile.writelines(['Completed: ' + now.strftime("%B %d, %Y") + '\n'])
            logfile.writelines(['Total rows completed: %s\n' % str(len(raw_data) - len(failed_rows))])
            logfile.writelines(['Total of %s rows were not completed' % len(failed_rows) + '\n'])
            for key in key_fails.keys():
                logfile.writelines([str(key_fails[key]) + ' ' + str(key) + ' key(s) could not be read' + '\n'])
            logfile.writelines(['%s surveys read, %s surveys failed' % (len(raw_data), len(failed_rows)) + '\n'])
            logfile.writelines(['%s percent success rate' % str(
                round((len(raw_data) - len(failed_rows)) / len(raw_data) * 100, 2)) + '\n'])

    out_header = header + out_keys
    writer.write_data(surveys, out_header, params)


if __name__ == "__main__":
    main()
