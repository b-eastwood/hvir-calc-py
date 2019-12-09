import getopt
import sys
import reader
import writer
import data_processor
import datetime
import logging
import logfiler
import os

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
        required = ['outfile','filepath']
        for r in required:
            if r not in params.keys():

                print("A requried parameter <%s> was missing" % r)
                raise getopt.GetoptError(msg="A requried parameter %s was missing" % r )
    except getopt.GetoptError:
        # Print something useful
        print('Must supply a filepath or stdin, methods specs or config settings')
        sys.exit(2)
    return params


def main():
    # Get the arguments from the command-line except the filename
    argv = sys.argv[1:]
    params = get_params(argv)
    print('Program started, input file: %s' % params['filepath'])

    if 'debug' in params.keys():
        if params['debug'] == '1':
            print('Setting debug level: Debug')
            logging.getLogger().setLevel(logging.DEBUG)
        else:
            print('Setting debug level: Warnings')
            logging.getLogger().setLevel(logging.WARNING)
    else:
        logging.getLogger().setLevel(logging.CRITICAL)

    params['data_params'], type_dict = reader.get_data_settings(params['config_file'])
    header, raw_data = reader.get_data(params)
    type_selector, converters = reader.validate_data_format(params['data_params'], header)
    key_fails, failed_rows, surveys,quality_assessment, out_keys,meta = data_processor.process_rows(raw_data, header, params, converters)
    if 'logfile' in params:
        #writer.write_log(logfiler.write_txt_log(params, key_fails, raw_data, failed_rows, meta), params['logfile'], 'Log:')
        writer.write_log(logfiler.create_pbi_log(quality_assessment, meta['attribute_quality'], meta, failed_rows,params), params['logfile'], ['Key', 'Value'])
    out_header = surveys[0].keys()
    writer.write_data(surveys, out_header, params,rounding=9)
    if len(quality_assessment) > 0:
        writer.write_data(quality_assessment, quality_assessment[0].keys(), params,sub_file='group_qual')
    if len(meta['attribute_quality']) > 0:
        writer.write_data(meta['attribute_quality'], meta['attribute_quality'][0].keys(),params,sub_file='attr_qual')
if __name__ == "__main__":
    main()
