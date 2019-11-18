import methods
import logging
from datetime import datetime
import json

def intersection(lst1, lst2):
    return list(set(lst1) & set(lst2))

def create_typer(datetime_format):
    # To add date and range check into the type selector
    type_selector = {'int':      lambda val: (int(val) if val != '' else None),
                     'float':    lambda val: (float(val) if val != '' else None),
                     'bool':     lambda val: (bool(int(val)) if val != '' else None),
                     'datetime': lambda val: (val if type(val) == datetime else datetime.strptime(val, datetime_format) if val != '' else None),
                     'str':      lambda val: (str(val).lower() if val != '' else None)
                     }
    return type_selector


def process_rows(raw_data, header, hvir_params, converters):
    surveys = []
    failed_rows = []
    key_fails = {}
    calculator = methods.HvirCalculator()
    out_keys = None
    quality_assessement = []
    meta = {}
    meta['num_ranged']  = 0
    meta['num_blank']   = 0
    meta['num_invalid'] = 0
    meta['total_acc_check'] = 0
    meta['accuracy'] = 0
    meta['min_date']    = datetime.strptime('19010101','%Y%m%d')
    meta['max_date']    = datetime.strptime('30000101','%Y%m%d')
    type_selector = create_typer(hvir_params['data_params']['datetime_format'])

    for row_num, row in enumerate(raw_data):
        survey = None
        try:
            survey, key_fails = cast_row(row, header, converters, key_fails)
        except:
            print("Couldn't read in this row: %s" % row_num)
            failed_rows.append(row_num)
        survey, out_keys = calculator.method_logic(survey, hvir_params)
        surveys.append(survey)
        try:
            survey, quality, num_invalid, num_blank, num_ranged, num_invalid, max_date, min_date, total_acc_check = check_quality(
                survey, hvir_params, type_selector)
            meta['num_ranged'] += num_ranged
            meta['num_blank'] += num_blank
            meta['num_invalid'] += num_invalid
            meta['total_acc_check'] += total_acc_check
            meta['min_date'] = min(min_date, meta['min_date'])
            meta['max_date'] = max(max_date, meta['max_date'])
            meta['accuracy'] = (num_invalid + num_blank + num_ranged) / (total_acc_check)
            quality_assessement.append(quality)

        except KeyError:
            logging.warning("couldn't calculate HVIR for this row: %s" % str(row_num))
            failed_rows.append(row_num)

    if meta['min_date'] == datetime.strptime('19010101', '%Y%m%d'):
        meta['min_date'] = 'Unknown'
    else:
        meta['min_date'].strftime('%d/%m/%Y')
    if meta['max_date'] == datetime.strptime('30000101', '%Y%m%d'):
        meta['min_date'] = 'Unknown'
    else:
        meta['min_date'].strftime('%d/%m/%Y')
    return key_fails, failed_rows, surveys, quality_assessement, out_keys,meta

def get_num_in(survey,keys):
    num = 0
    for key in keys:
        if key in survey.keys():
            if survey[key] != None:
                num += 1
    return num


def accurate_data(data_params,type_selector,survey,k):
    # Check range and format
    rng = data_params[k]['domain']['range']
    rng_type = data_params[k]['domain']['range_type']
    type = data_params[k]['type']
    if type == 'datetime':
        rng = [datetime.now() if x == '{dnow}' else None if x == 'None' else datetime.strptime(x,'%Y%m%d') for x in rng]  # Parse in the  datetime ranges
    try:
        if k in survey.keys():
            typed = type_selector[type](survey[k])
            if rng_type == 'set':
                if typed in rng:
                    #logging.debug('%s in set for key %s' % (typed, k))
                    return True, typed, None
                else:
                    logging.debug('%s Out of set for key %s' % (typed, k))
                    return False, None, 'ranged'
            elif rng_type == 'range':
                lower, upper = True, True
                if rng[0] != None:
                    if not (typed >= rng[0]):
                        lower = False
                if rng[1] != None:
                    if not (typed <= rng[1]):
                        upper = False
                if upper and lower:
                    #logging.debug('%s in range for key %s' % (typed, k))
                    return True,typed, None
                else:
                    logging.debug('%s Out of range for key %s' % (typed, k))
                    return False, None, 'ranged'
            elif rng_type == "None":
                # logging.debug('%s in range for key %s' % (typed, k))
                return True, typed, None
            else:
                logging.critical('bad range specified in config file fro key %s' % k)
                return False, None, 'ranged'
        else:
            logging.debug('data %s for key is missing ' %  k)
            return False, None, 'missing'
    except KeyError:
        logging.debug('Bad data format %s' % k)
        return False, None, 'bad data'
    except IOError:
        logging.debug("Bad type")
        return False, None, 'bad data'



def check_quality(survey,hvir_params,type_selector):
    quality = {}
    completeness      = {}
    accuracy          = {}
    max_dates = datetime.strptime('19010101','%Y%m%d')
    min_dates = datetime.strptime('30000101','%Y%m%d')
    num_blank   = 0
    num_ranged  = 0
    num_invalid = 0
    data_params       = hvir_params['data_params']["datatypes"]
    total_acc_check = 0
    with open(hvir_params['quality_config_file']) as json_file:
        quality_settings = json.load(json_file)
        data_requirements = quality_settings["data_requirements"]
        data_overrides    = quality_settings["data_overrides"]
        timeliness = quality_settings["timeliness"]

    for cat in data_requirements.keys():
        tot_k = len(data_requirements[cat])
        num_k = 0
        num_acc = 0
        for k in data_requirements[cat]:
            if k in survey.keys():
                total_acc_check += 1
                if survey[k] != None:
                    acc_check,value,error = accurate_data(data_params,type_selector,survey,k)
                    if acc_check:
                        num_acc += 1
                    else:
                        if error == 'ranged':
                            num_ranged += 1
                            survey[k] == None
                        elif error == 'bad data':
                            num_invalid += 1
                        else:
                            num_invalid += 1
                    num_k += 1

            else:
                #num_invalid += 1
                num_blank += 1

        acc  = num_acc/tot_k
        comp = num_k/tot_k
        # Check timeliness
        if cat in timeliness.keys():
            min_d = 'Missing'
            max_d = 'Missing'
            for k in intersection(timeliness[cat],survey.keys()):
                if survey[k] is not None:
                    acc_check, value,error = accurate_data(data_params, type_selector, survey, k)
                    if acc_check == False:
                        pass
                    elif value is not None:
                        if min_d == 'Missing':
                            min_d = value
                        elif min_d > value:
                            min_d = value
                        if max_d == 'Missing':
                            max_d = value
                        elif max_d < value:
                            max_d = value

                        max_dates = max(max_d, max_dates)
                        min_dates = min(min_dates, min_d)
            timeliness[cat] = min_d



        if cat in data_overrides.keys():
            for k in intersection(data_overrides[cat].keys(),survey.keys()):
                if data_overrides[cat][k] == survey[k]:
                    comp = data_overrides[cat][k][1]
        completeness[cat] = comp
        accuracy[cat]     = acc



    for k in completeness.keys(): # They are all done in this order so the csv is written correctly.
        quality[k+'_com'] = completeness[k]
        quality[k+'_acc'] = accuracy[k]
        if k in timeliness.keys():
            quality[k+'_tim'] = timeliness[k]
    quality['unique_id'] = survey['unique_id']
    return survey, quality,num_invalid,num_blank,num_ranged,num_invalid,max_dates,min_dates,total_acc_check


def cast_row(row, header, converters, key_fails):
    survey = {'mass_limit': None,
              'length_limit': None,
              'sealed_shoulder_width': None,
              'seal_flag': None,
              'sealed_should_width': None,
              'form_width': None,
              'seal_width': None}
    tmp_row = []
    for index, cell in enumerate(row):
        try:
            tmp_row.append(converters[header[index]](cell))
            survey[header[index]] = tmp_row[-1]
            # print('\t',header[index]+' '*(15-len(header[index])),'\t',index,'\t',cell+' '*(25-len(cell)),'\t',
        except:
            # print('Failed to pass row %s, field %s, with value %s' % (row_num,header[index],cell))
            tmp_row.append(None)
            survey[header[index]] = None
            if header[index] not in key_fails.keys():
                key_fails[header[index]] = 1
            else:
                key_fails[header[index]] += 1
    return survey, key_fails
