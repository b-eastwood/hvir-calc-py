import methods
import logging
from datetime import datetime
from datetime import date as date_class
import json

type_dict = {"str":[str,'A'],
             "datetime":[datetime,datetime.now()],
             "float":[float,0.1],
             "int":[int,1],
             "bool":[bool,False]
             }



def intersection(lst1, lst2):
    return list(set(lst1) & set(lst2))

class datetime_parser:
    def __init__(self,formats):
        self.formats = formats
    def parse(self,val,fin_year=False):
        if val == None or val == '':
            typed = None
            return typed
        else:
            if fin_year:
                t = str(val).split('/')
                t = str(t[0]) + '0101'
                val = t
                #logging.debug('Stripped datetime as was fin year %s',val)

            success = False
            i = 0
            while success == False and i < len(self.formats):
                try:
                    typed = datetime.strptime(val,self.formats[i])
                    success = True
                    #logging.debug("Sucessfully cast datetime %s with format %s" % (val, self.formats[i]))

                except ValueError as e:
                    pass
                    #logging.debug("Failed to cast datetime %s with format %s error: %s" % (val, self.formats[i], e))
                if success:
                    return typed
                i += 1


def create_typer(datetime_format):
    # To add date and range check into the type selector
    dt = datetime_parser(datetime_format)
    type_selector = {'int':      lambda val: (int(val) if val != '' else None),
                     'float':    lambda val: (float(val) if val != '' else None),
                     'bool':     lambda val: (bool(int(val)) if val != '' else None),
                     'datetime': dt.parse,
                     'str':      lambda val: (str(val).lower() if val != '' else None)
                     }
    return type_selector


def process_rows(raw_data, header, hvir_params, converters):
    header_warning(header,converters)
    surveys = []
    raw_surveys = []
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
    meta['complete'] = 0
    meta['incomplete'] = 0
    meta['num_valid'] = 0
    meta['min_date']    =  datetime.strptime('30000101','%Y%m%d')
    meta['max_date']    =  datetime.strptime('19010101','%Y%m%d')
    meta['attribute_quality'] = []
    type_selector = create_typer(hvir_params['data_params']['datetime_format'])

    for row_num, row in enumerate(raw_data):
        survey = None
        try:
            survey, key_fails,raw_survey = cast_row(row, header, converters, key_fails,hvir_params['data_params'])
        except:
            logging.debug("Couldn't read in this row: %s" % row_num)
            failed_rows.append(row_num)

        complete = 0
        incomplete = 0
        for k in converters.keys():
            if k not in survey.keys():
                incomplete += 1
            elif survey[k] == None:
                    incomplete += 1
            else:
                complete += 1

        survey, out_keys = calculator.method_logic(survey, hvir_params)
        if survey['hvir'] == 'NA':
            failed_rows.append(row_num)

        meta['incomplete'] += incomplete
        meta['complete']   += complete
        surveys.append(survey)
        raw_surveys.append(raw_survey)
        try:
            survey, quality, num_invalid, num_blank, num_ranged, num_invalid, num_valid, max_date, min_date, total_acc_check, num_valid, attribute_quality = check_quality(
                survey, hvir_params, type_selector)
            meta['num_ranged']      += num_ranged
            meta['num_blank']       += num_blank
            meta['num_invalid']     += num_invalid
            meta['num_valid']       += num_valid
            meta['total_acc_check'] += total_acc_check
            meta['min_date'] = min(min_date, meta['min_date'])
            meta['max_date'] = max(max_date, meta['max_date'])
            meta['accuracy'] = (num_invalid + num_blank + num_ranged) / (total_acc_check)
            meta['attribute_quality'].append(attribute_quality)
            quality_assessement.append(quality)

        except KeyError:
            logging.warning("couldn't calculate HVIR for this row: %s" % str(row_num))
            failed_rows.append(row_num)

    if meta['min_date'] == datetime.strptime('30000101', '%Y%m%d'):
        meta['min_date'] = 'Unknown'
    else:
        meta['min_date'] = meta['min_date'].strftime('%d/%m/%Y')

    if meta['max_date'] == datetime.strptime('19010101', '%Y%m%d'):
        meta['max_date'] = 'Unknown'
    else:
        meta['max_date'] = meta['max_date'].strftime('%d/%m/%Y')
    return key_fails, failed_rows, surveys, quality_assessement, out_keys,meta,raw_surveys

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
    the_type = data_params[k]['type']
    if the_type == 'datetime':
        rng = [datetime.now() if x == '{dnow}' else None if x == 'None' else datetime.strptime(x,'%Y%m%d') for x in rng]  # Parse in the  datetime ranges
    try:
        if k in survey.keys():
            if type(survey[k]) != type_dict[the_type][0]:
                typed = type_selector[the_type](survey[k])
            else:
                typed = survey[k]
            if rng_type == 'set':
                if typed in rng:
                    #logging.debug('%s in set for key %s' % (typed, k))
                    return True, typed, None
                else:
                    logging.debug("'%s' Out of set %s for key %s" % (typed, rng, k))
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
                    logging.debug('<%s> Out of range for key %s' % (typed, k))
                    return False, None, 'ranged'
            elif rng_type == "None":
                # logging.debug('%s in range for key %s' % (typed, k))
                return True, typed, None
            else:
                logging.critical('bad range specified in config file fro key %s' % k)
                return False, None, 'ranged'
        else:
            logging.debug('data %s for key is missing ' % k)
            return False, None, 'missing'
    except KeyError as e:
        logging.debug('Bad data format %s, error %s' % (k,e))
        return False, None, 'bad data'
    except TypeError as e:
        logging.debug("Bad type <%s> to %s for %s, error %s" % (survey[k], type, k, e))
        return False, None, 'bad data'



def check_quality(survey,hvir_params,type_selector):
    quality = {'unique_id':survey['unique_id']}
    completeness      = {}
    accuracy          = {}
    attribute_quality = {}
    max_dates = datetime.strptime('19010101','%Y%m%d')
    min_dates = datetime.strptime('30000101','%Y%m%d')
    num_blank   = 0
    num_ranged  = 0
    num_invalid = 0
    num_valid = 0
    incomplete, complete = 0, 0
    data_params       = hvir_params['data_params']["datatypes"]
    total_acc_check = 0
    with open(hvir_params['quality_config_file']) as json_file:
        quality_settings = json.load(json_file)
        data_requirements = quality_settings["data_requirements"]
        data_overrides    = quality_settings["data_overrides"]
        timeliness = quality_settings["timeliness"]

    for key_ in data_params:
        if key_ in survey.keys():
            if survey[key_] != None:
                acc_check, value, error = accurate_data(data_params, type_selector, survey, key_)
                if acc_check:
                    attribute_quality[key_] = 2
                    num_valid += 1
                else:
                    if error == 'ranged':
                        survey[key_] = None
                        attribute_quality[key_] = 1
                        num_ranged += 1
                        num_invalid += 1
                    elif error == 'bad data':
                        attribute_quality[key_] = 1
                        num_invalid += 1
                    else:
                        attribute_quality[key_] = 1
                        num_invalid += 1
            else:
                attribute_quality[key_] = 0
                num_invalid += 1
    if survey['unique_id'] != None:
        attribute_quality['unique_id'] = survey['unique_id'] # Match on unique id
    else:
        attribute_quality['unique_id'] = 'Missing'
    for cat in data_requirements.keys():
        tot_k = len(data_requirements[cat])
        num_k = 0
        num_acc = 0
        for k in data_requirements[cat]:
            if k in survey.keys():
                total_acc_check += 1
                if survey[k] != None:
                    num_k += 1
                    acc_check,value,error = accurate_data(data_params,type_selector,survey,k)
                    if acc_check:
                        num_acc += 1
                    else:
                        survey[k] = None
                        if error == 'ranged':
                            num_ranged += 1
                            survey[k] = None



        acc  = num_acc/tot_k
        comp = num_k/tot_k
        # Check timeliness
        if cat in timeliness.keys():
            min_d = 'Missing'
            max_d = 'Missing'
            for k in intersection(timeliness[cat],survey.keys()):
                if survey[k] is not None:
                    complete += 1
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
                else:
                    incomplete += 1
            if cat != 'fin_year':
                timeliness[cat] = min_d
            elif min_d == 'Missing':
                timeliness[cat] = 'Missing'
            else:
                timeliness[cat] = str(min_d.year) + '-' + str(min_d.year+1)[2:]



        if cat in data_overrides.keys():
            for k in intersection(data_overrides[cat].keys(),survey.keys()):
                if data_overrides[cat][k] == survey[k]:
                    comp = data_overrides[cat][k][1]
                    acc  = 1
        completeness[cat] = comp
        accuracy[cat]     = acc



    for k in completeness.keys(): # They are all done in this order so the csv is written correctly.
        quality[k+'_com'] = completeness[k]
        quality[k+'_acc'] = accuracy[k]
        if k in timeliness.keys():
            quality[k+'_tim'] = timeliness[k]
    return survey, quality,num_invalid,num_blank,num_ranged,num_invalid,num_valid,max_dates,min_dates,total_acc_check,num_acc,attribute_quality

def header_warning(header,converters):
    for k in converters.keys():
        if k not in header:
            logging.warning('Column %s not found in input data' % k)

def cast_row(row, header, converters, key_fails,data_types):
    survey = {}
    tmp_row = []
    raw_survey = {}
    for key_ in converters.keys():
        try:
            t_val = row[header.index(key_)]
            value = row[header.index(key_)]
            raw_value = row[header.index(key_)]
            raw_survey[key_] =  raw_value

            if data_types['datatypes'][key_]['type'] == 'datetime':
                #logging.debug('Converting Fin year Value: %s with Key: %s' % (key_,value))
                value = converters[key_](value,fin_year=key_=='fin_year')
            else:
                value = converters[key_](value)
            survey[key_] = value
        except ValueError:
            #Not in list
            value = None
            survey[key_] = value
            #key_fails += 1
        except Exception as e:
            value = None
            survey[key_] = value
            logging.debug('Failed to cast %s to class %s with type, error %s' % (t_val, key_ ,e))

        tmp_row.append(value)

    if 'seal_flag' not in survey.keys():
        survey['seal_flag'] =  None,
    if 'form_width' not in survey.keys():
        survey['form_width'] =  None,
    if 'seal_width' not in survey.keys():
        survey['seal_width'] =  None
    return survey, key_fails, raw_survey
