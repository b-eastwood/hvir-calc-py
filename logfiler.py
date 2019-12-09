import datetime


def create_pbi_log(quality_assessement,attrribute_quality,meta,failed_rows,params):
    write_lines = []
    total_rows = len(quality_assessement)
    sum_cells = (len(quality_assessement)*len(quality_assessement[0].keys()))
    no_data      = meta['incomplete']/sum_cells
    invalid_data = meta['num_invalid']/sum_cells - no_data
    valid_data =  1 - no_data -invalid_data

    write_lines.append(['Process Date',datetime.datetime.now().strftime("%B %d, %Y")])
    write_lines.append(['Total rows in dataset', total_rows])
    write_lines.append(['No data', no_data])
    write_lines.append(['Invalid data', invalid_data])
    write_lines.append(['Valid data', valid_data])
    write_lines.append(['HVIR No result', (len(failed_rows) / total_rows)])
    write_lines.append(['HVIR Successful', 1 - (len(failed_rows) / total_rows)])

    for k in quality_assessement[0].keys():
        if k != 'unique_id':
            valid = sum([a[k] if a[k] != 'Missing' else 0 for a in quality_assessement])/len([a[k] if a[k] != 'Missing' else 0 for a in quality_assessement])
            invalid = 1 - valid

            write_lines.append([k +' VALID',valid])
            write_lines.append([k + ' INVALID', invalid])
    for k in attrribute_quality[0].keys():
        zero = sum([a[k] == 0 for a in attrribute_quality])/len(attrribute_quality)
        one  = sum([a[k] == 1 for a in attrribute_quality])/len(attrribute_quality)
        two  = sum([a[k] == 2 for a in attrribute_quality])/len(attrribute_quality)

        write_lines.append([k+' 0',zero])
        write_lines.append([k+' 1',one])
        write_lines.append([k+' 2',two])
    return write_lines


def write_txt_log(params, key_fails, raw_data, failed_rows, meta):
    now = datetime.datetime.now()
    write_lines = []
    write_lines.append(['Completed: ' + now.strftime("%B %d, %Y") + '\n'])
    write_lines.append(['Total rows in dataset: %s\n' % str(len(raw_data))])
    write_lines.append(['Completeness: %s percent, %s BLANK attributes(s)\n' % (
        round(100 * meta['complete'] / (meta['complete'] + meta['incomplete']), 2), meta['incomplete'])])
    write_lines.append(['Accuracy: %s percent, %s INVALID attributes(s) %s attributes(s) OUT OF RANGE\n' % (
        round(100 * meta['num_valid'] / (meta['num_valid'] + meta['num_invalid']), 2), meta['num_invalid'],
        meta['num_ranged'])])
    write_lines.append(['Total of %s rows were not completed' % len(failed_rows) + '\n'])
    write_lines.append(['Timeliness: Condition data from %s to %s\n' % (meta['min_date'], meta['max_date'])])
    write_lines.append(['%s percent success rate for HVIR' % str(
        round((len(raw_data) - len(failed_rows)) / len(raw_data) * 100, 2)) + '\n'])
    return write_lines