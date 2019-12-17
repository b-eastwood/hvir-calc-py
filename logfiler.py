import datetime
year_keys = ['iri_date','rut_date','crk_date','tex_date']
fin_keys  = ['fin_year']

def create_dummy_vals(n):
    l = []
    for i in range(n):
        l.append('')
    return l
def create_pbi_log(surveys, quality_assessement,atrribute_quality,meta,failed_rows,params,writing_invalid=False):
    write_lines = []
    total_rows = len(quality_assessement)
    sum_cells = (len(quality_assessement)*len(quality_assessement[0].keys()))
    no_data      = meta['incomplete']/sum_cells
    invalid_data = meta['num_invalid']/sum_cells - no_data
    valid_data =  1 - no_data - invalid_data

    # Comprised of 3 data structures
    # One is two rows, keys on r1, data on r2
    # The second is 4 columns, first col is keys, next two are num blank, num invalid and num valid
    # Third ds is x rows, with 3 cols. First col is date key, second col is condition data validity
    # third col is fin year validity


    write_lines.append(['Process Date',datetime.datetime.now().strftime("%d/%m/%Y")])
    write_lines.append(['Total rows in dataset', total_rows])
    write_lines.append(['Total attributes in dataset',len(surveys)*55])
    write_lines.append(['Blanks', no_data])
    write_lines.append(['Invalid data', invalid_data])
    write_lines.append(['Valid data', valid_data])
    write_lines.append(['No HVIR result', (len(failed_rows) / total_rows)])
    write_lines.append(['HVIR Calculate', 1 - (len(failed_rows) / total_rows)])

    for k in quality_assessement[0].keys():
        if k != 'unique_id' and k.endswith('_acc'):
            valid = sum([a[k] if a[k] != 'Missing' else 0 for a in quality_assessement])/len([a[k] if a[k] != 'Missing' else 0 for a in quality_assessement])
            k = params['data_params']['acc_col_name'][k]
            write_lines.append([k,valid])
            if writing_invalid:
                invalid = 1 - valid
                write_lines.append([k + ' INVALID', invalid])

    h_row = [d[0] for d in write_lines]
    d_row = [d[1] for d in write_lines]
    ds_1 = [h_row,d_row]



    write_lines = [['Header names','Data Items','0 (Blank)','1 (Invalid)','2 (Valid)']]

    for k in atrribute_quality[0].keys():
        if k != 'unique_id':
            zero = sum([a[k] == 0 for a in atrribute_quality])/len(atrribute_quality)
            one  = sum([a[k] == 1 for a in atrribute_quality])/len(atrribute_quality)
            two  = sum([a[k] == 2 for a in atrribute_quality])/len(atrribute_quality)

            k = params['data_params']['logfile_col_names'][k] # overwrite with output key
            #write_lines.append([k+' 0',zero])
            #$write_lines.append([k+' 1',one])
            #write_lines.append([k+' 2',two])
            write_lines.append([k,zero,one,two])

    ds_2  = write_lines

    date_bins = {}
    fin_date_bins = {}
    fin_date_bins['Missing'] = 0
    date_bins['Missing'] = 0
    for k in surveys:
        dates = []
        for d_key in year_keys:
            if k[d_key] == None:
                dates.append(None)
            else:
                dates.append(k[d_key].year)
        if k[fin_keys[0]] == None:
            fin_date_bins['Missing'] += 1
        else:
            try:
                fin_date_bins[(k[fin_keys[0]].year+1)] += 1
            except:
                fin_date_bins[(k[fin_keys[0]].year+1)] =  1
        if len(set(dates)) == 1 and dates[0] == None:
            date_bins['Missing'] += 1
        else:
            d = min(dates)
            try:
                date_bins[d] += 1
            except KeyError:
                date_bins[d] = 1

    total = sum([date_bins[f] for f in date_bins.keys()])
    write_lines = [['Year', 'Condition Data', 'Financial Year End']]
    all_dates = {}
    for k in date_bins.keys():
        if k != 'Missing':
            all_dates[k] = [date_bins[k]/total,'']
    for k in fin_date_bins.keys():
        if k != 'Missing':
            try:
                all_dates[k][1] = fin_date_bins[k]/total
            except: #Need to add this date
                all_dates[k] = ['',fin_date_bins[k]/total]
    #print('Fin date bins',fin_date_bins.keys())
    missing_line = ['Missing','','']
    if 'Missing' in date_bins.keys():
        missing_line[1] = date_bins['Missing']/total
    if 'Missing' in fin_date_bins.keys():
        missing_line[2] = fin_date_bins['Missing']/total
    write_lines.append(missing_line)
    sorted_keys = list(all_dates.keys())
    sorted_keys.sort()
    for k in sorted_keys:
        write_lines.append([k,all_dates[k][0],all_dates[k][1]])
    ds_3 = write_lines
    #Now to combine the three datastructures
    vert_data = []
    for i in range(max([len(ds_2),len(ds_3)])):
        if i < len(ds_2):
            ds2_line = ds_2[i]
            vert_data.append(ds2_line)

        if i < len(ds_3):
            ds_3_line = ds_3[i]
            if len(vert_data) == i + 1: #Data was added in the previous step
                vert_data[-1] = vert_data[-1] + ds_3_line
            else: #data was not added, but we need to leave space for it
                dummy_cols = create_dummy_vals(len(ds_2[-1]))
                ds_3_line = dummy_cols + ds_3_line
                vert_data.append(ds_3_line)
        else: #no ds_3 data, need to create dummy data
            dummy_cols = create_dummy_vals(len(ds_3[-1]))
            vert_data[-1] = vert_data[-1] + dummy_cols

    #Combine ds1 and vert_data
    hoz_offset = len(ds_1[0])+1
    write_lines = []
    for i in range(len(vert_data)):
        if i > 1:
            dummy_cols = create_dummy_vals(hoz_offset)
            write_lines.append(dummy_cols+vert_data[i])
        else:
            write_lines.append(ds_1[i]+vert_data[i])



    #for k in date_bins.keys():

    #    write_lines.append(['Condition Data %s' % k,date_bins[k]/total])
    #for k in fin_date_bins.keys():
    #    write_lines.append(['Financial Year %s' % k,fin_date_bins[k]/total])
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