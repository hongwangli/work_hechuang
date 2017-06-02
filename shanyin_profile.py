#! /usr/bin/env python
# -*- coding: utf-8 -*-
from shanyin_profile_subfunction import *
import json
import gzip
import base64
from cStringIO import StringIO
from data_base_info import *
print db_info

codes = ('S0545','S0544','S0428','S0680','S0670','S0671','S0549','S0550',\
             'S0552','S0553','S0395','S0428','S0629','S0631','S0636','S0637',\
             'S0331','S0332','S0334','S0335','S0056','S0059','S0102')

fields_risk_wecash_credit_behavior = ('applicationPlatforms','applicationCount',\
        'registrationPlatforms','registrationCount','overduePlatforms','overdueCount',\
        'loanlendersSixMonthsCount','loanlendersTwelveMonthsCount','loanlendersCount',\
        'loanlendersSixMonthsPlatforms','loanlendersTwelveMonthsPlatforms',\
        'loanlendersPlatforms','rejectionSixMonthsCount',\
        'rejectionTwelveMonthsCount','rejectionCount','rejectionSixMonthsPlatforms',\
        'rejectionTwelveMonthsPlatforms','rejectionPlatforms')

def get_shanyin_detail(data_dict):
    result = []
    for code in codes:
        if data_dict.has_key(code):
            if code == 'S0631':
                split_results = data_dict[code].split(';')
                total = sum([int(c.split('_')[1]) for c in split_results if c.split('_')[1]!='NA'])             
                result.append(str(total))
            else :
                result.append(data_dict[code])
        else:
            result.append(u'-1.0')
    return result


def g(s):
    s1 = base64.b64decode(s)
    buf = StringIO(s1)
    f = gzip.GzipFile(mode = 'rb', fileobj = buf)
    try:
        r_data = f.read()
    except:
        pass
    return r_data

def work():
    #sql2 = 'select apiResult from risk_wecash_credit_behavior where userId = 735026'
    #data2 = readFromDB(db_info,sql2)
    #s = data2[0][0]
    #j = g(s)
    #data_dict2 = json.loads(j)
    #data_dict2['data']['credit_behavior'][0]['origin']
    #tables = ('application','registration','arrearage','overdue','loanlenders','rejection')

    input_file = '/home/lihongwang/shanyin/feayuqi'
    output_file = '/home/lihongwang/shanyin/feayuqi_shanyin'
    stat_file = '/home/lihongwang/shanyin/stat_shanyin'
    count = 0;
    count_detail_T = 0; count_detail_F = 0; count_behavior_T = 0; count_behavior_F = 0;
    with open (input_file, 'r') as rfp, open(output_file,'w') as wfp:
        for line in rfp:
            count += 1
            print 'count: ',count
            linelist = line.strip().split('\t')
            #print 'linelist:',linelist
            userid = linelist[0]
            sql = 'select resultDetail from risk_wecash_unionpay_detail where userId = %s ' % userid
            #print 'sql: ', sql
            data = readFromDB(db_info,sql)
            #print 'datas: ',data
            if data:
                data_json = json.loads(data[0][0])
                data_dict = data_json['data']['unionpay_score'][0]['origin']['result']['quota']
                result = get_shanyin_detail(data_dict)
                count_detail_T += 1
            else:
                reslut = [u'-1.0' for x in xrange(len(codes))]
                count_detail_F += 1

            #print 'result: ',result
            sql_credit = 'select %s from risk_wecash_credit_behavior where userId = %s' \
                        % (merge_fields(fields_risk_wecash_credit_behavior),userid)
            data_credit = readFromDB(db_info,sql_credit)
            if data_credit:
                data_credit2 = map(fill_none,data_credit[0])
                count_behavior_T += 1
            else:
                data_credit2 = [u'-1.0' for x in xrange(len(fields_risk_wecash_credit_behavior))]
                count_behavior_F += 1
            #print data_credit2
            linelist.extend(result)
            linelist.extend(data_credit2)
            #print linelist
            wfp.write('\t'.join(linelist) + '\n')
    with open(stat_file,'w') as wfp2:
        content1 = 'table risk_wecash_unionpay_detail' + ': ' + \
                      'exist: ' + str(count_detail_T) + '  ' + \
                      'not exist: ' + str(count_detail_F) + '  ' + \
                      'proportion: ' + str(1.0*count_detail_T/count) + '\n'
       
        content2 = 'table risk_wecash_credit_behavior' + ': ' + \
                      'exist: ' + str(count_behavior_T) + '  ' + \
                      'not exist: ' + str(count_behavior_F) + '  ' + \
                      'proportion: ' + str(1.0*count_behavior_T/count) + '\n' 
        wfp2.write(content1)
        wfp2.write(content2)

if __name__ == '__main__':
    s = 'H4sIAAAAAAAAAG2QwUrEMBCGX2WZ8y4kBRfp3aNPsEjJJmMdaZMySQtSetmTePIBRB9BPK2efBm7\
    +Bg2LbtiMeSSf/7/n4+0UKL3KkdI4ft1f3jbHe4f+4cXWIJRQUHagmY0FLIt3qiGHEO6acEx5WTj\
    9Dd+ChZO2QKtQfbRMfVsrrolqKoqSKtAzs4nzKh4LPqjuwbZ1HOV8Rb1Py0Vu4asjjT9x+fX+9NA\
    o52J7wsxHojhnHzgOUULVaHCteNycCcnH3IWqIwNiZBnKylX8nwhRDrewXUMZcc9l5mUyXotFtBF\
    JE3hbsLp98/QTdocyddaD/8IaeAaux/dfyFPlAEAAA=='
    #work()
    a = g(s)
    print a
    print 'this is a test!'
