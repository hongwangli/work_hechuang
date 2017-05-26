#!/usr/bin/python
#-*- encoding:utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import pandas as pd
import numpy as np
import MySQLdb                                                                                                            
import random
import db_info
db_info = ('172.16.51.13','lihongwang','awCnUGr4iytZtDQYGKgnfmp2',65000,'risk','utf8')
print db_info
def get_conn(dbInfo):
   try:
        conn=MySQLdb.connect(host=dbInfo[0],user=dbInfo[1],
                passwd=dbInfo[2],port=dbInfo[3],db =dbInfo[4],charset=dbInfo[5])
        return conn
   except MySQLdb.Error,e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])


def readFromDB(db_info,sql):
    try:
        conn=get_conn(db_info)
        cur=conn.cursor()
        cur.execute(sql)
        data = cur.fetchall()
        return data
    except MySQLdb.Error,e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])


def merge_fields(fields):
    result=''
    for f in fields:
        s=" %s, "% f
        result+=s
    return result[0:-2]+" "



def get_train_sample():
    data = pd.read_csv('/root/Desktop/hechuang/csv_file/traindfdel1.csv')
    userids = data.iloc[:,0].astype(int)
    over = data.iloc[:,1].astype(int)
    exist_id = ()
    for i,userid in enumerate(userids):
        print 'i: ~~~~~: ', i
        #sql = '''select id,userid,type,ruleName from risk.risk_auth_warnrule where  userid = %s ''' % userid
        sql = '''SELECT t.userId,t.authType,t.status,t.rejectType,t.rejectReason,t.rejectDetail,
                t1.ruleCode ,t1.ruleName,t1.type,t1.promptMessage FROM risk.risk_auth_credit_task t 
                LEFT JOIN risk.risk_auth_warnrule t1 ON t.userId = t1.userId 
                WHERE t.authType regexp '^basic' AND t1.ruleCode not like 'jd%%'
                AND t1.ruleCode NOT LIKE 'bank%%'  AND t1.ruleCode NOT LIKE 'zhima%%' and t.userId = %s ''' % userid
        data_mysql = readFromDB(db_info,sql)
        if (data_mysql):
            data_mysql_new = map(lambda x: x + (over[i],),data_mysql)
            exist_id = exist_id + tuple(data_mysql_new)
    
    exist_id_array = np.array(exist_id)
    exist_id_df = pd.DataFrame(exist_id_array)
    exist_id_df.to_csv('/root/Desktop/hechuang/csv_file/exist_id.csv')

def print_hello():
    print 'hello'

def get_negative():
    sql = ''' SELECT userId,rulenum,DATE_FORMAT(submitdate,'%Y-%m') as format_date from (
        SELECT t.userId,count(1) as rulenum,min(t.submitAuditDate) as submitdate
        FROM  risk.risk_auth_credit_task t
                LEFT JOIN risk.risk_auth_warnrule t1 ON t.userId = t1.userId
                WHERE
                    t.authType LIKE 'basic%%'
                    AND t.submitAuditDate IS NOT NULL
                    AND t.`status` LIKE 'refuse%%'
                    AND t1.ruleCode NOT LIKE 'jd%%'
                    AND t1.ruleCode NOT LIKE 'bank%%'
                    AND t1.ruleCode NOT LIKE 'zhima%%'
                    and t1.type = 0
                    group by t.userId
                    ) a
                    WHERE a.rulenum > 4  and a.rulenum < 9 and submitdate <= '2017-03-31' and submitdate >= '2017-01-01' '''

    dbInfo = db_info
    conn=MySQLdb.connect(host=dbInfo[0],user=dbInfo[1], passwd=dbInfo[2],port=dbInfo[3],db =dbInfo[4],charset=dbInfo[5])
    data_mysql = pd.read_sql(sql,conn) 
    count_group = data_mysql.groupby(['rulenum','format_date'])['userId'].count()
    count_all = data_mysql['userId'].count()
    count_sample  = 10000
    num_group = (count_group / count_all * count_sample).astype(int)
    group_all = num_group.keys()
    group_values = num_group.values
    sample_ids = []
    for i,gp in enumerate(group_all):
        list_id = data_mysql[(data_mysql['rulenum']==gp[0]) & (data_mysql['format_date']== gp[1])]['userId'].tolist()
        random.seed(1)
        sample_id = random.sample(list_id, group_values[i])
        sample_ids.extend(sample_id) 
    return sample_ids


if __name__ == "__main__":
    print_hello()
    samples = get_negative()
    sam = pd.DataFrame(samples,columns = ['userId'])
    sam.to_csv('sample_0526.csv',index = False)


